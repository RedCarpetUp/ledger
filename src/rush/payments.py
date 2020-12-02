from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.create_emi import (
    update_event_with_dpd,
    update_journal_entry,
)
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_downpayment,
    _adjust_for_prepayment,
    adjust_non_bill_payments,
    reduce_revenue_for_fee_refund,
    customer_refund_event,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.loan_schedule.loan_schedule import slide_payment_to_emis
from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    PaymentRequestsData,
    PaymentSplit,
    Fee,
)
from rush.utils import mul
from rush.writeoff_and_recovery import recovery_event


def payment_received(
    session: Session,
    user_loan: Optional[BaseLoan],
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
    payment_type: Optional[str] = None,
    user_product_id: Optional[int] = None,
    lender_id: Optional[int] = None,
    skip_closing: bool = False,
) -> None:
    assert user_loan is not None or lender_id is not None
    assert user_loan is not None or user_product_id is not None

    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={
            "payment_request_id": payment_request_id,
            "payment_type": payment_type,
            "user_product_id": user_product_id if user_product_id else user_loan.user_product_id,
            "lender_id": user_loan.lender_id if user_loan else lender_id,
        },
    )
    session.add(lt)
    session.flush()

    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{user_loan.lender_id if user_loan else lender_id}/lender/pg_account/a",
        event=lt,
        skip_closing=skip_closing,
        user_product_id=user_product_id if user_product_id else user_loan.user_product_id,
    )

    # TODO: check if this code is needed for downpayment, since there is no user loan at that point of time.
    if payment_type in ("downpayment", "card_activation_fees", "reset_joining_fees"):
        return

    run_anomaly(session=session, user_loan=user_loan, event_date=payment_date)
    gateway_charges = Decimal("0.5")
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=gateway_charges,
        gross_payment_amount=payment_amount,
        settlement_date=payment_date + relativedelta(days=2),
        user_loan=user_loan,
    )

    # Update dpd
    update_event_with_dpd(user_loan=user_loan, event=lt)
    # Update Journal Entry
    update_journal_entry(user_loan=user_loan, event=lt)


def refund_payment(
    session: Session,
    user_loan: BaseLoan,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
) -> None:
    lt = LedgerTriggerEvent(
        name="transaction_refund",
        loan_id=user_loan.loan_id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={
            "payment_request_id": payment_request_id,
        },
    )
    session.add(lt)
    session.flush()

    # Checking if bill is generated or not. if not then reduce from unbilled else treat as payment.
    transaction_refund_event(session=session, user_loan=user_loan, event=lt)
    run_anomaly(session=session, user_loan=user_loan, event_date=payment_date)

    # Update dpd
    update_event_with_dpd(user_loan=user_loan, event=lt)
    # Update Journal Entry
    update_journal_entry(user_loan=user_loan, event=lt)


def payment_received_event(
    session: Session,
    user_loan: BaseLoan,
    debit_book_str: str,
    event: LedgerTriggerEvent,
    skip_closing: bool = False,
    user_product_id: Optional[int] = None,
) -> None:
    payment_received_amt = Decimal(event.amount)

    payment_type = event.payment_type
    if not user_loan or payment_type == "card_reload_fees":
        assert user_product_id is not None

        if payment_type == "downpayment":
            _adjust_for_downpayment(session=session, event=event, amount=payment_received_amt)
        else:
            if not user_loan:
                identifier = "product"
                identifier_id = user_product_id
                assert payment_type != "card_reload_fees"
            else:
                identifier = "loan"
                identifier_id = user_loan.id
                assert payment_type == "card_reload_fees"

            adjust_non_bill_payments(
                session=session,
                event=event,
                amount=payment_received_amt,
                payment_type=payment_type,
                identifier=identifier,
                identifier_id=identifier_id,
                debit_book_str=debit_book_str,
            )

    else:
        actual_payment = payment_received_amt
        bills_data = find_amount_to_slide_in_bills(user_loan, payment_received_amt)
        for bill_data in bills_data:
            adjust_for_min_max_accounts(bill_data["bill"], bill_data["amount_to_adjust"], event.id)
            remaining_amount = _adjust_bill(
                session,
                bill_data["bill"],
                bill_data["amount_to_adjust"],
                event.id,
                debit_acc_str=debit_book_str,
            )
            # The amount to adjust is computed for this bill. It should all settle.
            assert remaining_amount == 0
            payment_received_amt -= bill_data["amount_to_adjust"]
        if user_loan.should_reinstate_limit_on_payment:
            user_loan.reinstate_limit_on_payment(event=event, amount=actual_payment)

        if payment_received_amt > 0:  # if there's payment left to be adjusted.
            _adjust_for_prepayment(
                session=session,
                loan_id=user_loan.loan_id,
                event_id=event.id,
                amount=payment_received_amt,
                debit_book_str=debit_book_str,
            )

        is_in_write_off = (
            get_account_balance_from_str(session, f"{user_loan.loan_id}/loan/write_off_expenses/e")[1]
            > 0
        )
        if is_in_write_off:
            recovery_event(user_loan, event)
            # TODO set loan status to recovered.

        # We will either slide or close bills
        slide_payment_to_emis(user_loan, event)

    create_payment_split(session, event)


def find_amount_to_slide_in_bills(user_loan: BaseLoan, total_amount_to_slide: Decimal) -> list:
    unpaid_bills = user_loan.get_unpaid_bills()
    bills_dict = [
        {
            "bill": bill,
            "total_outstanding": bill.get_outstanding_amount(),
            "amount_to_adjust": 0,
        }
        for bill in unpaid_bills
    ]
    total_amount_before_sliding = total_amount_to_slide
    total_loan_outstanding = sum(bill["total_outstanding"] for bill in bills_dict)
    # Either nothing is left to slide or loan is completely settled and there is excess payment.
    for bill_data in bills_dict:
        # If bill isn't generated there's no minimum amount scheduled. So can slide entire amount.
        if not bill_data["bill"].table.is_generated:
            amount_to_slide_based_on_ratio = total_amount_to_slide
        else:
            amount_to_slide_based_on_ratio = mul(
                bill_data["total_outstanding"] / total_loan_outstanding, total_amount_before_sliding
            )
        amount_to_adjust = min(
            amount_to_slide_based_on_ratio, total_amount_to_slide, bill_data["total_outstanding"]
        )
        bill_data["amount_to_adjust"] += amount_to_adjust
        bill_data["total_outstanding"] -= amount_to_adjust
    filtered_bills_dict = filter(lambda x: x["amount_to_adjust"] > 0, bills_dict)
    return filtered_bills_dict


def transaction_refund_event(session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    refund_amount = Decimal(event.amount)
    m2p_pool_account = f"{user_loan.lender_id}/lender/pool_balance/a"
    bills_data = find_amount_to_slide_in_bills(user_loan, refund_amount)

    for bill_data in bills_data:
        adjust_for_min_max_accounts(bill_data["bill"], bill_data["amount_to_adjust"], event.id)
        remaining_amount = _adjust_bill(
            session,
            bill_data["bill"],
            bill_data["amount_to_adjust"],
            event.id,
            debit_acc_str=m2p_pool_account,
        )
        assert remaining_amount == 0
        refund_amount -= bill_data["amount_to_adjust"]
    if refund_amount > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_loan.loan_id,
            event_id=event.id,
            amount=refund_amount,
            debit_book_str=m2p_pool_account,
        )

    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_loan.loan_id}/loan/refund_off_balance/l",  # Couldn't find anything relevant.
        amount=Decimal(event.amount),
    )
    create_payment_split(session, event)
    slide_payment_to_emis(user_loan, event)


def settle_payment_in_bank(
    session: Session,
    payment_request_id: str,
    gateway_expenses: Decimal,
    gross_payment_amount: Decimal,
    settlement_date: DateTime,
    user_loan: BaseLoan,
) -> None:
    settled_amount = gross_payment_amount - gateway_expenses
    event = LedgerTriggerEvent(
        name="payment_settled",
        loan_id=user_loan.loan_id,
        amount=settled_amount,
        post_date=settlement_date,
    )
    session.add(event)
    session.flush()

    payment_settlement_event(
        session=session, gateway_expenses=gateway_expenses, user_loan=user_loan, event=event
    )


def payment_settlement_event(
    session: Session, gateway_expenses: Decimal, user_loan: BaseLoan, event: LedgerTriggerEvent
) -> None:
    if gateway_expenses > 0:  # Adjust for gateway expenses.
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str="12345/redcarpet/gateway_expenses/e",
            credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
            amount=gateway_expenses,
        )
    _, writeoff_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/writeoff_expenses/e"
    )
    if writeoff_balance > 0:
        amount = min(writeoff_balance, event.amount)
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{user_loan.loan_id}/loan/bad_debt_allowance/ca",
            credit_book_str=f"{user_loan.loan_id}/loan/writeoff_expenses/e",
            amount=amount,
        )

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        amount=event.amount,
    )


def adjust_for_min_max_accounts(bill: BaseBill, payment_to_adjust_from: Decimal, event_id: int):
    min_due = bill.get_remaining_min()
    min_to_adjust_in_this_bill = min(min_due, payment_to_adjust_from)
    if min_to_adjust_in_this_bill != 0:
        # Reduce min amount
        create_ledger_entry_from_str(
            bill.session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=min_to_adjust_in_this_bill,
        )

    max_due = bill.get_remaining_max()
    max_to_adjust_in_this_bill = min(max_due, payment_to_adjust_from)
    if max_to_adjust_in_this_bill != 0:
        # Reduce min amount
        create_ledger_entry_from_str(
            bill.session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/max/l",
            credit_book_str=f"{bill.id}/bill/max/a",
            amount=max_to_adjust_in_this_bill,
        )


def get_payment_split_from_event(session: Session, event: LedgerTriggerEvent):
    split_data = (
        session.query(BookAccount.book_name, func.sum(LedgerEntry.amount))
        .filter(
            LedgerEntry.event_id == event.id,
            LedgerEntry.credit_account == BookAccount.id,
        )
        .group_by(BookAccount.book_name)
        .all()
    )
    allowed_accounts = (
        "cgst_payable",
        "igst_payable",
        "sgst_payable",
        "interest_receivable",
        "late_fine",
        "principal_receivable",
        "unbilled",
        "atm_fee",
        "card_activation_fees",
        "card_reload_fees",
        "downpayment",
        "reset_joining_fees",
    )
    # unbilled and principal belong to same component.
    updated_component_names = {
        "principal_receivable": "principal",
        "unbilled": "principal",
        "interest_receivable": "interest",
        "igst_payable": "igst",
        "cgst_payable": "cgst",
        "sgst_payable": "sgst",
    }
    normalized_split_data = {}
    for book_name, amount in split_data:
        if book_name not in allowed_accounts or amount == 0:
            continue
        if book_name in updated_component_names:
            book_name = updated_component_names[book_name]
        normalized_split_data[book_name] = normalized_split_data.get(book_name, 0) + amount
    return normalized_split_data


def create_payment_split(session: Session, event: LedgerTriggerEvent):
    """
    Create a payment split at ledger level. Has no emi context.
    Only tells how much principal, interest etc. got settled from x amount of payment.
    """
    split_data = get_payment_split_from_event(session, event)
    new_ps_objects = []
    for component, amount in split_data.items():
        new_ps_objects.append(
            {
                "payment_request_id": event.extra_details["payment_request_id"],
                "component": component,
                "amount_settled": amount,
                "loan_id": event.loan_id,
            }
        )

    session.bulk_insert_mappings(PaymentSplit, new_ps_objects)


def customer_refund(
    session: Session,
    user_loan: BaseLoan,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
):

    payment_request_data = (
        session.query(PaymentRequestsData)
        .filter(
            PaymentRequestsData.user_id == user_loan.user_id,
            PaymentRequestsData.payment_request_id == payment_request_id,
            PaymentRequestsData.payment_request_status == "Paid",
            PaymentRequestsData.row_status == "active",
        )
        .one()
    )

    fee = (
        session.query(Fee)
        .filter(
            Fee.name == payment_request_data.type,
            Fee.gross_amount == payment_request_data.payment_request_amount,
        )
        .order_by(Fee.updated_at.desc())
        .first()
    )

    if fee is None:
        return {"result": "error", "message": "No fee found"}

    fee_refund(
        session=session,
        user_loan=user_loan,
        payment_amount=payment_amount,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
        fee=fee,
    )

    lt = LedgerTriggerEvent(
        name="customer_refund",
        loan_id=user_loan.loan_id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={
            "payment_request_id": payment_request_id,
        },
    )
    session.add(lt)
    session.flush()

    customer_refund_event(
        session=session,
        loan_id=user_loan.loan_id,
        lender_id=user_loan.lender_id,
        event=lt,
    )

    update_journal_entry(user_loan=user_loan, event=lt)

    return {"result": "success", "message": "Customer Refund successfull"}


def fee_refund(
    session: Session,
    user_loan: BaseLoan,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
    fee: Fee,
):
    lt = LedgerTriggerEvent(
        name="fee_refund",
        loan_id=user_loan.loan_id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={
            "fee_id": fee.id,
            "payment_request_id": payment_request_id,
        },
    )
    session.add(lt)
    session.flush()

    reduce_revenue_for_fee_refund(
        session=session,
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        fee=fee,
    )

    update_journal_entry(user_loan=user_loan, event=lt)
