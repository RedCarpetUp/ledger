from decimal import Decimal
from typing import Optional

from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import case

from rush.anomaly_detection import run_anomaly
from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.create_emi import (
    update_event_with_dpd,
    update_journal_entry,
)
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_prepayment,
    adjust_for_revenue,
    adjust_non_bill_payments,
    reduce_revenue_for_fee_refund,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.loan_schedule.loan_schedule import slide_payment_to_emis
from rush.models import (
    BookAccount,
    Fee,
    LedgerEntry,
    LedgerTriggerEvent,
    PaymentRequestsData,
    PaymentSplit,
)
from rush.utils import mul
from rush.writeoff_and_recovery import recovery_event


def payment_received(
    session: Session,
    user_loan: Optional[BaseLoan],
    payment_request_data: PaymentRequestsData,
    skip_closing: bool = False,
) -> None:
    event = LedgerTriggerEvent.new(
        session,
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=payment_request_data.payment_request_amount,
        post_date=payment_request_data.intermediary_payment_date,
        extra_details={
            "payment_request_id": payment_request_data.payment_request_id,
        },
    )
    session.flush()

    remaining_payment_amount = payment_request_data.payment_request_amount

    loans = [user_loan]
    loans.extend(user_loan.get_child_loans())

    for loan in loans:
        if loan.get_remaining_min() and remaining_payment_amount:
            amount_to_adjust = min(loan.get_remaining_min(), remaining_payment_amount)
            remaining_payment_amount -= amount_to_adjust

            payment_received_event(
                session=session,
                user_loan=user_loan,
                payment_request_data=payment_request_data,
                amount_to_adjust=amount_to_adjust,
                debit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
                event=event,
                skip_closing=skip_closing,
            )

            run_anomaly(
                session=session,
                user_loan=user_loan,
                event_date=payment_request_data.intermediary_payment_date,
            )

            # Update dpd
            update_event_with_dpd(user_loan=user_loan, event=event)

    for loan in loans:
        if loan.get_remaining_max() and remaining_payment_amount:
            amount_to_adjust = min(loan.get_remaining_max(), remaining_payment_amount)
            remaining_payment_amount -= amount_to_adjust

            payment_received_event(
                session=session,
                user_loan=user_loan,
                payment_request_data=payment_request_data,
                amount_to_adjust=amount_to_adjust,
                debit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
                event=event,
                skip_closing=skip_closing,
            )

            run_anomaly(
                session=session,
                user_loan=user_loan,
                event_date=payment_request_data.intermediary_payment_date,
            )

            # Update dpd
            update_event_with_dpd(user_loan=user_loan, event=event)


def refund_payment(
    session: Session, user_loan: BaseLoan, payment_request_data: PaymentRequestsData
) -> None:
    lt = LedgerTriggerEvent(
        name="transaction_refund",
        loan_id=user_loan.loan_id,
        amount=payment_request_data.payment_request_amount,
        post_date=payment_request_data.intermediary_payment_date,
        extra_details={
            "payment_request_id": payment_request_data.payment_request_id,
        },
    )
    session.add(lt)
    session.flush()

    # Checking if bill is generated or not. if not then reduce from unbilled else treat as payment.
    transaction_refund_event(session=session, user_loan=user_loan, event=lt)
    run_anomaly(
        session=session, user_loan=user_loan, event_date=payment_request_data.intermediary_payment_date
    )

    # Update dpd
    update_event_with_dpd(user_loan=user_loan, event=lt)
    # Update Journal Entry
    update_journal_entry(user_loan=user_loan, event=lt)


def payment_received_event(
    session: Session,
    user_loan: BaseLoan,
    payment_request_data: PaymentRequestsData,
    debit_book_str: str,
    event: LedgerTriggerEvent,
    amount_to_adjust: Decimal,
    skip_closing: bool = False,
    user_product_id: Optional[int] = None,
) -> None:
    payment_received_amt = amount_to_adjust

    payment_type = payment_request_data.type
    if payment_type == "downpayment":
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
            credit_book_str=f"{user_loan.loan_id}/loan/downpayment/l",
            amount=payment_received_amt,
        )
    elif payment_type in (
        "card_reload_fees",
        "reset_joining_fees",
        "card_activation_fees",
        "card_upgrade_fees",
    ):
        adjust_non_bill_payments(
            session=session,
            event=event,
            amount=payment_received_amt,
            payment_type=payment_type,
            identifier="loan",
            identifier_id=user_loan.loan_id,
            debit_book_str=debit_book_str,
        )
    else:
        actual_payment = payment_received_amt
        payment_received_amt = adjust_payment(session, user_loan, event, debit_book_str)

        # Sometimes payments come in multiple decimal points.
        # adjust_payment() handles this while sliding, but we do this
        # for pre_payment
        payment_received_amt = round(payment_received_amt, 2)

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

    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.lender_id}/lender/gateway_expenses/e",
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        amount=payment_request_data.payment_execution_charges,
    )

    create_payment_split(session, event)


def find_split_to_slide_in_loan(session: Session, user_loan: BaseLoan, total_amount_to_slide: Decimal):
    # slide late fee.
    unpaid_bills = user_loan.get_unpaid_bills()
    unpaid_bill_ids = [unpaid_bill.table.id for unpaid_bill in unpaid_bills]
    split_info = []

    # higher priority is first
    fees_priority = ["atm_fee", "late_fee"]

    priority_case_expression = []
    for index, fee in enumerate(fees_priority):
        priority_case_expression.append((Fee.name == fee, index + 1))

    all_fees = (
        session.query(Fee)
        .filter(
            Fee.user_id == user_loan.user_id,
            Fee.name.in_(fees_priority),
            Fee.identifier_id.in_(unpaid_bill_ids),
            Fee.fee_status == "UNPAID",
            Fee.identifier == "bill",
        )
        .order_by(case(priority_case_expression))
        .all()
    )

    if all_fees:
        all_fees_by_type = {}
        for fee in all_fees:
            all_fees_by_type.setdefault(fee.name, []).append(fee)

        # Slide fees type-by-type
        for fee_type, fees in all_fees_by_type.items():
            total_fee_amount = sum(fee.remaining_fee_amount for fee in fees)
            total_amount_to_be_adjusted_in_fee = min(total_fee_amount, total_amount_to_slide)

            for fee in fees:
                bill = next(bill for bill in unpaid_bills if bill.table.id == fee.identifier_id)
                amount_to_slide_based_on_ratio = mul(
                    fee.remaining_fee_amount / total_fee_amount,
                    total_amount_to_be_adjusted_in_fee,
                )
                x = {
                    "type": "fee",
                    "bill": bill,
                    "fee": fee,
                    "amount_to_adjust": amount_to_slide_based_on_ratio,
                }
                split_info.append(x)
            total_amount_to_slide -= total_amount_to_be_adjusted_in_fee

    # slide interest.
    total_interest_amount = sum(bill.get_interest_due() for bill in unpaid_bills)
    if total_amount_to_slide > 0 and total_interest_amount > 0:
        total_amount_to_be_adjusted_in_interest = min(total_interest_amount, total_amount_to_slide)
        for bill in unpaid_bills:
            amount_to_slide_based_on_ratio = mul(
                bill.get_interest_due() / total_interest_amount, total_amount_to_be_adjusted_in_interest
            )
            if amount_to_slide_based_on_ratio > 0:  # will be 0 for 0 bill with late fee.
                x = {
                    "type": "interest",
                    "bill": bill,
                    "amount_to_adjust": amount_to_slide_based_on_ratio,
                }
                split_info.append(x)
        total_amount_to_slide -= total_amount_to_be_adjusted_in_interest

    # slide principal.
    total_principal_amount = sum(bill.get_principal_due() for bill in unpaid_bills)
    if total_amount_to_slide > 0 and total_principal_amount > 0:
        total_amount_to_be_adjusted_in_principal = min(total_principal_amount, total_amount_to_slide)
        for bill in unpaid_bills:
            amount_to_slide_based_on_ratio = mul(
                bill.get_principal_due() / total_principal_amount,
                total_amount_to_be_adjusted_in_principal,
            )
            if amount_to_slide_based_on_ratio > 0:
                x = {
                    "type": "principal",
                    "bill": bill,
                    "amount_to_adjust": amount_to_slide_based_on_ratio,
                }
                split_info.append(x)
        total_amount_to_slide -= total_amount_to_be_adjusted_in_principal
    return split_info


def transaction_refund_event(session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    m2p_pool_account = f"{user_loan.lender_id}/lender/pool_balance/a"
    refund_amount = adjust_payment(session, user_loan, event, m2p_pool_account)

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


def adjust_payment(
    session: Session,
    user_loan: BaseLoan,
    event: LedgerTriggerEvent,
    debit_book_str: str,
) -> Decimal:
    amount_to_adjust = Decimal(event.amount)
    split_data = find_split_to_slide_in_loan(session, user_loan, amount_to_adjust)

    for data in split_data:
        adjust_for_min_max_accounts(data["bill"], data["amount_to_adjust"], event.id)
        if data["type"] == "fee":
            adjust_for_revenue(
                session=session,
                event_id=event.id,
                payment_to_adjust_from=data["amount_to_adjust"],
                debit_str=debit_book_str,
                fee=data["fee"],
            )
        if data["type"] in ("interest", "principal"):
            remaining_amount = _adjust_bill(
                session,
                data["bill"],
                data["amount_to_adjust"],
                event.id,
                debit_acc_str=debit_book_str,
            )
            # The amount to adjust is computed for this bill. It should all settle.
            assert remaining_amount == 0
        amount_to_adjust -= data["amount_to_adjust"]

    return amount_to_adjust


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

    payment_settlement_event(session=session, user_loan=user_loan, event=event)

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id)
        .first()
    )

    update_journal_entry(user_loan=user_loan, event=payment_ledger_event)


def payment_settlement_event(session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
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
        "pre_payment",
        "card_upgrade_fees",
    )
    # unbilled and principal belong to same component.
    updated_component_names = {
        "principal_receivable": "principal",
        "downpayment": "principal",
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

    create_ledger_entry_from_str(
        session=session,
        event_id=lt.id,
        debit_book_str=f"{user_loan.loan_id}/loan/pre_payment/l",
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        amount=payment_amount,
    )

    update_journal_entry(user_loan=user_loan, event=lt)

    return {"result": "success", "message": "Prepayment Refund successfull"}


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

    return {"result": "success", "message": "Fee Refund successfull"}
