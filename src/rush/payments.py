from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_prepayment,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_remaining_bill_balance,
)
from rush.models import (
    CardEmis,
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)
from rush.writeoff_and_recovery import recovery_event


def payment_received(
    session: Session,
    user_card: BaseLoan,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
) -> None:
    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_card.loan_id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={"payment_request_id": payment_request_id},
    )
    session.add(lt)
    session.flush()

    payment_received_event(session, user_card, f"{user_card.lender_id}/lender/pg_account/a", lt)
    run_anomaly(session, user_card, payment_date)
    gateway_charges = Decimal("0.5")
    settle_payment_in_bank(
        session,
        payment_request_id,
        gateway_charges,
        payment_amount,
        payment_date + relativedelta(days=2),
        user_card,
    )


def refund_payment(
    session: Session,
    user_card: BaseLoan,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
    original_swipe: CardTransaction,
) -> None:
    lt = LedgerTriggerEvent(
        name="transaction_refund",
        loan_id=user_card.loan_id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={"payment_request_id": payment_request_id},
    )
    session.add(lt)
    session.flush()

    bill_of_original_transaction = (
        session.query(LoanData).filter_by(id=original_swipe.loan_id, is_generated=False).one_or_none()
    )
    # Checking if bill is generated or not. if not then reduce from unbilled else treat as payment.
    transaction_refund_event(session, user_card, lt, bill_of_original_transaction)
    run_anomaly(session, user_card, payment_date)


def payment_received_event(
    session: Session, user_card: BaseLoan, debit_book_str: str, event: LedgerTriggerEvent,
) -> None:
    payment_received = Decimal(event.amount)
    if event.name == "merchant_refund":
        pass
    elif event.name == "payment_received":
        actual_payment = payment_received
        bills_data = find_amount_to_slide_in_bills(user_card, payment_received)
        for bill_data in bills_data:
            adjust_for_min(bill_data["bill"], bill_data["amount_to_adjust"], event.id)
            remaining_amount = _adjust_bill(
                session,
                bill_data["bill"],
                bill_data["amount_to_adjust"],
                event.id,
                debit_acc_str=debit_book_str,
            )
            assert (
                remaining_amount == 0
            )  # The amount to adjust is computed for this bill. It should all settle.
            payment_received -= bill_data["amount_to_adjust"]
        if user_card.should_reinstate_limit_on_payment:
            user_card.reinstate_limit_on_payment(event=event, amount=actual_payment)

    if payment_received > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_card.loan_id,
            event_id=event.id,
            amount=payment_received,
            debit_book_str=debit_book_str,
        )

    is_in_write_off = (
        get_account_balance_from_str(session, f"{user_card.loan_id}/loan/write_off_expenses/e")[1] > 0
    )
    if is_in_write_off:
        recovery_event(user_card, event)
        # TODO set loan status to recovered.
    from rush.create_emi import slide_payments

    slide_payments(user_card=user_card, payment_event=event)


def find_amount_to_slide_in_bills(user_card: BaseLoan, total_amount_to_slide: Decimal) -> list:
    card_emis = (
        user_card.session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_card.loan_id,
            CardEmis.row_status == "active",
            CardEmis.payment_status == "UnPaid",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number)
        .all()
    )
    total_amount_to_slide = min(total_amount_to_slide, user_card.get_total_outstanding())
    bill_id_and_amount_to_adjust = {}
    for emi in card_emis:
        payment_received_on_emi = emi.get_payment_received_on_emi()
        bills_split = {k: Decimal(v) for k, v in emi.extra_details.items()}

        # I need to reduce the payment that we have received on this emi from the bill splits.
        if payment_received_on_emi:
            for bill_id, bill_split_value in bills_split.items():
                amount_to_reduce_per_bill = min(bill_split_value, payment_received_on_emi)
                bills_split[bill_id] -= amount_to_reduce_per_bill

        for bill_id, bill_split_value in bills_split.items():
            if bill_split_value == 0:
                continue
            # TODO check bill outstanding and the amount to adjust shouldn't be more than that.
            amount_to_adjust = min(bill_split_value, total_amount_to_slide)
            bill_id_and_amount_to_adjust[bill_id] = (
                bill_id_and_amount_to_adjust.get(bill_id, 0) + amount_to_adjust
            )
            total_amount_to_slide -= amount_to_adjust
        if total_amount_to_slide == 0:
            break
    bill_and_amount_to_adjust_list = []
    for bill_id, amount_to_adjust in bill_id_and_amount_to_adjust.items():
        bill = user_card.session.query(LoanData).filter_by(id=bill_id).one()
        bill_and_amount_to_adjust_dict = {
            "bill": user_card.convert_to_bill_class(bill),
            "amount_to_adjust": amount_to_adjust,
        }
        bill_and_amount_to_adjust_list.append(bill_and_amount_to_adjust_dict)
    return bill_and_amount_to_adjust_list


def transaction_refund_event(
    session: Session, user_card: BaseLoan, event: LedgerTriggerEvent, bill: LoanData
) -> None:
    refund_amount = Decimal(event.amount)
    m2p_pool_account = f"{user_card.lender_id}/lender/pool_balance/a"
    if bill:  # refund happened before bill was generated so reduce it from unbilled.
        # TODO check if refund amount is <= unbilled.
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=m2p_pool_account,
            credit_book_str=f"{bill.id}/bill/unbilled/a",
            amount=refund_amount,
        )
    else:  # Treat as payment.
        bills_data = find_amount_to_slide_in_bills(user_card, refund_amount)
        for bill_data in bills_data:
            adjust_for_min(bill_data["bill"], bill_data["amount_to_adjust"], event.id)
            refund_amount = _adjust_bill(
                session,
                bill_data["bill"],
                bill_data["amount_to_adjust"],
                event.id,
                debit_acc_str=m2p_pool_account,
            )
        if refund_amount > 0:  # if there's payment left to be adjusted.
            _adjust_for_prepayment(
                session=session,
                loan_id=user_card.loan_id,
                event_id=event.id,
                amount=refund_amount,
                debit_book_str=m2p_pool_account,
            )

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_card.loan_id}/loan/refund_off_balance/l",  # Couldn't find anything relevant.
        amount=Decimal(event.amount),
    )

    from rush.create_emi import group_bills_to_create_loan_schedule

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_card)


def settle_payment_in_bank(
    session: Session,
    payment_request_id: str,
    gateway_expenses: Decimal,
    gross_payment_amount: Decimal,
    settlement_date: DateTime,
    user_card: BaseLoan,
) -> None:
    settled_amount = gross_payment_amount - gateway_expenses
    event = LedgerTriggerEvent(
        name="payment_settled",
        loan_id=user_card.loan_id,
        amount=settled_amount,
        post_date=settlement_date,
    )
    session.add(event)
    session.flush()

    payment_settlement_event(session, gateway_expenses, user_card, event)


def payment_settlement_event(
    session: Session, gateway_expenses: Decimal, user_card: BaseLoan, event: LedgerTriggerEvent
) -> None:
    if gateway_expenses > 0:  # Adjust for gateway expenses.
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str="12345/redcarpet/gateway_expenses/e",
            credit_book_str=f"{user_card.lender_id}/lender/pg_account/a",
            amount=gateway_expenses,
        )
    _, writeoff_balance = get_account_balance_from_str(
        session, book_string=f"{user_card.loan_id}/loan/writeoff_expenses/e"
    )
    if writeoff_balance > 0:
        amount = min(writeoff_balance, event.amount)
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{user_card.loan_id}/loan/bad_debt_allowance/ca",
            credit_book_str=f"{user_card.loan_id}/loan/writeoff_expenses/e",
            amount=amount,
        )

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_card.lender_id}/lender/pg_account/a",
        amount=event.amount,
    )


def adjust_for_min(bill: BaseBill, payment_to_adjust_from: Decimal, event_id: int):
    min_due = bill.get_remaining_min()
    min_to_adjust_in_this_bill = min(min_due, payment_to_adjust_from)
    if min_to_adjust_in_this_bill == 0:
        return
    # Reduce min amount
    create_ledger_entry_from_str(
        bill.session,
        event_id=event_id,
        debit_book_str=f"{bill.id}/bill/min/l",
        credit_book_str=f"{bill.id}/bill/min/a",
        amount=min_to_adjust_in_this_bill,
    )
