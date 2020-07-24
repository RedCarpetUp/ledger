from decimal import Decimal
from typing import List

import dateutil.relativedelta
from sqlalchemy import (
    Date,
    cast,
    extract,
    func,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from rush.card import BaseCard
from rush.card.base_card import BaseBill
from rush.ledger_utils import (
    breakdown_account_variables_from_str,
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_book_account_by_string,
)
from rush.lender_interest import lender_interest
from rush.models import (
    CardTransaction,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import (
    div,
    mul,
)


def lender_disbursal_event(session: Session, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"12345/redcarpet/rc_cash/a",
        credit_book_str=f"62311/lender/lender_capital/l",
        amount=event.amount,
    )


def m2p_transfer_event(session: Session, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"62311/lender/pool_balance/a",
        credit_book_str=f"12345/redcarpet/rc_cash/a",
        amount=event.amount,
    )


def card_transaction_event(session: Session, user_card: BaseCard, event: LedgerTriggerEvent) -> None:
    amount = Decimal(event.amount)
    user_card_id = user_card.id
    swipe_id = event.extra_details["swipe_id"]
    bill = (
        session.query(LoanData)
        .filter(LoanData.id == CardTransaction.loan_id, CardTransaction.id == swipe_id)
        .scalar()
    )
    lender_id = bill.lender_id
    bill_id = bill.id

    # Reduce user's card balance
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card_id}/card/available_limit/l",
        credit_book_str=f"{user_card_id}/card/available_limit/a",
        amount=amount,
    )

    # Move debt from one account to another. We will be charged interest on lender_payable.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{lender_id}/lender/lender_capital/l",
        credit_book_str=f"{user_card_id}/card/lender_payable/l",
        amount=amount,
    )

    # Reduce money from lender's pool account
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/unbilled/a",
        credit_book_str=f"{lender_id}/lender/pool_balance/a",
        amount=amount,
    )


def bill_generate_event(
    session: Session, bill: BaseBill, user_card_id: int, event: LedgerTriggerEvent
) -> None:
    bill_id = bill.id

    # Move all unbilled book amount to billed account
    _, unbilled_balance = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/principal_receivable/a",
        credit_book_str=f"{bill_id}/bill/unbilled/a",
        amount=unbilled_balance,
    )

    # checking prepayment_balance
    _, prepayment_balance = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/pre_payment/l"
    )
    if prepayment_balance > 0:
        balance = min(unbilled_balance, prepayment_balance)
        # reducing balance from pre payment and unbilled
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{user_card_id}/card/pre_payment/l",
            credit_book_str=f"{bill_id}/bill/principal_receivable/a",
            amount=balance,
        )


def add_min_amount_event(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/min/a",
        credit_book_str=f"{bill.id}/bill/min/l",
        amount=amount,
    )


def payment_received_event(
    session: Session, user_card: BaseCard, debit_book_str: str, event: LedgerTriggerEvent,
) -> None:
    payment_received = Decimal(event.amount)
    gateway_charges = event.extra_details.get("gateway_charges")
    unpaid_bills = user_card.get_unpaid_bills()

    payment_received = _adjust_for_min(
        session, unpaid_bills, payment_received, event.id, debit_book_str=debit_book_str,
    )
    payment_received = _adjust_for_complete_bill(
        session, unpaid_bills, payment_received, event.id, debit_book_str=debit_book_str,
    )

    if payment_received > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session, user_card.id, event.id, payment_received, debit_book_str=debit_book_str
        )

    if gateway_charges > 0:  # Adjust for gateway expenses.
        _adjust_for_gateway_expenses(session, event, debit_book_str)

    _, writeoff_balance = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/writeoff_expenses/e"
    )
    if writeoff_balance > 0:
        amount = min(writeoff_balance, event.amount)
        _adjust_for_recovery(session, user_card.id, event.id, amount)

    else:
        _adjust_lender_payable(session, user_card.id, debit_book_str, gateway_charges, event)

    # Slide payment in emi
    from rush.create_emi import slide_payments

    slide_payments(session, user_card.user_id, payment_event=event)


def _adjust_for_gateway_expenses(session: Session, event: LedgerTriggerEvent, credit_book_str: str):
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str="12345/redcarpet/gateway_expenses/e",
        credit_book_str=credit_book_str,
        amount=event.extra_details["gateway_charges"],
    )


def _adjust_for_recovery(session: Session, user_card_id: int, event_id: int, amount: Decimal) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event_id,
        debit_book_str=f"{user_card_id}/card/bad_debt_allowance/ca",
        credit_book_str=f"{user_card_id}/card/writeoff_expenses/e",
        amount=Decimal(amount),
    )


def _adjust_lender_payable(
    session: Session,
    user_card_id: int,
    credit_book_str: str,
    gateway_charges: Decimal,
    event: LedgerTriggerEvent,
) -> None:
    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card_id}/card/lender_payable/l",
        credit_book_str=credit_book_str,
        amount=Decimal(event.amount) - Decimal(gateway_charges),
    )


def _adjust_bill(
    session: Session,
    bill: LoanData,
    amount_to_adjust_in_this_bill: Decimal,
    event_id: int,
    debit_acc_str: str,
) -> Decimal:
    def adjust(payment_to_adjust_from: Decimal, to_acc: str, from_acc: str) -> Decimal:
        if payment_to_adjust_from <= 0:
            return payment_to_adjust_from
        _, book_balance = get_account_balance_from_str(session, book_string=from_acc)
        if book_balance > 0:
            balance_to_adjust = min(payment_to_adjust_from, book_balance)
            create_ledger_entry_from_str(
                session,
                event_id=event_id,
                debit_book_str=to_acc,
                credit_book_str=from_acc,
                amount=Decimal(balance_to_adjust),
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    # Now adjust into other accounts.
    remaining_amount = adjust(
        amount_to_adjust_in_this_bill,
        to_acc=debit_acc_str,  # f"{bill.lender_id}/lender/pg_account/a"
        from_acc=f"{bill.id}/bill/late_fine_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount, to_acc=debit_acc_str, from_acc=f"{bill.id}/bill/interest_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount, to_acc=debit_acc_str, from_acc=f"{bill.id}/bill/principal_receivable/a",
    )
    return remaining_amount


def _adjust_for_min(
    session: Session,
    bills: List[BaseBill],
    payment_received: Decimal,
    event_id: int,
    debit_book_str: str,
) -> Decimal:
    for bill in bills:
        min_due = bill.get_remaining_min()
        amount_to_adjust_in_this_bill = min(min_due, payment_received)
        # Remove amount from the original variable.
        payment_received -= amount_to_adjust_in_this_bill
        if amount_to_adjust_in_this_bill == 0:
            continue
        # Reduce min amount
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=amount_to_adjust_in_this_bill,
        )
        remaining_amount = _adjust_bill(
            session, bill, amount_to_adjust_in_this_bill, event_id, debit_acc_str=debit_book_str,
        )
        assert remaining_amount == 0  # Can't be more than 0
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_complete_bill(
    session: Session,
    bills: List[BaseBill],
    payment_received: Decimal,
    event_id: int,
    debit_book_str: str,
) -> Decimal:
    for bill in bills:
        payment_received = _adjust_bill(
            session, bill, payment_received, event_id, debit_acc_str=debit_book_str,
        )
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_prepayment(
    session: Session, card_id: int, event_id: int, amount: Decimal, debit_book_str: str
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event_id,
        debit_book_str=debit_book_str,
        credit_book_str=f"{card_id}/card/pre_payment/l",
        amount=amount,
    )


def accrue_interest_event(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/interest_receivable/a",
        credit_book_str=f"{bill.id}/bill/interest_earned/r",
        amount=amount,
    )
    # adjust the given interest in schedule
    from rush.create_emi import adjust_interest_in_emis

    adjust_interest_in_emis(session, bill.user_id, event.post_date)


def accrue_late_fine_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/late_fine_receivable/a",
        credit_book_str=f"{bill.id}/bill/late_fine/r",
        amount=event.amount,
    )
    # adjust the given interest in schedule
    from rush.create_emi import adjust_late_fee_in_emis

    adjust_late_fee_in_emis(session, bill.user_id, event.post_date)

    # Add into min amount of the bill too.
    add_min_amount_event(session, bill, event, event.amount)


def refund_event(
    session: Session, bill: LoanData, user_card: BaseCard, event: LedgerTriggerEvent
) -> None:
    _, amount_billed = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    if amount_billed == 0:  # Refund before bill generation
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{bill.lender_id}/lender/merchant_refund/a",
            credit_book_str=f"{bill.id}/bill/unbilled/a",
            amount=event.amount,
        )
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{user_card.id}/card/lender_payable/l",
            credit_book_str=f"{bill.lender_id}/lender/merchant_refund/a",
            amount=event.amount,
        )
    else:
        payment_received_event(session, user_card, f"{bill.lender_id}/lender/merchant_refund/a", event)


def lender_interest_incur_event(session: Session, event: LedgerTriggerEvent) -> None:
    last_trigger = (
        session.query(LedgerTriggerEvent)
        .filter(LedgerTriggerEvent.name.in_(["lender_interest_incur"]),)
        .order_by(LedgerTriggerEvent.post_date.desc())
        .offset(1)
        .first()
    )
    if last_trigger == None:
        earliest_payable_change = (
            session.query(LedgerTriggerEvent)
            .filter(LedgerTriggerEvent.name.in_(["card_transaction"]),)
            .order_by(LedgerTriggerEvent.post_date)
            .first()
        )
        last_lender_incur_trigger = earliest_payable_change.post_date
    else:
        last_lender_incur_trigger = last_trigger.post_date

    all_user_cards = session.query(UserCard).all()

    for card in all_user_cards:
        lender_interest_rate = card.lender_rate_of_interest_annual

        # can't use div since interest is 1.00047 because div consumes the decimal.
        lender_interest_rate = (36500 + lender_interest_rate) / 36500

        params = {
            "card_id": card.id,
            "lender_interest_rate": lender_interest_rate,
            "event_post_date": event.post_date,
            "last_lender_incur_trigger": last_lender_incur_trigger,
        }
        total_amount = (
            session.execute(
                """   
                    WITH RECURSIVE date_wise_lender_payable_balance AS (
                        SELECT DATE(ledger_trigger_event.post_date) AS post_date, COALESCE(get_account_balance(:card_id,'card', 'lender_payable', 'l', DATE(ledger_trigger_event.post_date)), 0) AS amount FROM ledger_trigger_event
                        WHERE ledger_trigger_event.post_date > CAST(:last_lender_incur_trigger AS DATE) AND ledger_trigger_event.post_date <= CAST(:event_post_date AS DATE)
                        GROUP BY DATE(ledger_trigger_event.post_date) ORDER BY DATE(ledger_trigger_event.post_date) DESC
                    ),
                    day_wise_lender_payable_balance AS (
                        SELECT (post_date - CAST(:last_lender_incur_trigger AS DATE) -1 ) AS days, amount FROM date_wise_lender_payable_balance WHERE amount > 0
                    ),
                    interest_on_lender_payable_balance AS (
                        SELECT (days - 1- COALESCE(LAG(days) OVER(ORDER BY days), 0)) AS daily, amount FROM day_wise_lender_payable_balance
                    ),
                    reccruring_interest_on_lender_payable_balance_view AS (
                        SELECT *, POW(:lender_interest_rate, daily)* amount - amount as interest_amount, row_number() over(order by amount) as rn FROM interest_on_lender_payable_balance order by daily desc 
                    ),
                    rec_query(rn, days, amount, amount_interest) as 
                    (
                        select rn, CAST(daily AS NUMERIC), amount, interest_amount
                        from reccruring_interest_on_lender_payable_balance_view
                        where rn = 1
                        union all   
                        select 
                        t.rn, t.daily, t.amount, POW(:lender_interest_rate, t.daily)*(t.amount + p.amount_interest) - t.amount - p.amount_interest
                        from rec_query p
                        join reccruring_interest_on_lender_payable_balance_view t on t.rn = p.rn + 1
                    )
                    SELECT ROUND(SUM(amount_interest), 2) FROM rec_query;   
                """,
                params,
            ).scalar()
            or 0
        )
        if total_amount > 0:
            create_ledger_entry_from_str(
                session,
                event_id=event.id,
                debit_book_str=f"{card.id}/card/redcarpet_expenses/e",
                credit_book_str=f"{card.id}/card/lender_payable/l",
                amount=total_amount,
            )


def writeoff_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/bad_debt_allowance/ca",
        amount=event.amount,
    )
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/writeoff_expenses/e",
        credit_book_str=f"{user_card.id}/redcarpet/redcarpet_account/a",
        amount=event.amount,
    )


def customer_refund_event(
    session: Session, card_id: int, lender_id: int, event: LedgerTriggerEvent
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{card_id}/card/pre_payment/l",
        credit_book_str=f"{lender_id}/lender/pg_account/a",
        amount=Decimal(event.amount),
    )


def limit_assignment_event(session: Session, card_id: int, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{card_id}/card/available_limit/a",
        credit_book_str=f"{card_id}/card/available_limit/l",
        amount=Decimal(event.amount),
    )
