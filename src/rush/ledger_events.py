import datetime
from decimal import Decimal
from typing import (
    List,
    Tuple,
)

import dateutil.relativedelta
from sqlalchemy import (
    Date,
    cast,
    extract,
    func,
)
from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_all_unpaid_bills,
    get_book_account_by_string,
    get_remaining_bill_balance,
    is_bill_closed,
    is_min_paid,
)
from rush.models import (
    BookAccount,
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


def card_transaction_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
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
        credit_book_str=f"{user_card.id}/card/lender_payable/l",
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
    session: Session, bill: LoanData, user_card_id: int, event: LedgerTriggerEvent
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
        balance = min(unbilled_balance, pre_payment_amount)
        # reducing balance from pre payment and unbilled
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{user_card_id}/card/pre_payment/l",
            credit_book_str=f"{bill_id}/bill/principal_receivable/a",
            amount=balance,
        )


def add_min_amount_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    bill_id = bill.id

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/min/a",
        credit_book_str=f"{bill_id}/bill/min/l",
        amount=event.amount,
    )


def payment_received_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    payment_received = Decimal(event.amount)
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)

    payment_received = _adjust_for_min(session, unpaid_bills, payment_received, event.id)
    payment_received = _adjust_for_complete_bill(session, unpaid_bills, payment_received, event.id)

    if payment_received > 0:
        _adjust_for_prepayment(session, user_card.id, event.post_date, payment_received)

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/pg_account/a",
        amount=payment_received,
    )


def _adjust_bill(
    session: Session, bill: LoanData, amount_to_adjust_in_this_bill: Decimal, event_id: int
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
                amount=balance_to_adjust,
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    # Now adjust into other accounts.
    remaining_amount = adjust(
        amount_to_adjust_in_this_bill,
        to_acc=f"{bill.lender_id}/lender/pg_account/a",
        from_acc=f"{bill.id}/bill/late_fine_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount,
        to_acc=f"{bill.lender_id}/lender/pg_account/a",
        from_acc=f"{bill.id}/bill/interest_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount,
        to_acc=f"{bill.lender_id}/lender/pg_account/a",
        from_acc=f"{bill.id}/bill/principal_receivable/a",
    )
    return remaining_amount


def _adjust_for_min(
    session: Session, bills: List[LoanData], payment_received: Decimal, event_id: int
) -> Decimal:
    for bill in bills:
        min_due = bill.get_minimum_amount_to_pay(session)
        amount_to_adjust_in_this_bill = min(min_due, payment_received)
        # Remove amount from the original variable.
        payment_received -= amount_to_adjust_in_this_bill

        # Reduce min amount
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=amount_to_adjust_in_this_bill,
        )
        remaining_amount = _adjust_bill(session, bill, amount_to_adjust_in_this_bill, event_id)
        assert remaining_amount == 0  # Can't be more than 0
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_complete_bill(
    session: Session, bills: List[LoanData], payment_received: Decimal, event_id: int
) -> Decimal:
    for bill in bills:
        payment_received = _adjust_bill(session, bill, payment_received, event_id)
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_prepayment(
    session: Session, card_id: int, event_date: datetime, amount: Decimal
) -> None:
    lt = LedgerTriggerEvent(name="pre_payment", amount=amount, post_date=event_date)
    session.add(lt)
    session.flush()
    create_ledger_entry_from_str(
        session,
        event_id=lt.id,
        debit_book_str=f"62311/lender/lender_pg/a",
        credit_book_str=f"{card_id}/card/pre_payment/l",
        amount=amount,
    )


def accrue_interest_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/interest_receivable/a",
        credit_book_str=f"{bill.id}/bill/interest_earned/r",
        amount=event.amount,
    )


def accrue_late_fine_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/late_fine_receivable/a",
        credit_book_str=f"{bill.id}/bill/late_fine/r",
        amount=event.amount,
    )

    # Add into min amount of the bill too.
    add_min_amount_event(session, bill, event)


def refund_event(
    session: Session, bill: LoanData, user_card: UserCard, event: LedgerTriggerEvent
) -> None:

    # Refund before bill generation
    _, amount_billed = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    print(amount_billed)
    if amount_billed == 0:
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"62311/lender/merchant_refund/a",
            credit_book_str=f"{bill.id}/bill/unbilled/a",
            amount=event.amount,
        )
    else:
        if is_bill_closed(session, bill) == False:
            if amount_billed == event.amount:
                create_ledger_entry_from_str(
                    session,
                    event_id=event.id,
                    debit_book_str=f"62311/lender/merchant_refund/a",
                    credit_book_str=f"{bill.id}/bill/principal_receivable/a",
                    amount=event.amount,
                )
            else:
                amount = event.amount - amount_billed
                create_ledger_entry_from_str(
                    session,
                    event_id=event.id,
                    debit_book_str=f"62311/lender/merchant_refund/a",
                    credit_book_str=f"{bill.id}/bill/principal_receivable/a",
                    amount=amount_billed,
                )
                _, min_balance = get_account_balance_from_str(
                    session, book_string=f"{bill.id}/bill/min/a"
                )
                create_ledger_entry_from_str(
                    session,
                    event_id=event.id,
                    debit_book_str=f"{bill.id}/bill/min/l",
                    credit_book_str=f"{bill.id}/bill/min/a",
                    amount=min_balance,
                )
                amount = amount - min_balance
                if amount > 0:
                    _adjust_for_prepayment(session, user_card.id, event.post_date, amount)
        else:
            _adjust_for_prepayment(session, user_card.id, event.post_date, event.amount)


def lender_interest_incur_event(session: Session, event: LedgerTriggerEvent) -> None:
    last_lender_incur_trigger = (
        session.query(LedgerTriggerEvent.post_date)
        .filter(LedgerTriggerEvent.name.in_(["lender_interest_incur"]),)
        .order_by(LedgerTriggerEvent.post_date.desc())
        .offset(1)
        .first()
    )
    if last_lender_incur_trigger == None:
        last_lender_incur_trigger = event.post_date + dateutil.relativedelta.relativedelta(months=-1)
    no_of_days = (event.post_date - last_lender_incur_trigger).days
    all_user_cards = session.query(UserCard).all()

    for card in all_user_cards:
        book_account = get_book_account_by_string(
            session, book_string=f"{card.id}/card/lender_payable/l"
        )

        # credit interest for payable
        credit_balance_per_date = (
            session.query(
                cast(LedgerTriggerEvent.post_date, Date).label("post_date"),
                LedgerEntry.amount.label("amount"),
            )
            .order_by(LedgerTriggerEvent.post_date.desc())
            .filter(
                LedgerEntry.event_id == LedgerTriggerEvent.id,
                LedgerEntry.credit_account == book_account.id,
                # LedgerTriggerEvent.post_date <= event.post_date,
                # LedgerTriggerEvent.post_date >= last_lender_incur_trigger,
            )
            # .all()
            .subquery("credit_balance_per_date")
        )
        credit_balance = (
            session.query(
                credit_balance_per_date.c.post_date,
                func.sum(credit_balance_per_date.c.amount)
                .over(order_by=credit_balance_per_date.c.post_date)
                .label("amount"),
            )
            .group_by(credit_balance_per_date.c.post_date, credit_balance_per_date.c.amount)
            .order_by(credit_balance_per_date.c.post_date.desc())
            .subquery("credit_balance")
        )
        last_credit_balance = div(
            mul(
                (
                    Decimal(
                        session.query(
                            extract("day", (event.post_date - credit_balance.c.post_date))
                            * credit_balance.c.amount
                        )
                        .limit(1)
                        .scalar()
                        or 0
                    )
                ),
                18,
            ),
            36500,
        )
        remaining_credit_balance = session.query(
            extract("day", (credit_balance.c.post_date - last_lender_incur_trigger)).label("days"),
            credit_balance.c.amount,
        ).subquery("remaining_credit_balance")

        remaining_credit = session.query(
            (
                (
                    remaining_credit_balance.c.days
                    - func.coalesce(
                        func.lag(remaining_credit_balance.c.days).over(
                            order_by=remaining_credit_balance.c.days
                        ),
                        0,
                    )
                )
                * remaining_credit_balance.c.amount
                / 36500
            ).label("amount")
        ).subquery("remaing_credit")

        # debit interest for payable
        debit_balance_per_date = (
            session.query(
                cast(LedgerTriggerEvent.post_date, Date).label("post_date"),
                LedgerEntry.amount.label("amount"),
            )
            .order_by(LedgerTriggerEvent.post_date.desc())
            .filter(
                LedgerEntry.event_id == LedgerTriggerEvent.id,
                LedgerEntry.debit_account == book_account.id,
                # LedgerTriggerEvent.post_date <= event.post_date,
                # LedgerTriggerEvent.post_date >= last_lender_incur_trigger,
            )
            # .all()
            .subquery("debit_balance_per_date")
        )
        debit_balance = (
            session.query(
                debit_balance_per_date.c.post_date,
                func.sum(debit_balance_per_date.c.amount)
                .over(order_by=debit_balance_per_date.c.post_date)
                .label("amount"),
            )
            .group_by(debit_balance_per_date.c.post_date, debit_balance_per_date.c.amount)
            .order_by(debit_balance_per_date.c.post_date.desc())
            .subquery("debit_balance")
        )
        last_debit_balance = div(
            mul(
                (
                    Decimal(
                        session.query(
                            extract("day", (event.post_date - debit_balance.c.post_date))
                            * debit_balance.c.amount
                        )
                        .limit(1)
                        .scalar()
                        or 0
                    )
                ),
                18,
            ),
            36500,
        )
        remaining_debit_balance = session.query(
            extract("day", (debit_balance.c.post_date - last_lender_incur_trigger)).label("days"),
            debit_balance.c.amount,
        ).subquery("remaining_debit_balance")

        remaining_debit = session.query(
            (
                (
                    remaining_debit_balance.c.days
                    - func.coalesce(
                        func.lag(remaining_debit_balance.c.days).over(
                            order_by=remaining_debit_balance.c.days
                        ),
                        0,
                    )
                )
                * remaining_debit_balance.c.amount
                / 36500
            ).label("amount")
        ).subquery("remaing_debit")

        total_amount = (
            mul(Decimal(session.query(func.sum(remaining_credit.c.amount)).scalar() or 0), 18)
            + last_credit_balance
            - mul(Decimal(session.query(func.sum(remaining_debit.c.amount)).scalar() or 0), 18)
            - last_debit_balance
        )
        if total_amount > 0:
            create_ledger_entry_from_str(
                session,
                event_id=event.id,
                debit_book_str=f"{card.id}/redcarpet/redcarpet_expenses/l",
                credit_book_str=f"{card.id}/card/lender_payable/l",
                amount=total_amount,
            )


def writeoff_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/lender_expenses/l",
        amount=event.amount,
    )


def recovery_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    payment_received = Decimal(event.amount)
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)

    payment_received = _adjust_for_min(session, unpaid_bills, payment_received, event.id)
    payment_received = _adjust_for_complete_bill(session, unpaid_bills, payment_received, event.id)

    if payment_received > 0:
        _adjust_for_prepayment(session, user_card.id, event.post_date, payment_received)

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/pg_account/a",
        amount=payment_received,
    )
