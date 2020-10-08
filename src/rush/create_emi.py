from datetime import datetime
from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql import func

from rush.card.base_card import BaseLoan
from rush.models import (
    BookAccount,
    EventDpd,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
)


def update_event_with_dpd(
    user_loan: BaseLoan,
    to_date: DateTime = None,
    from_date: DateTime = None,
    event: LedgerTriggerEvent = None,
) -> None:
    def actual_event_update(
        session: Session,
        is_debit: bool,
        ledger_trigger_event,
        ledger_entry,
        account,
    ):
        if not is_debit:
            debit_amount = Decimal(0)
            credit_amount = ledger_entry.amount
            account_id = account.identifier
            account_balance = ledger_entry.debit_account_balance
        else:
            debit_amount = ledger_entry.amount
            credit_amount = Decimal(0)
            account_id = account.identifier
            account_balance = ledger_entry.credit_account_balance

        if isinstance(ledger_trigger_event.post_date, datetime):
            event_post_date = ledger_trigger_event.post_date.date()
        else:
            event_post_date = ledger_trigger_event.post_date
        # In case of moratorium reset all post dates to start of moratorium
        if LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=event_post_date
        ):
            moratorium = (
                session.query(LoanMoratorium).filter(LoanMoratorium.loan_id == user_loan.loan_id).first()
            )
            event_post_date = moratorium.start_date

        # We need to get the bill because we have to check if min is paid
        bill = user_loan.convert_to_bill_class(
            (
                session.query(LoanData)
                .filter(
                    LoanData.loan_id == user_loan.loan_id,
                    LoanData.id == account_id,
                )
                .first()
            )
        )

        # Adjust dpd in loan schedule
        first_unpaid_mark = False
        bill_dpd = 0
        all_emis = user_loan.get_loan_schedule()
        for emi in all_emis:
            if emi.payment_status != "Paid":
                if not first_unpaid_mark:
                    first_unpaid_mark = True
                    # Bill dpd
                    # Only calculate bill dpd is min is not 0
                    if bill and bill.get_remaining_min() > 0:
                        bill_dpd = (event_post_date - emi.due_date).days
                # Schedule dpd
                schedule_dpd = (event_post_date - emi.due_date).days
                # We should only consider the daily dpd event for increment
                if schedule_dpd >= emi.dpd:
                    emi.dpd = schedule_dpd

        new_event = EventDpd(
            bill_id=account_id,
            loan_id=user_loan.loan_id,
            event_id=ledger_trigger_event.id,
            credit=credit_amount,
            debit=debit_amount,
            balance=account_balance,
            dpd=bill_dpd,
        )
        session.add(new_event)

    session = user_loan.session

    debit_book_account = aliased(BookAccount)
    credit_book_account = aliased(BookAccount)
    events_list = session.query(
        LedgerTriggerEvent, LedgerEntry, debit_book_account, credit_book_account
    ).filter(
        LedgerEntry.event_id == LedgerTriggerEvent.id,
        LedgerEntry.debit_account == debit_book_account.id,
        LedgerEntry.credit_account == credit_book_account.id,
    )
    if from_date and to_date:
        events_list = events_list.filter(
            LedgerTriggerEvent.post_date > from_date,
            LedgerTriggerEvent.post_date <= to_date,
        )
    elif to_date:
        events_list = events_list.filter(
            LedgerTriggerEvent.post_date <= to_date,
        )

    if event:
        events_list = events_list.filter(LedgerTriggerEvent.id == event.id)
    events_list = events_list.order_by(LedgerTriggerEvent.post_date.asc()).all()

    for ledger_trigger_event, ledger_entry, debit_account, credit_account in events_list:
        if (
            ledger_trigger_event.name
            in [
                "accrue_interest",
                "charge_late_fine",
                "atm_fee_added",
            ]
            and debit_account.identifier_type == "bill"
            and debit_account.book_name == "max"
        ):
            actual_event_update(session, False, ledger_trigger_event, ledger_entry, debit_account)

        elif (
            ledger_trigger_event.name
            in [
                "card_transaction",
            ]
            and debit_account.identifier_type == "bill"
            and debit_account.book_name == "unbilled"
        ):
            actual_event_update(session, False, ledger_trigger_event, ledger_entry, debit_account)

        elif (
            ledger_trigger_event.name
            in [
                "reverse_interest_charges",
                "reverse_late_charges",
                "payment_received",
                "transaction_refund",
            ]
            and credit_account.identifier_type == "bill"
            and credit_account.book_name == "max"
        ):
            actual_event_update(session, True, ledger_trigger_event, ledger_entry, credit_account)

        else:
            continue

    # Calculate card level dpd
    max_dpd = session.query(func.max(EventDpd.dpd).label("max_dpd")).one()
    user_loan.dpd = max_dpd.max_dpd
    if not user_loan.ever_dpd or max_dpd.max_dpd > user_loan.ever_dpd:
        user_loan.ever_dpd = max_dpd.max_dpd

    session.flush()


def daily_dpd_update(session, user_loan, post_date):
    first_unpaid_mark = False
    loan_level_due_date = None
    event = LedgerTriggerEvent(name="daily_dpd_update", loan_id=user_loan.loan_id, post_date=post_date)
    session.add(event)
    all_emis = user_loan.get_loan_schedule()
    for emi in all_emis:
        if emi.payment_status != "Paid":
            if not first_unpaid_mark:
                first_unpaid_mark = True
                loan_level_due_date = emi.due_date
            # Schedule dpd
            dpd = (post_date.date() - emi.due_date).days
            # We should only consider the daily dpd event for increment
            if dpd >= emi.dpd:
                emi.dpd = dpd

    unpaid_bills = user_loan.get_unpaid_bills()
    for bill in unpaid_bills:
        if first_unpaid_mark and loan_level_due_date:
            bill_dpd = (post_date.date() - loan_level_due_date).days
            new_event = EventDpd(
                bill_id=bill.id,
                loan_id=user_loan.loan_id,
                event_id=event.id,
                credit=Decimal(0),
                debit=Decimal(0),
                balance=bill.get_remaining_max(),
                dpd=bill_dpd,
            )
            session.add(new_event)

    # Calculate card level dpd
    max_dpd = session.query(func.max(EventDpd.dpd).label("max_dpd")).one()
    user_loan.dpd = max_dpd.max_dpd
    if not user_loan.ever_dpd or max_dpd.max_dpd > user_loan.ever_dpd:
        user_loan.ever_dpd = max_dpd.max_dpd
    session.flush()
