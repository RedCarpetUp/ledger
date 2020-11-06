from datetime import datetime
from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import String

from rush.card.base_card import BaseLoan
from rush.models import (
    BookAccount,
    EventDpd,
    JournalEntry,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
    UserData,
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
        bill_dpd = -999
        all_emis = user_loan.get_loan_schedule(only_unpaid_emis=True)
        for emi in all_emis:
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
    unpaid_emis = user_loan.get_loan_schedule(only_unpaid_emis=True)
    if unpaid_emis:
        first_unpaid_emi = unpaid_emis[0]
        user_loan.dpd = first_unpaid_emi.dpd
        if not user_loan.ever_dpd or first_unpaid_emi.dpd > user_loan.ever_dpd:
            user_loan.ever_dpd = first_unpaid_emi.dpd

    session.flush()


def daily_dpd_update(session, user_loan, post_date):
    first_unpaid_mark = False
    loan_level_due_date = None
    event = LedgerTriggerEvent(name="daily_dpd_update", loan_id=user_loan.loan_id, post_date=post_date)
    session.add(event)
    all_emis = user_loan.get_loan_schedule(only_unpaid_emis=True)
    for emi in all_emis:
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
    unpaid_emis = user_loan.get_loan_schedule(only_unpaid_emis=True)
    if unpaid_emis:
        first_unpaid_emi = unpaid_emis[0]
        user_loan.dpd = first_unpaid_emi.dpd
        if not user_loan.ever_dpd or first_unpaid_emi.dpd > user_loan.ever_dpd:
            user_loan.ever_dpd = first_unpaid_emi.dpd
    session.flush()


def create_journal_entry(
    session,
    voucher_type,
    date_ledger,
    ledger,
    alias,
    group_name,
    debit,
    credit,
    narration,
    instrument_date,
    sort_order,
    ptype,
    event_id,
):
    entry = JournalEntry(
        voucher_type=voucher_type,
        date_ledger=date_ledger,
        ledger=ledger,
        alias=alias,
        group_name=group_name,
        debit=debit,
        credit=credit,
        narration=narration,
        instrument_date=instrument_date,
        sort_order=sort_order,
        ptype=ptype,
        event_id=event_id,
    )
    session.add(entry)
    session.flush()
    return entry


def get_journal_entry_narration(event_name) -> String:
    if event_name == "charge_late_fine":
        return "Late Fee"
    elif event_name == "atm_fee_added":
        return "ATM Fee"
    elif event_name == "reload_fee_added":
        return "Reload Fee"
    elif event_name == "processing_fee_added":
        return "Processing Fee"
    elif event_name == "payment_received":
        return "Receipt-Import"
    elif event_name == "transaction_refund":
        return "Payment Received From Merchant"


def get_journal_entry_ptype(event_name) -> String:
    if event_name == "charge_late_fine":
        return "Late Fee-Card TL-Customer"
    elif event_name == "atm_fee_added":
        return "CF ATM Fee-Customer"
    elif event_name == "reload_fee_added":
        return "CF Reload Fee-Customer"
    elif event_name == "processing_fee_added":
        return "CF Processing Fee-Customer"
    elif event_name == "payment_received":
        return "CF-Customer"
    elif event_name == "transaction_refund":
        return "CF-Merchant"


def get_journal_entry_ledger_for_payment(event_name) -> String:
    if event_name == "payment_received":
        return "Axis Bank Ltd-Collections A/c"
    elif event_name == "transaction_refund":
        return "Cards Upload A/c"


def update_journal_entry(
    user_loan: BaseLoan,
    event: LedgerTriggerEvent,
) -> None:
    # TODO Think about alias accounts. Also what is processing and reload event?
    session = user_loan.session
    user_name = ""
    user_data = (
        session.query(UserData)
        .filter(UserData.row_status == "active", UserData.user_id == user_loan.user_id)
        .one_or_none()
    )
    if user_data:
        user_name = user_data.first_name + " " + user_data.last_name
    if event.name == "card_transaction":
        create_journal_entry(
            session,
            "Journal-Disbursement",
            event.post_date,
            user_name,
            "",
            "RedCarpet",
            event.amount,
            0,
            "RedCarpet Disbursement",
            event.post_date,
            1,
            "Disbursal Card",
            event.id,
        )
        create_journal_entry(
            session,
            "",
            event.post_date,
            "Cards Upload A/C",
            "",
            "RedCarpet",
            0,
            event.amount,
            "",
            event.post_date,
            2,
            "Disbursal Card",
            event.id,
        )
    elif (
        event.name == "charge_late_fine"
        or event.name == "atm_fee_added"
        or event.name == "reload_fee_added"
    ):
        from rush.utils import get_gst_split_from_amount

        d = get_gst_split_from_amount(
            event.amount, sgst_rate=Decimal(0), cgst_rate=Decimal(0), igst_rate=Decimal(18)
        )
        create_journal_entry(
            session,
            "Sales-Import",
            event.post_date,
            user_name,
            "",
            "RedCarpet",
            event.amount,
            0,
            get_journal_entry_narration(event.name),
            event.post_date,
            1,
            get_journal_entry_ptype(event.name),
            event.id,
        )
        create_journal_entry(
            session,
            "",
            event.post_date,
            get_journal_entry_narration(event.name),
            "",
            "RedCarpet",
            0,
            d["net_amount"],
            "",
            event.post_date,
            2,
            get_journal_entry_ptype(event.name),
            event.id,
        )
        create_journal_entry(
            session,
            "",
            event.post_date,
            "IGST",
            "",
            "RedCarpet",
            0,
            d["igst"],
            "",
            event.post_date,
            3,
            get_journal_entry_ptype(event.name),
            event.id,
        )
    elif event.name == "payment_received" or event.name == "transaction_refund":
        create_journal_entry(
            session,
            "Receipt-Import",
            event.post_date,
            get_journal_entry_ledger_for_payment(event.name),
            "",
            "RedCarpet",
            event.amount,
            0,
            get_journal_entry_narration(event.name),
            event.post_date,
            1,
            get_journal_entry_ptype(event.name),
            event.id,
        )
        create_journal_entry(
            session,
            "",
            event.post_date,
            user_name,
            "",
            "RedCarpet",
            0,
            event.amount,
            "",
            event.post_date,
            2,
            get_journal_entry_ptype(event.name),
            event.id,
        )
    elif event.name == "bill_generate":
        create_journal_entry(
            session,
            "Journal-Import",
            event.post_date,
            user_name,
            "",
            "RedCarpet",
            event.amount,
            0,
            "From/To",
            event.post_date,
            1,
            "CF To TL",
            event.id,
        )
        create_journal_entry(
            session,
            "Journal-Import",
            event.post_date,
            user_name,
            "",
            "RedCarpet",
            0,
            event.amount,
            "From/To",
            event.post_date,
            2,
            "CF To TL",
            event.id,
        )
