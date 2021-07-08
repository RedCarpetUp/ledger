from datetime import datetime
from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import (
    TIMESTAMP,
    String,
)

from rush.card.base_card import BaseLoan
from rush.models import (
    BookAccount,
    EventDpd,
    JournalEntry,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
    PaymentRequestsData,
    PaymentSplit,
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

    for (
        ledger_trigger_event,
        ledger_entry,
        debit_account,
        credit_account,
    ) in events_list:
        if (
            ledger_trigger_event.name
            in [
                "accrue_interest",
                "charge_late_fee",
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
                "fee_removed",
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
    if len(unpaid_emis) > 0:
        first_unpaid_emi = unpaid_emis[0]
        # Exception case in which if future emis are paid we consider the due date to be
        # the 15th of the next month rather than the actual due date of the first unpaid emi
        # So essentially dpd will never go below -45 in any case. More like shouldn't go.
        # ~Ananth
        from rush.anomaly_detection import get_last_payment_event

        last_payment_event = get_last_payment_event(session, user_loan)
        if last_payment_event:
            if isinstance(last_payment_event.post_date, datetime):
                event_post_date = last_payment_event.post_date.date()
            else:
                event_post_date = last_payment_event.post_date
        else:
            if isinstance(event.post_date, datetime):
                event_post_date = event.post_date.date()
            else:
                event_post_date = event.post_date
        min_due_date = event_post_date + relativedelta(months=+1, day=15)
        if first_unpaid_emi.due_date > min_due_date:
            user_loan.dpd = (event_post_date - min_due_date).days
        else:
            user_loan.dpd = first_unpaid_emi.dpd
        if not user_loan.ever_dpd or user_loan.dpd > user_loan.ever_dpd:
            user_loan.ever_dpd = user_loan.dpd

    session.flush()


def daily_dpd_update(session, user_loan, post_date):
    first_unpaid_mark = False
    loan_level_due_date = None
    event = LedgerTriggerEvent(name="daily_dpd_update", loan_id=user_loan.loan_id, post_date=post_date)
    session.add(event)
    if isinstance(event.post_date, datetime):
        daily_update_event_date = event.post_date.date()
    else:
        daily_update_event_date = event.post_date
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
    if len(unpaid_emis) > 0:
        first_unpaid_emi = unpaid_emis[0]
        # Exception case in which if future emis are paid we consider the due date to be
        # the 15th of the next month rather than the actual due date of the first unpaid emi
        # So essentially dpd will never go below -45 in any case. More like shouldn't go.
        # ~Ananth
        from rush.anomaly_detection import get_last_payment_event

        last_payment_event = get_last_payment_event(session, user_loan)
        if last_payment_event:
            if isinstance(last_payment_event.post_date, datetime):
                event_post_date = last_payment_event.post_date.date()
            else:
                event_post_date = last_payment_event.post_date
        else:
            if isinstance(event.post_date, datetime):
                event_post_date = event.post_date.date()
            else:
                event_post_date = event.post_date
        min_due_date = event_post_date + relativedelta(months=+1, day=15)
        if first_unpaid_emi.due_date > min_due_date:
            user_loan.dpd = (daily_update_event_date - min_due_date).days
        else:
            user_loan.dpd = first_unpaid_emi.dpd
        if not user_loan.ever_dpd or user_loan.dpd > user_loan.ever_dpd:
            user_loan.ever_dpd = user_loan.dpd
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
    loan_id,
    user_id,
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
        loan_id=loan_id,
        user_id=user_id,
    )
    session.add(entry)
    session.flush()
    return entry


def get_journal_entry_narration(event_name) -> String:
    if event_name == "late_fee":
        return "Late Fee"
    elif event_name == "atm_fee":
        return "ATM Fee"
    elif event_name in ("card_reload_fees", "card_upgrade_fees"):
        return "Reload Fee"
    elif event_name == "card_activation_fees":
        return "Processing Fee"
    elif event_name in ("payment_received", "loan_written_off", "customer_refund"):
        return "Receipt-Import"
    elif event_name == "transaction_refund":
        return "Payment Received From Merchant"


def get_journal_entry_ptype(event_name, is_term_loan=False) -> String:
    if event_name in ("charge_late_fee", "late_fee"):
        return "Late Fee-Card TL-Customer" if not is_term_loan else "Late Fee-TL-Customer"
    elif event_name in ("atm_fee_added", "atm_fee"):
        return "CF ATM Fee-Customer" if not is_term_loan else "ATM Fee-TL-Customer"
    elif event_name in (
        "reload_fee_added",
        "card_reload_fees",
        "upgrade_fee_added",
        "card_upgrade_fees",
    ):
        return "CF Reload Fee-Customer" if not is_term_loan else "Reload Fee-TL-Customer"
    elif event_name in ("pre_product_fee_added", "card_activation_fees", "reset_joining_fees"):
        return "CF Processing Fee-Customer" if not is_term_loan else "Processing Fee-TL-Customer"
    elif event_name == "payment_received":
        return "Card TL-Customer" if not is_term_loan else "TL-Customer"
    elif event_name in ("payment_received-unbilled", "payment_received-pre_payment"):
        return "CF-Customer" if not is_term_loan else "TL-Customer"
    elif event_name == "transaction_refund":
        return "Card TL-Merchant" if not is_term_loan else "TL-Merchant"
    elif event_name == "loan_written_off":
        return "Card TL-Redcarpet" if not is_term_loan else "TL-Redcarpet"
    elif event_name == "customer_refund":
        return "Refund CF-Customer" if not is_term_loan else "Refund TL-Customer"
    else:
        return f"{event_name.title()}-" + ("Card TL-Customer" if not is_term_loan else "TL-Customer")


def get_journal_entry_ledger_for_payment(event_name) -> String:
    if event_name in ("payment_received", "loan_written_off"):
        return "Axis Bank Ltd-Collections A/c"
    elif event_name == "transaction_refund":
        return "Cards upload A/c"
    elif event_name == "customer_refund":
        return "Axis Bank Ltd-Disbursement A/c"


def get_ledger_for_fee(fee_acc) -> String:
    if fee_acc == "late_fee":
        return "Late Fee"
    elif fee_acc in ("atm_fee", "reset_joining_fees", "card_activation_fees"):
        return "Processing Fee"
    elif fee_acc in ("card_reload_fees", "card_upgrade_fees"):
        return "Reload Fee"
    else:
        return fee_acc.upper()  # sgst, cgst.


def update_journal_entry(
    user_loan: BaseLoan,
    event: LedgerTriggerEvent,
    user_id: Optional[int] = None,
    session: Optional[Session] = None,
) -> None:

    from rush.card.utils import is_term_loan_subclass

    if not session:
        session = user_loan.session
    if not user_id:
        user_id = user_loan.user_id
    loan_id = None
    if user_loan:
        loan_id = user_loan.id

    if not event.amount:  # Don't need 0 amount bills entries.
        return
    query = """
        SELECT
            UPPER(
                CASE
                    WHEN (v3_user_documents.text_details_json ->> 'address_type'::text) IS NOT NULL AND length(v3_user_documents.text_details_json ->> 'name'::text) > 3
                    THEN v3_user_documents.text_details_json ->> 'name'::text
                    ELSE NULL::text
                END
            ) AS aadhar_name
        FROM v3_user_documents
        WHERE v3_user_documents.user_id = :user_id AND v3_user_documents.row_status::text = 'active'::text AND v3_user_documents.document_type::text = 'Aadhar'::text
        AND v3_user_documents.sequence = 1 AND v3_user_documents.verification_status::text = 'APPROVED'::text
    """
    user_name = session.execute(query, {"user_id": user_id}).scalar()

    if not user_name:
        user_name = (
            session.query(func.upper(UserData.first_name))
            .filter(UserData.row_status == "active", UserData.user_id == user_id)
            .scalar()
        ) or "John Doe"

    is_term_loan = is_term_loan_subclass(user_loan=user_loan)
    if event.name == "card_transaction" or event.name == "disbursal":
        ptype = ("Disbursal" + (" TL" if is_term_loan else " Card"),)
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
            ptype,
            event.id,
            loan_id,
            user_id,
        )
        create_journal_entry(
            session,
            "",
            event.post_date,
            "Cards upload A/c" if not is_term_loan else "Axis Bank Ltd-Disbursement A/c",
            "",
            "RedCarpet",
            0,
            event.amount,
            "",
            event.post_date,
            2,
            ptype,
            event.id,
            loan_id,
            user_id,
        )
    elif event.name in ["payment_received", "transaction_refund", "loan_written_off", "customer_refund"]:
        payment_request_data = (
            session.query(PaymentRequestsData)
            .filter(
                PaymentRequestsData.payment_request_id == event.extra_details["payment_request_id"],
                PaymentRequestsData.row_status == "active",
            )
            .first()
        )
        total_gateway_expenses = payment_request_data.payment_execution_charges or 0
        actual_gateway_expenses = (
            event.amount / payment_request_data.payment_request_amount
        ) * total_gateway_expenses
        actual_gateway_expenses = round(actual_gateway_expenses, 2)
        settlement_date = payment_request_data.payment_received_in_bank_date
        # loan id to take prepayment from that loan only
        payment_split_data = (
            session.query(PaymentSplit.component, PaymentSplit.amount_settled)
            .filter(
                PaymentSplit.payment_request_id == event.extra_details["payment_request_id"],
                PaymentSplit.component.in_(["pre_payment"]),
                PaymentSplit.loan_id == user_loan.loan_id,
            )
            .all()
        )
        prepayment_amount = 0
        for split_data in payment_split_data:
            if split_data[0] == "pre_payment":
                prepayment_amount = split_data[1]
        for count in range(len(payment_split_data) + 1):
            if count == len(payment_split_data):
                gateway_expenses = actual_gateway_expenses
                amount = event.amount - gateway_expenses - prepayment_amount
                p_type = get_journal_entry_ptype(event.name, is_term_loan=is_term_loan)
                narration_name = get_journal_entry_narration(event.name)
            else:
                amount = payment_split_data[count][1]
                gateway_expenses = actual_gateway_expenses if event.amount == prepayment_amount else 0
                amount = amount - gateway_expenses
                if event.name == "payment_received":
                    narration_name = "Receipt-Import"
                    p_type = "TL-Customer" if is_term_loan else "CF-Customer"
                elif event.name == "transaction_refund":
                    narration_name = "Payment Received From Merchant"
                    p_type = "TL-Merchant" if is_term_loan else "CF-Merchant"
                else:
                    p_type = get_journal_entry_ptype(event.name, is_term_loan=is_term_loan)
                    narration_name = get_journal_entry_narration(event.name)

            if amount <= 0:
                continue
            if payment_request_data.type not in ("collection"):
                loan_id = None
                p_type = "CF-Customer"
            payment_received_journal_entry(
                event,
                settlement_date,
                user_name,
                narration_name,
                p_type,
                session,
                amount,
                gateway_expenses,
                loan_id,
                user_id,
            )
        from rush.payments import get_payment_split_from_event

        split_data = get_payment_split_from_event(session, event)
        filtered_split_data = {}
        for key, value in split_data.items():
            if key != "pre_payment":
                filtered_split_data[key] = value
        principal_and_interest = filtered_split_data.pop("principal", 0) + filtered_split_data.pop(
            "interest", 0
        )
        is_term_loan_unbilled = is_term_loan and filtered_split_data.get("unbilled", False)
        # if there is something else apart from principal and interest.
        if filtered_split_data and not is_term_loan_unbilled and event.amount != principal_and_interest:
            TL = " TL" if is_term_loan else ""
            sales_import_amount = 0
            narration_name = ""
            fee_count = 0
            event_name = ""
            # First loop to get narration name.
            for (
                settled_acc,
                _,
            ) in filtered_split_data.items():
                # So if there are more than one fee, it becomes "Late fee Reload fee".
                if settled_acc not in ("sgst", "cgst", "igst"):
                    fee_count += 1
                    event_name = settled_acc
                    narration_name += f"{get_ledger_for_fee(settled_acc)}"
            narration_name = narration_name.strip()
            if fee_count == 1:
                if payment_request_data.type not in ("collection"):
                    is_term_loan = False
                    TL = ""
                p_type = get_journal_entry_ptype(event_name, is_term_loan=is_term_loan)
                if event.name == "loan_written_off":
                    p_type = p_type.replace("Customer", "Redcarpet")
            else:
                p_type = (
                    f"{narration_name} -TL-Customer"
                    if is_term_loan
                    else f"{narration_name} -Card TL-Customer"
                )
            for sort_order, (settled_acc, amount) in enumerate(filtered_split_data.items(), 2):
                sales_import_amount += amount
                create_journal_entry(
                    session,
                    "",
                    settlement_date,
                    get_ledger_for_fee(settled_acc),
                    "",
                    "RedCarpet" + TL,
                    0,
                    amount,
                    "",
                    settlement_date,
                    sort_order,
                    p_type,
                    event.id,
                    loan_id,
                    user_id,
                )
            create_journal_entry(
                session,
                "Sales-Import",
                settlement_date,
                user_name,
                "",
                "RedCarpet" + TL,
                sales_import_amount,
                0,
                narration_name,
                settlement_date,
                1,
                p_type,
                event.id,
                loan_id,
                user_id,
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
            loan_id,
            user_id,
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
            loan_id,
            user_id,
        )


def payment_received_journal_entry(
    event: LedgerTriggerEvent,
    settlement_date: DateTime,
    user_name: str,
    narration_name: str,
    p_type: str,
    session: Optional[Session] = None,
    amount: Decimal = Decimal(0),
    gateway_expenses: Decimal = Decimal(0),
    loan_id: int = None,
    user_id: Optional[int] = None,
) -> None:
    create_journal_entry(
        session,
        "Receipt-Import",
        settlement_date,
        get_journal_entry_ledger_for_payment(event.name),
        "",
        "RedCarpet",
        amount,
        0,
        narration_name,
        settlement_date,
        1,
        p_type,
        event.id,
        loan_id,
        user_id,
    )
    create_journal_entry(
        session,
        "",
        settlement_date,
        "Bank Charges(RC)",
        "",
        "RedCarpet",
        gateway_expenses,
        0,
        "",
        settlement_date,
        2,
        p_type,
        event.id,
        loan_id,
        user_id,
    )
    create_journal_entry(
        session,
        "",
        settlement_date,
        user_name,
        "",
        "RedCarpet",
        0,
        amount + gateway_expenses,
        "",
        settlement_date,
        3,
        p_type,
        event.id,
        loan_id,
        user_id,
    )
