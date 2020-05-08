from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry,
    get_book_account_by_string,
    get_account_balance,
)
from rush.models import LedgerTriggerEvent, LoanData, LoanEmis, CardTransaction


def card_transaction_event(session: Session, user_id: int, event: LedgerTriggerEvent) -> None:
    amount = event.amount
    # swipe_id = event.extra_details["swipe_id"]
    # bill_id = session.query(CardTransaction.loan_id).filter_by(id=swipe_id).scalar()
    # Get money from lender pool account to lender limit used.
    lender_pool_account = get_book_account_by_string(
        session=session, book_string="62311/lender/pool_account/l"
    )
    lender_limit_utilized = get_book_account_by_string(
        session=session, book_string="62311/lender/limit_utilized/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=lender_pool_account.id,
        to_book_id=lender_limit_utilized.id,
        amount=amount,
    )

    # Reduce user's card balance TODO This goes in negative right now. Need to add load money event.
    user_card_balance = get_book_account_by_string(
        session, book_string=f"{user_id}/user/user_card_balance/l"
    )
    unbilled_transactions = get_book_account_by_string(
        session, book_string=f"{user_id}/user/unbilled_transactions/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=user_card_balance.id,
        to_book_id=unbilled_transactions.id,
        amount=amount,
    )


def bill_close_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    bill_tenure = 12
    interest_monthly = 3
    unbilled_book = get_book_account_by_string(
        session, book_string=f"{bill.id}/bill/unbilled_transactions/a"
    )
    unbilled_balance = get_account_balance(session=session, book_account=unbilled_book)
    principal_per_month = unbilled_balance / bill_tenure
    interest_amount_per_month = unbilled_balance * interest_monthly / 100
    total_interest = interest_amount_per_month * bill_tenure
    total_bill_amount = unbilled_balance + total_interest

    # Create schedule.
    for schedule_count in range(bill_tenure):
        schedule_due_date = bill.agreement_date + relativedelta(months=schedule_count)
        schedule = LoanEmis.new(session, loan_id=bill.id, due_date=schedule_due_date)

        principal_due_book = get_book_account_by_string(
            session, book_string=f"{schedule.id}/emi/principal_due/a"
        )
        create_ledger_entry(
            session,
            event_id=event.id,
            from_book_id=unbilled_book.id,
            to_book_id=principal_due_book.id,
            amount=principal_per_month,
        )

        dummy_interest = get_book_account_by_string(
            session, book_string=f"{schedule.id}/emi/interest_due/l"
        )
        interest_due = get_book_account_by_string(
            session, book_string=f"{schedule.id}/emi/interest_due/a"
        )
        create_ledger_entry(
            session,
            event_id=event.id,
            from_book_id=dummy_interest.id,
            to_book_id=interest_due.id,
            amount=interest_amount_per_month,
        )
