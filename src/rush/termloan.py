from decimal import Decimal
from typing import (
    Any,
    Dict,
)

from dateutil.relativedelta import relativedelta
from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import (
    LedgerTriggerEvent,
    LoanData,
)
from rush.utils import (
    div,
    get_current_ist_time,
    mul,
)


def create_user_term_loan(
    session: Session,
    user_id: int,
    bill_start_date: Date,
    bill_close_date: Date,
    lender_id: int,
    amount: Decimal,
    tenure: int,
    interest_free_period_in_days: int,
) -> Dict[str, Any]:

    if bill_start_date < get_current_ist_time().date():
        return {"result": "error", "message": "Loan date before disbursal"}
    new_disbursal = LoanData(
        user_id=user_id,
        lender_id=lender_id,
        bill_start_date=bill_start_date,
        bill_close_date=bill_close_date,
        bill_due_date=bill_start_date + relativedelta(days=interest_free_period_in_days),
        is_generated=True,
        bill_tenure=tenure,
        principal=amount,
        principal_instalment=div(amount, tenure),
    )
    session.add(new_disbursal)
    session.flush()
    event = LedgerTriggerEvent(
        performed_by=user_id,
        name="Tenure loan Disbursal",
        card_id=None,
        post_date=bill_start_date,
        amount=amount,
    )
    session.add(event)
    session.flush()
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{lender_id}/lender/lender_capital/l",
        credit_book_str=f"{new_disbursal.id}/loan/lender_payable/l",
        amount=amount,
    )
    # Reduce money from lender's pool account
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{new_disbursal.id}/bill/billed/a",
        credit_book_str=f"{lender_id}/lender/pool_balance/a",
        amount=amount,
    )

    return {"result": "success", "message": "Term loan created", "loan": new_disbursal}
