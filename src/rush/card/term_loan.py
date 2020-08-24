from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from rush.ledger_events import term_loan_creation_event
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
)
from rush.utils import div


class TermLoan(Loan):
    session: Session = None

    __mapper_args__ = {"polymorphic_identity": "term_loan"}

    def __init__(self, session: Session, **kwargs):
        self.session = session
        super().__init__(**kwargs)

    def prepare(self, session: Session) -> None:
        self.session = session

    def set_loan_data(self, **kwargs):
        loan_data = LoanData(
            user_id=kwargs["user_id"],
            loan_id=kwargs["loan_id"],
            lender_id=kwargs["lender_id"],
            bill_start_date=kwargs["bill_start_date"],
            bill_close_date=kwargs["bill_close_date"],
            bill_due_date=kwargs["bill_start_date"]
            + relativedelta(days=kwargs["interest_free_period_in_days"]),
            is_generated=True,
            bill_tenure=kwargs["tenure"],
            principal=kwargs["amount"],
            principal_instalment=div(kwargs["amount"], kwargs["tenure"]),
        )
        self.session.add(loan_data)
        self.session.flush()

    def trigger_loan_creation_event(self, **kwargs) -> None:
        event = LedgerTriggerEvent(
            performed_by=kwargs["user_id"],
            name="termloan_disbursal_event",
            loan_id=kwargs["loan_id"],
            post_date=kwargs["bill_start_date"],
            amount=kwargs["amount"],
        )
        self.session.add(event)
        self.session.flush()

        term_loan_creation_event(
            session=self.session, loan_id=self.loan_id, event=event, lender_id=self.lender_id
        )
