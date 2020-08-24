from decimal import Decimal

from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from rush.card.utils import get_product_id_from_card_type
from rush.ledger_events import loan_disbursement_event
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
)
from rush.utils import (
    div,
    get_current_ist_time,
)


class TermLoan(Loan):
    session: Session = None

    __mapper_args__ = {"polymorphic_identity": "term_loan"}

    def __init__(self, session: Session, **kwargs):
        self.session = session
        super().__init__(**kwargs)

    def prepare(self, session: Session) -> None:
        self.session = session

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        loan = cls(
            session=session,
            user_id=kwargs["user_id"],
            product_id=get_product_id_from_card_type(session=session, card_type=kwargs["card_type"]),
            lender_id=kwargs["lender_id"],
            rc_rate_of_interest_monthly=Decimal(3),
            lender_rate_of_interest_annual=Decimal(18),
            amortization_date=kwargs.get(
                "loan_creation_date", get_current_ist_time().date()
            ),  # TODO: change this later.
        )
        session.add(loan)
        session.flush()

        kwargs["loan_id"] = loan.id

        loan.set_loan_data(**kwargs)
        loan.trigger_loan_creation_event(**kwargs)

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
        session.add(loan_data)
        session.flush()

        event = LedgerTriggerEvent(
            performed_by=kwargs["user_id"],
            name="termloan_disbursal_event",
            loan_id=kwargs["loan_id"],
            post_date=kwargs["bill_start_date"],
            amount=kwargs["amount"],
        )

        session.add(event)
        session.flush()

        loan_disbursement_event(
            session=session, loan=loan, event=event,
        )

        return loan
