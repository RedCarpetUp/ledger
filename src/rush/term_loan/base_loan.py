from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from rush.card.utils import get_product_id_from_card_type
from rush.ledger_events import term_loan_creation_event
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
)
from rush.utils import div

class BaseLoan:
    session: Session = None

    def __init__(self, session: Session):
        self.session = session

    @staticmethod
    def create(session: Session, **kwargs) -> LoanData:
        loan = Loan(
            user_id=kwargs["user_id"],
            product_id=get_product_id_from_card_type(session=session, card_type=kwargs["product_type"]),
        )
        session.add(loan)
        session.flush()

        kwargs["loan_id"] = loan.id

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
            name="Tenure loan Disbursal",
            card_id=None,
            post_date=kwargs["bill_start_date"],
            amount=kwargs["amount"],
        )
        session.add(event)
        session.flush()

        term_loan_creation_event(session=session, loan=loan_data, event=event, lender_id=kwargs["lender_id"])

        return loan_data
