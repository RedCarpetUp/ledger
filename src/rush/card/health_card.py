from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from sqlalchemy.orm import Session

from rush.card.base_card import (
    B,
    BaseBill,
    BaseCard,
)
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    UserCard,
)

HEALTH_TXN_MCC = [
    "8011",
    "8021",
    "8031",
    "8041",
    "8042",
    "8043",
    "8049",
    "8050",
    "8062",
    "8071",
    "8099",
    "5912",
]


class HealthCard(BaseCard):
    # todo: add implementation for health card.
    def __init__(self, session: Session, bill_class: Type[B], user_card: UserCard, loan: Loan):
        super().__init__(session=session, bill_class=bill_class, user_card=user_card, loan=loan)
        self.multiple_limits = True
        self.should_reinstate_limit_on_payment = True

    @staticmethod
    def get_limit_type(mcc: str) -> str:
        return "available_limit" if mcc not in HEALTH_TXN_MCC else "health_limit"

    def get_split_payment(self, session: Session, payment_amount: Decimal) -> Dict[str, Decimal]:
        # TODO: change negative due calculation logic, once @raghav adds limit addition logic.
        _, non_medical_due = get_account_balance_from_str(
            session, book_string=f"{self.loan_id}/card/available_limit/l"
        )

        _, medical_due = get_account_balance_from_str(
            session, book_string=f"{self.loan_id}/card/health_limit/l"
        )

        medical_settlement = Decimal(Decimal(0.9) * payment_amount)
        non_medical_settlement = Decimal(Decimal(0.1) * payment_amount)

        non_medical_settlement += medical_settlement - min(-1 * medical_due, medical_settlement)
        medical_settlement = min(-1 * medical_due, medical_settlement)

        if non_medical_settlement > -1 * non_medical_due:
            if medical_settlement < -1 * medical_due:
                medical_settlement += non_medical_settlement - (-1 * non_medical_due)
                non_medical_settlement = -1 * non_medical_due

        return {
            "medical": Decimal(round(medical_settlement)),
            "non_medical": Decimal(round(non_medical_settlement)),
        }

    def reinstate_limit_on_payment(
        self, session: Session, event: LedgerTriggerEvent, amount: Decimal
    ) -> None:
        settlement_limit = self.get_split_payment(session=session, payment_amount=amount)

        from rush.ledger_events import health_limit_assignment_event

        # settling medical limit
        health_limit_assignment_event(
            session=session,
            loan_id=self.loan_id,
            event=event,
            amount=settlement_limit["medical"],
            limit_str="health_limit",
        )

        # settling non medical limit
        health_limit_assignment_event(
            session=session,
            loan_id=self.loan_id,
            event=event,
            amount=settlement_limit["non_medical"],
            limit_str="available_limit",
        )


class HealthBill(BaseBill):
    # todo: add implementation for health card bills.
    pass
