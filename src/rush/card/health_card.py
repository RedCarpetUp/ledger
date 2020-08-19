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

    @staticmethod
    def get_limit_type(mcc: str) -> str:
        return "available_limit" if mcc not in HEALTH_TXN_MCC else "health_limit"

    def get_split_payment(self, session: Session, payment_amount: Decimal) -> Dict[str, Decimal]:
        # TODO: change negative due calculation logic, once @raghav adds limit addition logic.
        _, non_medical_due = get_account_balance_from_str(
            session, book_string=f"{self.loan_id}/loan/available_limit/l"
        )

        _, medical_due = get_account_balance_from_str(
            session, book_string=f"{self.loan_id}/loan/health_limit/l"
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


class HealthBill(BaseBill):
    # todo: add implementation for health card bills.
    pass
