from typing import Type

from sqlalchemy.orm import Session

from rush.card.base_card import (
    B,
    BaseBill,
    BaseCard,
)
from rush.models import UserCard

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
    def __init__(self, session: Session, bill_class: Type[B], user_card: UserCard):
        super().__init__(session=session, bill_class=bill_class, user_card=user_card)
        self.multiple_limits = True

    @staticmethod
    def get_limit_type(mcc: str) -> str:
        return "available_limit" if mcc not in HEALTH_TXN_MCC else "health_limit"


class HealthBill(BaseBill):
    # todo: add implementation for health card bills.
    pass
