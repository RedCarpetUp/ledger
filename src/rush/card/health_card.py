from rush.card.base_card import (
    BaseBill,
    BaseCard,
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
    @staticmethod
    def get_limit_type(mcc: str) -> str:
        return "available_limit" if mcc not in HEALTH_TXN_MCC else "health_limit"


class HealthBill(BaseBill):
    # todo: add implementation for health card bills.
    pass
