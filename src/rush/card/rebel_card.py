from typing import Type

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)


class RebelBill(BaseBill):
    pass


class RebelCard(BaseLoan):
    bill_class: Type[B] = RebelBill

    __mapper_args__ = {"polymorphic_identity": "rebel"}
