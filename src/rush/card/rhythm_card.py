from typing import Type

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)


class RhythmBill(BaseBill):
    pass


class RhythmCard(BaseLoan):
    bill_class: Type[B] = RhythmBill

    __mapper_args__ = {"polymorphic_identity": "rhythm"}
