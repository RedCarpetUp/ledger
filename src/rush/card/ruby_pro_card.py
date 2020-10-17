from typing import Type

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)


class RubyProBill(BaseBill):
    pass


class RubyProCard(BaseLoan):
    bill_class: Type[B] = RubyProBill

    __mapper_args__ = {"polymorphic_identity": "rubypro"}
