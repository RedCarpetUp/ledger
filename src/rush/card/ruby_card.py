from typing import Type

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)


class RubyBill(BaseBill):
    pass


class RubyCard(BaseLoan):
    bill_class: Type[B] = RubyBill

    __mapper_args__ = {"polymorphic_identity": "ruby"}
