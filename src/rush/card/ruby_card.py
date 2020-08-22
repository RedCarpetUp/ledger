from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)


class RubyCard(BaseLoan):
    __mapper_args__ = {"polymorphic_identity": "ruby_card"}
    pass


class RubyBill(BaseBill):
    pass
