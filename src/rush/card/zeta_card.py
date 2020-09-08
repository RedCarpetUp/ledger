from rush.card.base_card import BaseLoan


class ZetaCard(BaseLoan):

    __mapper_args__ = {"polymorphic_identity": "zeta_card"}
