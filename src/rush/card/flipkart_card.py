from decimal import Decimal

from sqlalchemy import func

from rush.card.base_card import (
    BaseBill,
    BaseCard,
)
from rush.models import CardTransaction
from rush.utils import (
    div,
    mul,
    round_up_decimal,
)


class FlipkartCard(BaseCard):
    def accure_interest_from_flipkart(self) -> None:
        pass


class FlipkartBill(BaseBill):
    def get_interest_to_charge(self, rate_of_interest: Decimal) -> Decimal:
        flipkart_transaction_amount = (
            self.session.query(func.sum(CardTransaction.amount))
            .filter(CardTransaction.loan_id == self.id, CardTransaction.description == "Flipkart")
            .scalar()
            or 0
        )
        principal_without_flipkart_txns = self.table.principal - flipkart_transaction_amount
        interest_on_new_principal = mul(
            principal_without_flipkart_txns, div(div(rate_of_interest, 12), 100)
        )

        not_rounded_emi = self.table.principal_instalment + interest_on_new_principal
        rounded_emi = round_up_decimal(not_rounded_emi)

        rounding_difference = rounded_emi - not_rounded_emi

        new_interest = interest_on_new_principal + rounding_difference
        return new_interest