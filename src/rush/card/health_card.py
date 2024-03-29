from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import LedgerTriggerEvent

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


class HealthBill(BaseBill):
    # todo: add implementation for health card bills.
    pass


class HealthCard(BaseLoan):
    should_reinstate_limit_on_payment: bool = True
    bill_class: Type[B] = HealthBill

    __mapper_args__ = {"polymorphic_identity": "health_card"}

    @staticmethod
    def get_limit_type(mcc: str) -> str:
        return "available_limit" if mcc not in HEALTH_TXN_MCC else "health_limit"

    def get_split_payment(self, payment_amount: Decimal) -> Dict[str, Decimal]:
        # TODO: change negative due calculation logic, once @raghav adds limit addition logic.
        _, non_medical_due = get_account_balance_from_str(
            session=self.session, book_string=f"{self.loan_id}/card/available_limit/l"
        )

        _, medical_due = get_account_balance_from_str(
            session=self.session, book_string=f"{self.loan_id}/card/health_limit/l"
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

    def reinstate_limit_on_payment(self, event: LedgerTriggerEvent, amount: Decimal) -> None:
        settlement_limit = self.get_split_payment(payment_amount=amount)

        # settling medical limit
        create_ledger_entry_from_str(
            session=self.session,
            event_id=event.id,
            debit_book_str=f"{self.loan_id}/card/health_limit/a",
            credit_book_str=f"{self.loan_id}/card/health_limit/l",
            amount=settlement_limit["medical"],
        )

        # settling non medical limit
        # this creates available_limit account entry
        super().reinstate_limit_on_payment(event=event, amount=settlement_limit["non_medical"])
