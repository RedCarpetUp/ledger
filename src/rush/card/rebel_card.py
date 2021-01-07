from decimal import Decimal
from operator import add
from typing import (
    List,
    Type,
)

from sqlalchemy.orm import session
from sqlalchemy.sql.sqltypes import (
    DateTime,
    Integer,
)

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.card.transaction_loan import TransactionLoan
from rush.models import LedgerTriggerEvent


class RebelBill(BaseBill):
    pass


class RebelCard(BaseLoan):
    bill_class: Type[B] = RebelBill

    def get_transaction_loans(self) -> List[TransactionLoan]:
        return (
            self.session.query(TransactionLoan)
            .join(LedgerTriggerEvent, LedgerTriggerEvent.extra_details["child_loan_id"].astext.cast(Integer) == TransactionLoan.id)
            .filter(
                LedgerTriggerEvent.name == "transaction_to_loan",
                LedgerTriggerEvent.loan_id == self.id,
            )
            .all()
        )

    def get_remaining_min(self, date_to_check_against: DateTime) -> Decimal:
        txn_loans = self.get_transaction_loans()

        txn_loans_remaining_min_sum = sum(
            loan.get_remaining_min(date_to_check_against=date_to_check_against) for loan in txn_loans
        )

        return (
            super().get_remaining_min(date_to_check_against=date_to_check_against)
            + txn_loans_remaining_min_sum
        )

    def get_remaining_max(self, date_to_check_against: DateTime) -> Decimal:
        txn_loans = self.get_transaction_loans()

        txn_loans_remaining_max_sum = sum(
            loan.get_remaining_max(date_to_check_against=date_to_check_against) for loan in txn_loans
        )

        return (
            super().get_remaining_max(date_to_check_against=date_to_check_against)
            + txn_loans_remaining_max_sum
        )

    def get_all_bills(
        self,
        are_generated: bool = False,
        only_unpaid_bills: bool = False,
        only_closed_bills: bool = False,
    ) -> List[BaseBill]:
        txn_loans = self.get_transaction_loans()

        all_bills = []

        for loan in txn_loans:
            all_bills.extend(
                loan.get_all_bills(
                    are_generated=are_generated,
                    only_unpaid_bills=only_unpaid_bills,
                    only_closed_bills=only_closed_bills,
                )
            )

        all_bills.extend(
            super().get_all_bills(
                are_generated=are_generated,
                only_unpaid_bills=only_unpaid_bills,
                only_closed_bills=only_closed_bills,
            )
        )

        return all_bills

    __mapper_args__ = {"polymorphic_identity": "rebel"}
