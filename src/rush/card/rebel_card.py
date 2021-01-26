from decimal import Decimal
from operator import add
from typing import (
    List,
    Optional,
    Type,
)

from sqlalchemy.orm import session
from sqlalchemy.sql.sqltypes import (
    Boolean,
    DateTime,
    Integer,
)

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.card.transaction_loan import TransactionLoan
from rush.models import (
    Base,
    LedgerTriggerEvent,
)


class RebelBill(BaseBill):
    pass


class RebelCard(BaseLoan):
    bill_class: Type[B] = RebelBill

    def get_child_loans(self) -> List[BaseLoan]:
        child_loans = (
            self.session.query(BaseLoan)
            .join(
                LedgerTriggerEvent,
                LedgerTriggerEvent.extra_details["child_loan_id"].astext.cast(Integer) == BaseLoan.id,
            )
            .filter(
                LedgerTriggerEvent.name.in_(
                    [
                        "transaction_to_loan",
                    ]
                ),
                LedgerTriggerEvent.loan_id == self.id,
            )
            .all()
        )

        return child_loans

    def get_remaining_min(
        self,
        date_to_check_against: Optional[DateTime] = None,
        include_child_loans: Optional[Boolean] = True,
    ) -> Decimal:
        if include_child_loans:
            txn_loans = self.get_child_loans()
            txn_loans_remaining_min_sum = sum(
                loan.get_remaining_min(date_to_check_against=date_to_check_against) for loan in txn_loans
            )
        else:
            txn_loans_remaining_min_sum = 0

        return (
            super().get_remaining_min(date_to_check_against=date_to_check_against)
            + txn_loans_remaining_min_sum
        )

    def get_remaining_max(
        self,
        date_to_check_against: Optional[DateTime] = None,
        include_child_loans: Optional[Boolean] = True,
    ) -> Decimal:
        if include_child_loans:
            txn_loans = self.get_child_loans()
            txn_loans_remaining_max_sum = sum(
                loan.get_remaining_max(date_to_check_against=date_to_check_against) for loan in txn_loans
            )
        else:
            txn_loans_remaining_max_sum = 0

        return (
            super().get_remaining_max(date_to_check_against=date_to_check_against)
            + txn_loans_remaining_max_sum
        )

    __mapper_args__ = {"polymorphic_identity": "rebel"}
