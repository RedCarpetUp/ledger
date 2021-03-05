from typing import (
    List,
    Type,
)

from sqlalchemy.sql.sqltypes import Integer

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.models import LedgerTriggerEvent


class RebelBill(BaseBill):
    pass


class RebelCard(BaseLoan):
    bill_class: Type[B] = RebelBill

    def get_child_loans(self) -> List[BaseLoan]:
        child_loans: List[BaseLoan] = (
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

        for child_loan in child_loans:
            child_loan.prepare(session=self.session)

        return child_loans

    __mapper_args__ = {"polymorphic_identity": "rebel"}
