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

    __mapper_args__ = {"polymorphic_identity": "rebel"}
