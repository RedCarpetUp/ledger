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

    __mapper_args__ = {"polymorphic_identity": "rebel"}
