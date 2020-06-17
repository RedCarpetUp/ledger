from decimal import Decimal as DecimalType
from typing import (
    Any,
    Dict,
    Tuple,
)

from pendulum import DateTime
from pydantic.dataclasses import dataclass as py_dataclass
from sqlalchemy import (
    DECIMAL,
    JSON,
    TIMESTAMP,
    Column,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from rush.utils import get_current_ist_time

Base = declarative_base()  # type: Any


class AuditMixin(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)
    updated_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)
    performed_by = Column(Integer, default=1, nullable=True)

    @classmethod
    def snapshot(
        cls,
        primary_key: str,
        new_data: Dict[str, Any],
        session: Session,
        skip_columns: Tuple[str, ...] = ("id", "created_at", "updated_at"),
    ) -> Any:
        old_row = (
            session.query(cls)
            .filter(
                getattr(cls, primary_key) == new_data[primary_key],
                getattr(cls, "row_status") == "active",
            )
            .with_for_update(skip_locked=True)
            .one_or_none()
        )
        if old_row:
            old_row.row_status = "inactive"
            session().flush()
        cls_keys = cls.__table__.columns.keys()
        keys_to_skip = [key for key in new_data.keys() if key not in cls_keys]
        new_skip_columns = keys_to_skip + list(skip_columns)
        for column in new_skip_columns:
            new_data.pop(column, None)
        new_obj = cls.new(**new_data)
        session.flush()
        return new_obj

    @classmethod
    def new(cls, session: Session, **kwargs) -> Any:
        obj = cls(**kwargs)
        session.add(obj)
        session.flush()  # TODO remove this. this is only temporary.
        return obj


def get_or_create(session: Session, model: Any, defaults: Dict[Any, Any] = None, **kwargs: str) -> Any:
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        params = dict((k, v) for k, v in kwargs.items())
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        session.flush()
        return instance


class User(AuditMixin):
    __tablename__ = "users"
    name = Column(String(50))
    email = Column(String(100))
    fullname = Column(String(50))
    nickname = Column(String(12))


@py_dataclass
class AuditMixinPy:
    id: int
    performed_by: int


@py_dataclass
class UserPy(AuditMixinPy):
    name: str
    email: str
    fullname: str
    nickname: str


class LedgerTriggerEvent(AuditMixin):

    __tablename__ = "ledger_trigger_event"
    name = Column(String(50))
    post_date = Column(TIMESTAMP)
    amount = Column(Numeric)
    extra_details = Column(JSON, default="{}")


@py_dataclass
class LedgerTriggerEventPy(AuditMixinPy):
    name: str
    extra_details: Dict[str, Any]


class BookAccount(AuditMixin):
    __tablename__ = "book_account"
    identifier = Column(Integer)
    identifier_type = Column(String(50))  # bill, emi, user etc.
    book_name = Column(String(50))
    account_type = Column(String(50))
    book_date = Column(Date())


@py_dataclass
class BookAccountPy(AuditMixinPy):
    identifier: int
    book_type: str
    account_type: str


class LedgerEntry(AuditMixin):
    __tablename__ = "ledger_entry"
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id))
    debit_account = Column(Integer, ForeignKey(BookAccount.id))
    credit_account = Column(Integer, ForeignKey(BookAccount.id))
    amount = Column(DECIMAL)


@py_dataclass
class LedgerEntryPy(AuditMixinPy):
    event_id: int
    debit_account: int
    credit_account: int
    amount: DecimalType
    business_date: DateTime


class UserCard(AuditMixin):
    __tablename__ = "user_card"
    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    card_activation_date = Column(Date, nullable=True)
    statement_period_in_days = Column(Integer, default=30, nullable=False)  # 30 days
    interest_free_period_in_days = Column(Integer, default=45, nullable=False)


class LoanData(AuditMixin):
    __tablename__ = "loan_data"
    user_id = Column(Integer, ForeignKey(User.id))
    agreement_date = Column(TIMESTAMP, nullable=False)
    card_id = Column(Integer, ForeignKey(UserCard.id))
    rc_rate_of_interest_annual = Column(Numeric, nullable=False)
    lender_rate_of_interest_annual = Column(Numeric, nullable=False)


@py_dataclass
class LoanDataPy(AuditMixinPy):
    user_id: int
    agreement_date: DateTime
    bill_generation_date: DateTime


class CardTransaction(AuditMixin):
    __tablename__ = "card_transaction"
    loan_id = Column(Integer, ForeignKey(LoanData.id))
    txn_time = Column(TIMESTAMP, nullable=False)
    amount = Column(Numeric, nullable=False)
    description = Column(String(100), nullable=False)


class CardEmis(AuditMixin):
    __tablename__ = "card_emis"
    card_id = Column(Integer, ForeignKey(UserCard.id))
    due_date = Column(TIMESTAMP, nullable=False)
    due_amount = Column(Numeric, nullable=False)
    interest_current_month = Column(Numeric, nullable=False)
    interest_next_month = Column(Numeric, nullable=False)
    emi_number = Column(Integer, nullable=False)
    late_fee = Column(Numeric, nullable=False)
    row_status = Column(String(length=10), nullable=False, default='active')
