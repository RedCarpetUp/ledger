from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)

from pendulum import DateTime
from pydantic.dataclasses import dataclass as py_dataclass
from sqlalchemy import (
    DECIMAL,
    JSON,
    TIMESTAMP,
    Boolean,
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
            session.flush()
        cls_keys = cls.__table__.columns.keys()
        keys_to_skip = [key for key in new_data.keys() if key not in cls_keys]
        new_skip_columns = keys_to_skip + list(skip_columns)
        for column in new_skip_columns:
            new_data.pop(column, None)
        new_obj = cls.new(session, **new_data)
        session.flush()
        return new_obj

    @classmethod
    def new(cls, session: Session, **kwargs) -> Any:
        obj = cls(**kwargs)
        session.add(obj)
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


@py_dataclass
class LedgerTriggerEventPy(AuditMixinPy):
    name: str
    extra_details: Dict[str, Any]


class BookAccount(AuditMixin):
    __tablename__ = "book_account"
    identifier = Column(Integer)
    identifier_type = Column(String(50))  # bill, emi, user, lender etc.
    book_name = Column(String(50))
    account_type = Column(String(50))
    book_date = Column(Date())


@py_dataclass
class BookAccountPy(AuditMixinPy):
    identifier: int
    book_type: str
    account_type: str


@py_dataclass
class LedgerEntryPy(AuditMixinPy):
    event_id: int
    debit_account: int
    credit_account: int
    amount: Decimal
    business_date: DateTime


class UserCard(AuditMixin):
    __tablename__ = "user_card"
    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    card_activation_date = Column(Date, nullable=True)
    statement_period_in_days = Column(Integer, default=30, nullable=False)  # 30 days
    interest_free_period_in_days = Column(Integer, default=45, nullable=False)


class LedgerTriggerEvent(AuditMixin):

    __tablename__ = "ledger_trigger_event"
    name = Column(String(50))
    card_id = Column(Integer, ForeignKey(UserCard.id))
    post_date = Column(TIMESTAMP)
    amount = Column(Numeric)
    extra_details = Column(JSON, default="{}")


class LedgerEntry(AuditMixin):
    __tablename__ = "ledger_entry"
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id))
    debit_account = Column(Integer, ForeignKey(BookAccount.id))
    credit_account = Column(Integer, ForeignKey(BookAccount.id))
    amount = Column(DECIMAL)


class LoanData(AuditMixin):
    __tablename__ = "loan_data"
    user_id = Column(Integer, ForeignKey(User.id))
    lender_id = Column(Integer, nullable=False)
    agreement_date = Column(TIMESTAMP, nullable=False)
    card_id = Column(Integer, ForeignKey(UserCard.id))
    is_generated = Column(Boolean, nullable=False, server_default="false")
    rc_rate_of_interest_annual = Column(Numeric, nullable=False)  # Make this monthly only
    lender_rate_of_interest_annual = Column(Numeric, nullable=False)
    principal = Column(Numeric, nullable=True)
    principal_instalment = Column(Numeric, nullable=True)

    def get_minimum_amount_to_pay(self, session: Session, to_date: Optional[DateTime] = None) -> Decimal:
        from rush.ledger_utils import get_account_balance_from_str

        _, min_due = get_account_balance_from_str(
            session, book_string=f"{self.id}/bill/min/a", to_date=to_date
        )
        return min_due

    @staticmethod
    def get_latest_bill(session: Session, user_id: int) -> Any:
        latest_bill = (
            session.query(LoanData)
            .filter(LoanData.user_id == user_id, LoanData.is_generated.is_(True))
            .order_by(LoanData.agreement_date.desc())
            .first()
        )
        return latest_bill


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
    due_amount = Column(Numeric, nullable=False, default=Decimal(0))
    total_due_amount = Column(Numeric, nullable=False, default=Decimal(0))
    interest_current_month = Column(Numeric, nullable=False, default=Decimal(0))
    interest_next_month = Column(Numeric, nullable=False, default=Decimal(0))
    interest = Column(Numeric, nullable=False, default=Decimal(0))
    emi_number = Column(Integer, nullable=False)
    late_fee = Column(Numeric, nullable=False, default=Decimal(0))
    row_status = Column(String(length=10), nullable=False, default="active")
    dpd = Column(Integer, nullable=True, default=0)
    last_payment_date = Column(TIMESTAMP, nullable=True)
    total_closing_balance = Column(Numeric, nullable=False, default=Decimal(0))
    total_closing_balance_post_due_date = Column(Numeric, nullable=False, default=Decimal(0))
    late_fee_received = Column(Numeric, nullable=False, default=Decimal(0))
    interest_received = Column(Numeric, nullable=False, default=Decimal(0))
    payment_received = Column(Numeric, nullable=False, default=Decimal(0))
    payment_status = Column(String(length=10), nullable=False, default="UnPaid")

    def as_dict(self):
        emi_dict = {
            c.name: getattr(self, c.name).isoformat()
            if isinstance(getattr(self, c.name), datetime)
            else getattr(self, c.name)
            for c in self.__table__.columns
        }
        return emi_dict


class EmiPaymentMapping(AuditMixin):
    __tablename__ = "emi_payment_mapping"
    card_id = Column(Integer, ForeignKey(UserCard.id), nullable=False)
    emi_number = Column(Integer, nullable=False)
    payment_date = Column(TIMESTAMP, nullable=False)
    payment_request_id = Column(String(), nullable=False)
    interest_received = Column(Numeric, nullable=True, default=Decimal(0))
    late_fee_received = Column(Numeric, nullable=True, default=Decimal(0))
    principal_received = Column(Numeric, nullable=True, default=Decimal(0))
