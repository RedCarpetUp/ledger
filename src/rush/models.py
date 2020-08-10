from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)

from pendulum import Date as PythonDate
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
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    Session,
    relationship,
)
from sqlalchemy.schema import Index

from rush.utils import get_current_ist_time

Base = declarative_base()  # type: Any


class AuditMixin(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)
    updated_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)
    performed_by = Column(Integer, default=1, nullable=True)

    @classmethod
    def new(cls, session: Session, **kwargs) -> Any:
        obj = cls(**kwargs)
        session.add(obj)
        return obj

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        return d

    def as_dict_for_json(self):
        d = {
            c.name: getattr(self, c.name).isoformat()
            if isinstance(getattr(self, c.name), datetime)
            else getattr(self, c.name)
            for c in self.__table__.columns
        }
        return d


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


@py_dataclass
class AuditMixinPy:
    id: int
    performed_by: int


class Lenders(AuditMixin):
    __tablename__ = "rc_lenders"
    lender_name = Column(String(), nullable=False)
    row_status = Column(String(length=10), nullable=False, default="active")


@py_dataclass
class LenderPy(AuditMixinPy):
    lender_name: str
    row_status: str


# class User(AuditMixin):
#     __tablename__ = "users"
#     name = Column(String(50))
#     email = Column(String(100))
#     fullname = Column(String(50))
#     nickname = Column(String(12))


class UserData(AuditMixin):
    __tablename__ = "v3_user_data"

    user_id = Column(Integer, ForeignKey("v3_users.id"), nullable=False)

    first_name = Column(String(length=255), nullable=True)
    last_name = Column(String(length=255), nullable=True)
    email = Column(String(length=255), nullable=True, unique=False)  # TODO should email be unique?
    email_verified = Column(Boolean, nullable=False, server_default="false")
    gender = Column(String(length=20), nullable=True)
    date_of_birth = Column(TIMESTAMP, nullable=True)
    # parent_contact = Column(String(length=50), nullable=True)
    # parent_name = Column(String(length=100), nullable=True)
    pocket_money = Column(String(length=30), nullable=True)
    is_ambassador = Column(Boolean, nullable=True)
    became_ambassador_at = Column(TIMESTAMP, nullable=True)
    referred_by = Column(String(length=50), nullable=True)  # user_id of referral user which are referred
    has_app = Column(Boolean, nullable=True)
    unique_id = Column(String(length=50))
    lender_id = Column(Integer, nullable=True)

    status = Column(String(length=50), nullable=False)
    access_token = Column(String(length=50), nullable=True)  # To authenticate requests coming from app.
    credit_limit = Column(Numeric, server_default="0", nullable=False)
    available_credit_limit = Column(Numeric, server_default="0", nullable=False)

    utm_campaign = Column(String(length=50), nullable=True)
    utm_source = Column(String(length=50), nullable=True)
    utm_medium = Column(String(length=50), nullable=True)
    referral_code = Column(String(length=50), nullable=True)

    # TODO columns from app_numbers
    signup_otp = Column(Integer, nullable=True)
    signup_otp_created_at = Column(TIMESTAMP, nullable=True)
    gcm_id = Column(String(200), nullable=True)
    pusher_channel = Column(String(50), nullable=True)
    view_tags = Column(JSON, server_default="{}", nullable=True)

    # Aggregate columns
    total_credit_used = Column(Numeric, server_default="0", nullable=False)
    rc_cash_balance = Column(Numeric, server_default="0", nullable=False)
    total_credit_payment_pending = Column(Numeric, server_default="0", nullable=False)
    total_overdue_payment = Column(Numeric, server_default="0", nullable=False)
    amount_due_as_of_today = Column(Numeric, server_default="0", nullable=False)
    amount_paid_as_of_today = Column(Numeric, server_default="0", nullable=False)
    amount_paid_by_due_date = Column(Numeric, server_default="0", nullable=False)
    amount_paid_after_due_date = Column(Numeric, server_default="0", nullable=False)
    # TODO ask diff btwn this and total_credit_payment_pending
    unpaid_pending_amount = Column(Numeric, server_default="0", nullable=False)
    row_status = Column(String(length=20), nullable=False, default="active")

    ecdsa_signing_key = Column(String(length=100), nullable=True)
    assigned_to = Column(Integer, ForeignKey("v3_users.id"), nullable=True)

    corporate_email = Column(String(length=255), nullable=True)
    corporate_email_verified = Column(Boolean, nullable=False, server_default="false")

    __table_args__ = (
        Index(
            "unique_index_on_row_status_user_id",
            user_id,
            row_status,
            unique=True,
            postgresql_where=row_status == "active",
        ),
    )


class User(AuditMixin):
    __tablename__ = "v3_users"
    phone_number = Column(String(20), nullable=False, default="0000000000")
    histories = relationship("UserData", foreign_keys=[UserData.user_id])
    roles = relationship("UserRoles")
    identities = relationship("UserIdentities", back_populates="user")
    latest = relationship(
        "UserData",
        lazy="joined",
        uselist=False,
        primaryjoin="and_(User.id==UserData.user_id, UserData.row_status=='active')",
    )

    data_class = UserData


class UserIdentities(AuditMixin):
    __tablename__ = "v3_user_identities"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    identity = Column(String(length=255), nullable=False)
    identity_type = Column(String(length=50), nullable=False)
    row_status = Column(String(length=20), nullable=False)
    comments = Column(Text, nullable=True)
    user = relationship("User", back_populates="identities")
    __table_args__ = (
        Index("index_on_user_id_v3_user_identity", user_id),
        Index(
            "index_on_identity_v3_user_identity",
            identity,
            row_status,
            unique=True,
            postgresql_where=row_status == "active",
        ),
    )


class Role(AuditMixin):
    __tablename__ = "v3_roles"

    name = Column(String(length=50), nullable=False)
    data = Column(JSON, server_default="{}", nullable=True)
    comments = Column(Text)

    __table_args__ = (Index("index_on_name_and_id_v3_roles", name, "id"),)


class UserRoles(AuditMixin):
    __tablename__ = "v3_user_roles"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    role_id = Column(Integer, ForeignKey(Role.id), nullable=False)
    data = Column(JSON, server_default="{}", nullable=True)
    row_status = Column(String(20), default="active", nullable=False)

    __table_args__ = (
        Index("index_on_user_id_v3_user_roles", user_id),
        Index(
            "unique_index_on_user_id_role_id",
            user_id,
            role_id,
            row_status,
            unique=True,
            postgresql_where=row_status == "active",
        ),
    )


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


# class UserCard(AuditMixin):
#     __tablename__ = "user_card"
#     user_id = Column(Integer, ForeignKey(User.id), nullable=False)
#     lender_id = Column(Integer, ForeignKey(Lenders.id), nullable=False)
#     card_type = Column(String, nullable=False)
#     card_activation_date = Column(Date, nullable=True)
#     statement_period_in_days = Column(Integer, default=30, nullable=False)  # 30 days
#     interest_free_period_in_days = Column(Integer, default=45, nullable=False)
#     rc_rate_of_interest_monthly = Column(Numeric, nullable=False)
#     lender_rate_of_interest_annual = Column(Numeric, nullable=False)
#     dpd = Column(Integer, nullable=True)


class Loan(AuditMixin):
    __tablename__ = "v3_loans"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    is_deleted = Column(Boolean, nullable=True)

    # histories = relationship("LoanData", foreign_keys="LoanData.loan_id")
    # latest = relationship(
    #     "LoanData",
    #     lazy="joined",
    #     uselist=False,
    #     primaryjoin="and_(Loan.id==LoanData.loan_id, LoanData.row_status=='active')",
    # )

    # emi_histories = relationship("EmiData")
    # emis_latest = relationship(
    #     "EmiData", primaryjoin="and_(Loan.id==EmiData.loan_id, EmiData.row_status=='active')"
    # )

    __table_args__ = (Index("index_on_v3_loans_user_id_and_id", user_id, "id"),)


class CardNames(AuditMixin):
    __tablename__ = "v3_card_names"
    name = Column(String(20), nullable=False, unique=True)


class CardKitNumbers(AuditMixin):
    __tablename__ = "v3_card_kit_numbers"
    kit_number = Column(String(12), unique=True, nullable=False)
    card_name_id = Column(Integer, ForeignKey(CardNames.id), nullable=False)
    card_type = Column(String(12), nullable=True)
    last_5_digits = Column(String(5), nullable=False)
    status = Column(String(15), nullable=False)
    extra_details = Column(JSON, nullable=False, default={})
    user_cards = relationship("UserCard")


class UserCard(AuditMixin):
    __tablename__ = "v3_user_cards"
    user_id = Column(Integer, ForeignKey(User.id), index=True, nullable=False)
    lender_id = Column(Integer, ForeignKey(Lenders.id), nullable=False)
    kit_number = Column(
        String(12), ForeignKey(CardKitNumbers.kit_number), nullable=False, default="00000"
    )
    credit_limit = Column(Numeric, nullable=False, default=1000)  # limit in rupees
    cash_withdrawal_limit = Column(Numeric, nullable=False, default=1000)
    drawdown_id = Column(Integer, ForeignKey(Loan.id), nullable=True)  # to detect payments
    details = Column(JSON, nullable=True, server_default="{}")
    activation_type = Column(String(12), nullable=False, default="P")
    row_status = Column(String(20), nullable=False, default="active")
    kyc_status = Column(String(20), server_default="PENDING", nullable=True)

    single_txn_spend_limit = Column(Integer, nullable=True)
    no_of_txn_per_day = Column(Integer, nullable=True)
    international_usage = Column(Boolean, nullable=False, default=False)
    daily_spend_limit = Column(Integer, nullable=True)

    card_type = Column(String, nullable=True)
    card_activation_date = Column(Date, nullable=True)
    statement_period_in_days = Column(Integer, default=30, nullable=True)  # 30 days
    interest_free_period_in_days = Column(Integer, default=45, nullable=True)
    rc_rate_of_interest_monthly = Column(Numeric, nullable=True)
    lender_rate_of_interest_annual = Column(Numeric, nullable=True)
    dpd = Column(Integer, nullable=True)

    __table_args__ = (
        Index(
            "idx_uniq_kit_number_row_status",
            kit_number,
            row_status,
            unique=True,
            postgresql_where=row_status == "active",
        ),
        Index(
            "idx_user_cards_uniq_user_id_drawdown_id_row_status",
            user_id,
            drawdown_id,
            unique=True,
            postgresql_where=row_status == "active",
        ),
    )


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
    lender_id = Column(Integer, ForeignKey(Lenders.id), nullable=False)
    bill_start_date = Column(Date, nullable=False)
    bill_close_date = Column(Date, nullable=False)
    bill_due_date = Column(Date, nullable=False)
    bill_tenure = Column(Integer, nullable=False, default=12)
    card_id = Column(Integer, ForeignKey(UserCard.id))
    is_generated = Column(Boolean, nullable=False, server_default="false")
    principal = Column(Numeric, nullable=True)
    principal_instalment = Column(Numeric, nullable=True)
    interest_to_charge = Column(Numeric, nullable=True)


@py_dataclass
class LoanDataPy(AuditMixinPy):
    user_id: int
    bill_start_date: DateTime
    bill_generation_date: DateTime


class CardTransaction(AuditMixin):
    __tablename__ = "card_transaction"
    loan_id = Column(Integer, ForeignKey(LoanData.id))
    txn_time = Column(TIMESTAMP, nullable=False)
    amount = Column(Numeric, nullable=False)
    source = Column(String(30), nullable=False)
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
    atm_fee = Column(Numeric, nullable=False, default=Decimal(0))
    row_status = Column(String(length=10), nullable=False, default="active")
    dpd = Column(Integer, nullable=True, default=0)
    last_payment_date = Column(TIMESTAMP, nullable=True)
    total_closing_balance = Column(Numeric, nullable=False, default=Decimal(0))
    total_closing_balance_post_due_date = Column(Numeric, nullable=False, default=Decimal(0))
    late_fee_received = Column(Numeric, nullable=False, default=Decimal(0))
    atm_fee_received = Column(Numeric, nullable=False, default=Decimal(0))
    interest_received = Column(Numeric, nullable=False, default=Decimal(0))
    payment_received = Column(Numeric, nullable=False, default=Decimal(0))
    payment_status = Column(String(length=10), nullable=False, default="UnPaid")
    extra_details = Column(JSON, default=lambda: {})


class EmiPaymentMapping(AuditMixin):
    __tablename__ = "emi_payment_mapping"
    card_id = Column(Integer, ForeignKey(UserCard.id), nullable=False)
    emi_number = Column(Integer, nullable=False)
    payment_date = Column(TIMESTAMP, nullable=False)
    payment_request_id = Column(String(), nullable=False)
    interest_received = Column(Numeric, nullable=True, default=Decimal(0))
    late_fee_received = Column(Numeric, nullable=True, default=Decimal(0))
    atm_fee_received = Column(Numeric, nullable=True, default=Decimal(0))
    principal_received = Column(Numeric, nullable=True, default=Decimal(0))
    row_status = Column(String(length=10), nullable=False, default="active")


class LoanMoratorium(AuditMixin):
    __tablename__ = "loan_moratorium"

    card_id = Column(Integer, ForeignKey(UserCard.id), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    @classmethod
    def is_in_moratorium(cls, session: Session, card_id: int, date_to_check_against: PythonDate) -> bool:
        v = (
            session.query(cls)
            .filter(
                cls.card_id == card_id,
                date_to_check_against >= cls.start_date,
                date_to_check_against <= cls.end_date,
            )
            .one_or_none()
        )
        return v is not None
