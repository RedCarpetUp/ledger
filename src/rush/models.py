from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    Dict,
)

from pendulum import Date as PythonDate
from pendulum import DateTime
from pydantic.dataclasses import dataclass as py_dataclass
from sqlalchemy import (
    DECIMAL,
    TIMESTAMP,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.indexable import index_property
from sqlalchemy.orm import (
    Session,
    relationship,
)
from sqlalchemy.schema import Index
from sqlalchemy.util.langhelpers import hybridproperty

from rush.utils import get_current_ist_time

Base = declarative_base()  # type: Any


class pg_json_property(index_property):
    def __init__(self, attr_name, index, cast_type, default=None):
        super(pg_json_property, self).__init__(attr_name, index, default=default)
        self.cast_type = cast_type

    def expr(self, model):
        expr = super(pg_json_property, self).expr(model)
        return expr.astext.cast(self.cast_type)


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

    @classmethod
    def snapshot(
        cls, session: Session, primary_key, new_data, skip_columns=("id", "created_at", "updated_at")
    ):
        assert hasattr(cls, "row_status") == True

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

        new_obj = cls.new(**new_data)
        session.flush()
        return new_obj


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


class Product(AuditMixin):
    __tablename__ = "product"
    product_name = Column(String(), nullable=False, unique=True)


@py_dataclass
class ProductPy(AuditMixinPy):
    product_name: str


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


class UserProduct(AuditMixin):
    __tablename__ = "user_product"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    product_type = Column(String(), ForeignKey(Product.product_name), nullable=False)


class Loan(AuditMixin):
    __tablename__ = "loan"
    user_id = Column(Integer, ForeignKey(User.id))
    user_product_id = Column(Integer, ForeignKey(UserProduct.id), nullable=True)
    amortization_date = Column(TIMESTAMP, nullable=True)
    loan_status = Column(String(), nullable=True)
    interest_type = Column(String(), nullable=True)
    product_type = Column(String(), ForeignKey(Product.product_name), nullable=False)
    lender_id = Column(Integer, ForeignKey(Lenders.id), nullable=False)
    rc_rate_of_interest_monthly = Column(Numeric, nullable=False)
    lender_rate_of_interest_annual = Column(Numeric, nullable=False)
    interest_free_period_in_days = Column(Integer, default=45, nullable=True)
    min_tenure = Column(Integer, nullable=True)
    min_multiplier = Column(Numeric, nullable=True)
    dpd = Column(Integer, nullable=True)
    ever_dpd = Column(Integer, nullable=True)
    downpayment_percent = Column(Numeric, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": "loan",
        "polymorphic_on": product_type,
    }


@py_dataclass
class LoanPy(AuditMixinPy):
    user_id: int
    amortization_date: DateTime
    loan_status: str
    product_id: int


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
    balance = Column(DECIMAL, default=0)


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
    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    is_deleted = Column(Boolean, nullable=True)


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


class LedgerTriggerEvent(AuditMixin):
    __tablename__ = "ledger_trigger_event"
    name = Column(String(50))
    loan_id = Column(Integer, ForeignKey(Loan.id))
    post_date = Column(TIMESTAMP)
    amount = Column(Numeric)
    extra_details = Column(JSON, default={})

    user_product_id = pg_json_property("extra_details", "user_product_id", Integer, default=None)
    payment_type = pg_json_property("extra_details", "payment_type", String, default=None)


class LedgerEntry(Base):
    __tablename__ = "ledger_entry"
    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    debit_account = Column(Integer, ForeignKey(BookAccount.id), nullable=False)
    debit_account_balance = Column(DECIMAL, nullable=False)
    credit_account = Column(Integer, ForeignKey(BookAccount.id), nullable=False)
    credit_account_balance = Column(DECIMAL, nullable=False)
    amount = Column(DECIMAL, nullable=False)
    created_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)


class LoanData(AuditMixin):
    __tablename__ = "loan_data"
    user_id = Column(Integer, ForeignKey(User.id))
    bill_start_date = Column(Date, nullable=False)
    bill_close_date = Column(Date, nullable=False)
    bill_due_date = Column(Date, nullable=False)
    bill_tenure = Column(Integer, nullable=False, default=12)
    loan_id = Column(Integer, ForeignKey(Loan.id))
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
    mcc = Column(String(10), nullable=True)
    trace_no = Column(String(20), nullable=False)
    txn_ref_no = Column(String(50), nullable=False)
    status = Column(String(15), nullable=False)


class CardEmis(AuditMixin):
    __tablename__ = "card_emis"
    loan_id = Column(Integer, ForeignKey(Loan.id))
    bill_id = Column(Integer, ForeignKey(LoanData.id), nullable=True)
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

    def get_payment_received_on_emi(self):
        return (
            self.atm_fee_received
            + self.late_fee_received
            + self.interest_received
            + self.payment_received
        )


class EmiPaymentMapping(AuditMixin):
    __tablename__ = "emi_payment_mapping"
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    emi_number = Column(Integer, nullable=False)
    payment_date = Column(TIMESTAMP, nullable=False)
    payment_request_id = Column(String(), nullable=False)
    interest_received = Column(Numeric, nullable=True, default=Decimal(0))
    late_fee_received = Column(Numeric, nullable=True, default=Decimal(0))
    atm_fee_received = Column(Numeric, nullable=True, default=Decimal(0))
    principal_received = Column(Numeric, nullable=True, default=Decimal(0))
    row_status = Column(String(length=10), nullable=False, default="active")


class LoanSchedule(AuditMixin):
    __tablename__ = "loan_schedule"
    loan_id = Column(Integer, ForeignKey(Loan.id))
    bill_id = Column(Integer, ForeignKey(LoanData.id), nullable=True)  # hate this. - Raghav
    emi_number = Column(Integer, nullable=False)
    due_date = Column(Date, nullable=False)
    principal_due = Column(Numeric, nullable=False)
    interest_due = Column(Numeric, nullable=False)
    # total_due_amount = Column(Numeric, nullable=False)  # This should be a calculated column.
    dpd = Column(Integer, nullable=False, default=-999)
    last_payment_date = Column(TIMESTAMP, nullable=True)
    total_closing_balance = Column(Numeric, nullable=False)
    payment_received = Column(Numeric, nullable=False, default=0)
    payment_status = Column(String(length=6), nullable=False, default="UnPaid")

    @hybrid_property
    def total_due_amount(self):
        return self.principal_due + self.interest_due

    @hybrid_property
    def remaining_amount(self):
        return self.total_due_amount - self.payment_received

    def make_emi_unpaid(self):
        self.payment_received = 0
        self.payment_status = "UnPaid"
        self.last_payment_date = None

    def can_mark_emi_paid(self) -> bool:
        return self.remaining_amount <= Decimal(1)


class PaymentMapping(AuditMixin):
    __tablename__ = "emi_payment_mapping_new"
    payment_request_id = Column(String(), nullable=False, index=True)
    emi_id = Column(Integer, ForeignKey(LoanSchedule.id), nullable=False, index=True)
    amount_settled = Column(Numeric, nullable=False)
    row_status = Column(String(8), nullable=False, default="active")

    __table_args__ = (
        Index(
            "idx_uniq_on_row_status_emi_payment_mapping",
            payment_request_id,
            emi_id,
            unique=True,
            postgresql_where=row_status == "active",
        ),
    )


class PaymentSplit(AuditMixin):
    __tablename__ = "payment_split"
    payment_request_id = Column(String(), nullable=False, index=True)
    component = Column(String(50), nullable=False)
    amount_settled = Column(Numeric, nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)


class LoanMoratorium(AuditMixin):
    __tablename__ = "loan_moratorium"

    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    @classmethod
    def is_in_moratorium(cls, session: Session, loan_id: int, date_to_check_against: PythonDate) -> bool:
        if not date_to_check_against:
            date_to_check_against = get_current_ist_time()
        v = (
            session.query(cls)
            .filter(
                cls.loan_id == loan_id,
                date_to_check_against >= cls.start_date,
                date_to_check_against <= cls.end_date,
            )
            .one_or_none()
        )
        return v is not None


class Fee(AuditMixin):
    __tablename__ = "fee"

    user_id = Column(Integer, ForeignKey(User.id))
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    identifier = Column(String(), nullable=False)
    identifier_id = Column(Integer, nullable=False)
    name = Column(String(30), nullable=False)
    net_amount = Column(Numeric, nullable=False)
    sgst_rate = Column(Numeric, nullable=False)
    cgst_rate = Column(Numeric, nullable=False)
    igst_rate = Column(Numeric, nullable=False)
    gross_amount = Column(Numeric, nullable=False)
    net_amount_paid = Column(Numeric, nullable=True)
    sgst_paid = Column(Numeric, nullable=True)
    cgst_paid = Column(Numeric, nullable=True)
    igst_paid = Column(Numeric, nullable=True)
    gross_amount_paid = Column(Numeric, nullable=True)
    fee_status = Column(String(10), nullable=False, default="UNPAID")

    __mapper_args__ = {
        "polymorphic_identity": "fee",
        "polymorphic_on": identifier,
    }


class BillFee(Fee):

    __mapper_args__ = {
        "polymorphic_identity": "bill",
    }


class LoanFee(Fee):

    __mapper_args__ = {
        "polymorphic_identity": "loan",
    }


class ProductFee(Fee):

    __mapper_args__ = {
        "polymorphic_identity": "product",
    }


class EventDpd(AuditMixin):
    __tablename__ = "event_dpd"

    bill_id = Column(Integer, ForeignKey(LoanData.id), nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    debit = Column(Numeric, nullable=True, default=Decimal(0))
    credit = Column(Numeric, nullable=True, default=Decimal(0))
    balance = Column(Numeric, nullable=True, default=Decimal(0))
    dpd = Column(Integer, nullable=False)
    row_status = Column(String(length=10), nullable=False, default="active")


class UserInstrument(AuditMixin):
    __tablename__ = "user_instrument"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    type = Column(String(), nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    details = Column(JSON, default=lambda: {})
    kyc_status = Column(String(length=20), default="PENDING", nullable=True)
    no_of_txn_per_day = Column(Integer, nullable=True)
    single_txn_spend_limit = Column(Integer, nullable=True)
    daily_spend_limit = Column(Integer, nullable=True)
    international_usage = Column(Boolean, default=False, nullable=False)
    credit_limit = Column(Numeric, nullable=True)
    name = Column(String(), nullable=False)
    activation_date = Column(Date, nullable=True)
    instrument_id = Column(String(), nullable=False)
    status = Column(String(), nullable=False)

    __mapper_args__ = {
        "polymorphic_identity": "user_instrument",
        "polymorphic_on": type,
    }


class UserCard(UserInstrument):

    __mapper_args__ = {
        "polymorphic_identity": "card",
    }

    activation_type = Column(String(length=12), nullable=True)

    def __init__(self, **kwargs):
        assert kwargs.get("kit_number") is not None
        assert kwargs.get("activation_type") is not None
        assert kwargs.get("card_name") is not None

        kwargs["instrument_id"] = kwargs.pop("kit_number")
        kwargs["name"] = kwargs.pop("card_name")

        kwargs["status"] = kwargs.get("status", "INACTIVE")
        kwargs["activation_date"] = kwargs.get("card_activation_date")

        super().__init__(**kwargs)

    @hybridproperty
    def card_activation_date(self):
        return self.activation_date

    @hybridproperty
    def kit_number(self):
        return self.instrument_id

    @hybridproperty
    def card_name(self):
        return self.name


class UserUPI(UserInstrument):

    __mapper_args__ = {
        "polymorphic_identity": "upi",
    }

    def __init__(self, **kwargs):
        assert kwargs.get("upi_id") is not None
        assert kwargs.get("upi_merchant") is not None

        kwargs["name"] = kwargs.pop("upi_merchant")
        kwargs["instrument_id"] = kwargs.pop("upi_id")

        kwargs["status"] = kwargs.get("status", "INACTIVE")

        super().__init__(**kwargs)

    @hybridproperty
    def upi_id(self):
        return self.instrument_id

    @hybridproperty
    def upi_merchant(self):
        return self.name
