from decimal import Decimal
from typing import (
    Any,
    Dict,
)

from pendulum import Date as PythonDate
from sqlalchemy import (
    DECIMAL,
    TIMESTAMP,
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import (
    JSON,
    JSONB,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    Session,
    relationship,
)
from sqlalchemy.schema import Index
from sqlalchemy.sql.elements import and_
from sqlalchemy.util.langhelpers import hybridproperty

from rush.utils import get_current_ist_time

Base = declarative_base()  # type: Any


class AuditMixin(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)
    updated_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)
    performed_by = Column(Integer, default=1, nullable=True)

    @classmethod
    def ledger_new(cls, session: Session, **kwargs) -> Any:
        obj = cls(**kwargs)
        session.add(obj)
        return obj

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
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


class Lenders(AuditMixin):
    __tablename__ = "rc_lenders"
    lender_name = Column(String(), nullable=False)
    row_status = Column(String(length=10), nullable=False, default="active")


class Product(AuditMixin):
    __tablename__ = "product"
    product_name = Column(String(), nullable=False, unique=True)


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
    credit_limit: Decimal = Column(Numeric, server_default="0", nullable=False)
    available_credit_limit: Decimal = Column(Numeric, server_default="0", nullable=False)

    utm_campaign = Column(String(length=50), nullable=True)
    utm_source = Column(String(length=50), nullable=True)
    utm_medium = Column(String(length=50), nullable=True)
    referral_code = Column(String(length=50), nullable=True)

    # TODO columns from app_numbers
    signup_otp = Column(Integer, nullable=True)
    signup_otp_created_at = Column(TIMESTAMP, nullable=True)
    gcm_id = Column(String(200), nullable=True)
    pusher_channel = Column(String(50), nullable=True)
    view_tags = Column(JSONB, server_default="{}", nullable=True)

    # Aggregate columns
    total_credit_used: Decimal = Column(Numeric, server_default="0", nullable=False)
    rc_cash_balance: Decimal = Column(Numeric, server_default="0", nullable=False)
    total_credit_payment_pending: Decimal = Column(Numeric, server_default="0", nullable=False)
    total_overdue_payment: Decimal = Column(Numeric, server_default="0", nullable=False)
    amount_due_as_of_today: Decimal = Column(Numeric, server_default="0", nullable=False)
    amount_paid_as_of_today: Decimal = Column(Numeric, server_default="0", nullable=False)
    amount_paid_by_due_date: Decimal = Column(Numeric, server_default="0", nullable=False)
    amount_paid_after_due_date: Decimal = Column(Numeric, server_default="0", nullable=False)
    # TODO ask diff btwn this and total_credit_payment_pending
    unpaid_pending_amount: Decimal = Column(Numeric, server_default="0", nullable=False)
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
    __tablename__ = "v3_loans"
    user_id = Column(Integer, ForeignKey(User.id))
    user_product_id = Column(Integer, ForeignKey(UserProduct.id), nullable=True)
    amortization_date = Column(TIMESTAMP, nullable=True)
    loan_status = Column(String(), nullable=True)
    interest_type = Column(String(), nullable=True)
    product_type = Column(String(), ForeignKey(Product.product_name), nullable=True)
    lender_id = Column(Integer, ForeignKey(Lenders.id), nullable=True)
    rc_rate_of_interest_monthly: Decimal = Column(Numeric, nullable=True)
    lender_rate_of_interest_annual: Decimal = Column(Numeric, nullable=True)
    interest_free_period_in_days = Column(Integer, default=45, nullable=True)
    min_tenure = Column(Integer, nullable=True)
    min_multiplier: Decimal = Column(Numeric, nullable=True)
    dpd = Column(Integer, nullable=True)
    ever_dpd = Column(Integer, nullable=True)
    downpayment_percent: Decimal = Column(Numeric, nullable=True, default=Decimal(0))
    can_close_early = Column(Boolean, nullable=True, default=True)
    tenure_in_months = Column(Integer, nullable=True)
    sub_product_type = Column(String(15), nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": "v3_loans",
        "polymorphic_on": product_type,
    }


class BookAccount(AuditMixin):
    __tablename__ = "book_account"
    identifier = Column(Integer)
    identifier_type = Column(String(50))  # bill, emi, user, lender etc.
    book_name = Column(String(50))
    account_type = Column(String(50))
    balance = Column(DECIMAL, default=0)


class LedgerTriggerEvent(AuditMixin):
    __tablename__ = "ledger_trigger_event"
    name = Column(String(50))
    loan_id = Column(Integer, ForeignKey(Loan.id))
    post_date = Column(TIMESTAMP)
    amount: Decimal = Column(Numeric)
    extra_details = Column(JSON, default={})

    def __init__(self, **kwargs):
        lender_event_names = ("lender_disbursal", "m2p_transfer", "incur_lender_interest")

        # loan_id should not be null for all non-lender events
        if kwargs["name"] not in lender_event_names:
            assert kwargs["loan_id"] is not None

        super().__init__(**kwargs)


class LedgerEntry(Base):
    __tablename__ = "ledger_entry"
    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    debit_account = Column(Integer, ForeignKey(BookAccount.id), nullable=False)
    debit_account_balance = Column(DECIMAL)
    credit_account = Column(Integer, ForeignKey(BookAccount.id), nullable=False)
    credit_account_balance = Column(DECIMAL)
    amount = Column(DECIMAL, nullable=False)
    created_at = Column(TIMESTAMP, default=get_current_ist_time(), nullable=False)


class LoanData(AuditMixin):
    __tablename__ = "loan_data"
    user_id = Column(Integer, ForeignKey(User.id))
    bill_start_date = Column(Date, nullable=False)
    bill_close_date = Column(Date, nullable=False)
    bill_due_date = Column(Date, nullable=False)
    bill_tenure = Column(Integer, nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id))
    is_generated = Column(Boolean, nullable=False, server_default="false")
    principal: Decimal = Column(Numeric, nullable=True)
    gross_principal: Decimal = Column(Numeric, nullable=True)
    principal_instalment: Decimal = Column(Numeric, nullable=True)
    interest_to_charge: Decimal = Column(Numeric, nullable=True)


class CardTransaction(AuditMixin):
    __tablename__ = "card_transaction"
    loan_id = Column(Integer, ForeignKey(LoanData.id), nullable=False)
    txn_time = Column(TIMESTAMP, nullable=False)
    amount: Decimal = Column(Numeric, nullable=False)
    source = Column(String(30), nullable=False)
    description = Column(String(100), nullable=True)
    mcc = Column(String(10), nullable=True)
    trace_no = Column(String(20), nullable=True)
    txn_ref_no = Column(String(50), nullable=True)
    status = Column(String(15), nullable=True)

    __table_args__ = (
        Index(
            "unique_index_on_txn_ref_no_card_transaction",
            txn_ref_no,
            unique=True,
        ),
    )


class LoanSchedule(AuditMixin):
    __tablename__ = "loan_schedule"
    loan_id = Column(Integer, ForeignKey(Loan.id))
    bill_id = Column(Integer, ForeignKey(LoanData.id), nullable=True)  # hate this. - Raghav
    emi_number = Column(Integer, nullable=False)
    due_date = Column(Date, nullable=False)
    principal_due: Decimal = Column(Numeric, nullable=False)
    interest_due: Decimal = Column(Numeric, nullable=False)
    # total_due_amount: Decimal  = Column(Numeric, nullable=False)  # This should be a calculated column.
    dpd = Column(Integer, nullable=False, default=-999)
    last_payment_date = Column(TIMESTAMP, nullable=True)
    total_closing_balance: Decimal = Column(Numeric, nullable=False)
    payment_received: Decimal = Column(Numeric, nullable=False, default=0)
    payment_status = Column(String(length=6), nullable=False, default="UnPaid")

    @hybrid_property
    def total_due_amount(self):
        return self.principal_due + self.interest_due

    @hybrid_property
    def remaining_amount(self):
        return self.total_due_amount - self.payment_received

    def interest_to_accrue(self, session: Session):
        loan_moratorium = (
            session.query(LoanMoratorium)
            .filter(
                LoanMoratorium.loan_id == self.loan_id,
            )
            .order_by(LoanMoratorium.start_date.desc())
            .first()
        )
        if not loan_moratorium or self.due_date > loan_moratorium.due_date_after_moratorium:
            return self.interest_due

        moratorium_interest_for_this_emi = (
            session.query(MoratoriumInterest.interest)
            .filter(
                MoratoriumInterest.loan_schedule_id == self.id,
            )
            .scalar()
        )
        if moratorium_interest_for_this_emi:  # if emi is present in moratorium table then return that.
            return moratorium_interest_for_this_emi

        # Now emi can either be from the bill right after moratorium or bills before/during moratorium period.
        # For former we return the schedule's interest due. For later we reduce the total moratorium interest of the
        # bill from the schedule's interest due.
        total_bill_moratorium_interest = MoratoriumInterest.get_bill_total_moratorium_interest(
            session=session, loan_id=self.loan_id, bill_id=self.bill_id
        )
        if not total_bill_moratorium_interest:  # if nothing comes then emi is from non moratorium bill.
            return self.interest_due

        emi_interest_without_moratorium = self.interest_due - total_bill_moratorium_interest
        return emi_interest_without_moratorium

    def make_emi_unpaid(self):
        self.payment_received = 0
        self.payment_status = "UnPaid"
        self.last_payment_date = None

    def can_mark_emi_paid(self) -> bool:
        return self.remaining_amount <= Decimal(1)


class LoanMoratorium(AuditMixin):
    __tablename__ = "loan_moratorium"

    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    due_date_after_moratorium = Column(Date, nullable=False)

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


class MoratoriumInterest(AuditMixin):
    __tablename__ = "moratorium_interest"

    moratorium_id = Column(Integer, ForeignKey(LoanMoratorium.id), nullable=False)
    interest = Column(Numeric, nullable=False)
    loan_schedule_id = Column(Integer, ForeignKey(LoanSchedule.id))

    @classmethod
    def get_bill_total_moratorium_interest(cls, session: Session, loan_id: int, bill_id: int) -> Decimal:
        total_bill_moratorium_interest = (
            session.query(func.sum(cls.interest))
            .join(
                LoanMoratorium,
                and_(
                    cls.moratorium_id == LoanMoratorium.id,
                    LoanMoratorium.loan_id == loan_id,
                ),
            )
            .filter(
                LoanSchedule.bill_id == bill_id,
                LoanSchedule.id == cls.loan_schedule_id,
            )
            .scalar()
        )
        return total_bill_moratorium_interest


class PaymentMapping(AuditMixin):
    __tablename__ = "emi_payment_mapping_new"
    payment_request_id = Column(String(), nullable=False, index=True)
    emi_id = Column(Integer, ForeignKey(LoanSchedule.id), nullable=False, index=True)
    amount_settled: Decimal = Column(Numeric, nullable=False)
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
    amount_settled: Decimal = Column(Numeric, nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=True)


class Fee(AuditMixin):
    __tablename__ = "fee"

    user_id = Column(Integer, ForeignKey(User.id))
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    identifier = Column(String(), nullable=False)
    identifier_id = Column(Integer, nullable=False)
    name = Column(String(30), nullable=False)
    net_amount: Decimal = Column(Numeric, nullable=False)
    sgst_rate: Decimal = Column(Numeric, nullable=False)
    cgst_rate: Decimal = Column(Numeric, nullable=False)
    igst_rate: Decimal = Column(Numeric, nullable=False)
    gross_amount: Decimal = Column(Numeric, nullable=False)
    net_amount_paid: Decimal = Column(Numeric, nullable=True, default=0)
    sgst_paid: Decimal = Column(Numeric, nullable=True, default=0)
    cgst_paid: Decimal = Column(Numeric, nullable=True, default=0)
    igst_paid: Decimal = Column(Numeric, nullable=True, default=0)
    gross_amount_paid: Decimal = Column(Numeric, nullable=True, default=0)
    fee_status = Column(String(10), nullable=False, default="UNPAID")

    @hybrid_property
    def remaining_fee_amount(self) -> Decimal:
        return self.gross_amount - self.gross_amount_paid


class EventDpd(AuditMixin):
    __tablename__ = "event_dpd"

    bill_id = Column(Integer, ForeignKey(LoanData.id), nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    debit: Decimal = Column(Numeric, nullable=True, default=Decimal(0))
    credit: Decimal = Column(Numeric, nullable=True, default=Decimal(0))
    balance: Decimal = Column(Numeric, nullable=True, default=Decimal(0))
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
    credit_limit: Decimal = Column(Numeric, nullable=True)
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


class JournalEntry(AuditMixin):
    __tablename__ = "journal_entries"

    voucher_type = Column(String(length=50), default="", nullable=False)
    date_ledger = Column(TIMESTAMP, nullable=False)
    ledger = Column(String(length=50), nullable=False)
    alias = Column(String(length=50), nullable=True)
    group_name = Column(String(length=50), nullable=False)
    debit: Decimal = Column(Numeric, nullable=True)
    credit: Decimal = Column(Numeric, nullable=True)
    narration = Column(String(length=50), nullable=True)
    instrument_date = Column(TIMESTAMP, nullable=False)
    sort_order = Column(Integer, nullable=False)
    ptype = Column(String(length=50), nullable=True)
    row_status = Column(String(length=10), default="active", nullable=False)
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id), nullable=False)
    loan_id = Column(Integer, ForeignKey(Loan.id), nullable=True)
    user_id = Column(Integer, ForeignKey(User.id), nullable=False)


class PaymentRequestsData(AuditMixin):
    __tablename__ = "v3_payment_requests_data"

    type = Column(String(length=20), nullable=False)
    payment_request_amount: Decimal = Column(Numeric, nullable=False)
    payment_request_status = Column(String(length=20), nullable=False)
    source_account_id = Column(Integer, nullable=False)
    destination_account_id = Column(Integer, nullable=False)
    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    payment_request_id = Column(String(length=50), nullable=False)
    row_status = Column(String(length=20), default="active", nullable=False)
    payment_reference_id = Column(String(length=120), nullable=True)
    intermediary_payment_date = Column(TIMESTAMP, nullable=True)
    payment_received_in_bank_date = Column(TIMESTAMP, nullable=True)
    payment_request_mode = Column(String(length=20), nullable=True)
    payment_execution_charges: Decimal = Column(Numeric, nullable=True)
    payment_gateway_id = Column(Integer, nullable=True)
    gateway_response = Column(JSONB, default=lambda: {})
    collection_by = Column(String(length=20), server_default="customer")
    collection_request_id = Column(String(length=50), nullable=True)
    payment_request_comments = Column(Text, nullable=True)
    prepayment_amount: Decimal = Column(Numeric, nullable=True)
    net_payment_amount: Decimal = Column(Numeric, nullable=True)
    fee_amount: Decimal = Column(Numeric, nullable=True)
    expire_date = Column(TIMESTAMP, nullable=True)
    coupon_code = Column(String(length=25), nullable=True)
    coupon_data = Column(JSONB, default=lambda: {})
    gross_request_amount: Decimal = Column(Numeric, nullable=True)
    extra_details = Column(JSONB, default=lambda: {})


class CollectionOrders(AuditMixin):
    __tablename__ = "v3_collection_order_mapping"

    collection_request_id = Column(
        String(length=32),
        nullable=False,
    )
    batch_id = Column(Integer, ForeignKey(Loan.id), nullable=False)
    amount_to_pay = Column(Numeric, nullable=False)
    amount_paid = Column(Numeric, nullable=False, default=0)
    row_status = Column(String(length=20), default="active", nullable=False)
    extra_details = Column(JSONB, server_default="{}", nullable=True)


class UserDocuments(AuditMixin):
    __tablename__ = "v3_user_documents"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False)
    user_product_id = Column(Integer, ForeignKey(UserProduct.id), nullable=True)
    document_type = Column(String(length=50), nullable=False)
    user_product_id = Column(Integer, ForeignKey(UserProduct.id), nullable=True)
    document_identification = Column(Text, nullable=True)
    sequence = Column(Integer, server_default="1", nullable=False)
    image_url = Column(String(length=255), nullable=False)
    text_details_json = Column(JSONB, server_default="{}", nullable=False)
    validity_date = Column(TIMESTAMP, nullable=True)
    verification_date = Column(TIMESTAMP, nullable=True)
    verification_status = Column(String(length=255), nullable=True)
    reject_reason = Column(Text, nullable=True)
    comments = Column(Text, nullable=True)
    lender_id = Column(Integer, ForeignKey(User.id))
    issue_date = Column(TIMESTAMP, nullable=True)
    row_status = Column(String(length=20), server_default="active", nullable=False)
    document_identification_hash = Column(Text, nullable=True)
    original_created_at = Column(TIMESTAMP, nullable=True)
    uid_token = Column(Text, nullable=True)
    image_data = Column(JSONB, nullable=True)
    image_match_data = Column(JSONB, nullable=True)
