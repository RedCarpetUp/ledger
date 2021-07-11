from datetime import date
from decimal import Decimal
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

from dateutil.relativedelta import relativedelta
from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Session

from rush.ledger_utils import get_account_balance_from_str
from rush.loan_schedule.calculations import (
    get_down_payment,
    get_interest_for_integer_emi,
    get_monthly_instalment,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    Loan,
    LoanData,
    LoanMoratorium,
    LoanSchedule,
)
from rush.utils import get_current_ist_time


class BaseBill:
    session: Session = None
    table: LoanData = None
    id = None
    bill_start_date = None
    user_loan: "BaseLoan"
    round_emi_to = "one"

    def __init__(self, session: Session, user_loan: "BaseLoan", loan_data: LoanData):
        self.session = session
        self.table = loan_data
        self.user_loan = user_loan
        self.__dict__.update(loan_data.__dict__)

    def get_interest_to_charge(
        self,
        principal: Optional[Decimal] = None,
        instalment: Optional[Decimal] = None,  # Updated instalment for bill extension
    ) -> Decimal:
        if not principal:
            down_payment = self.get_down_payment(include_first_emi=False)
            principal = self.table.principal - down_payment

        if not instalment:
            instalment = self.get_instalment_amount(to_round=False)

        interest = get_interest_for_integer_emi(
            principal=principal,
            instalment=instalment,
            interest_rate_monthly=self.user_loan.rc_rate_of_interest_monthly,
            round_to=self.round_emi_to,
        )
        return interest

    def get_scheduled_min_amount(self, to_round: Optional[bool] = True) -> Decimal:
        if not self.table.is_generated:
            return Decimal(0)
        tenure = self.user_loan.min_tenure or self.table.bill_tenure

        min_scheduled = self.get_instalment_amount(tenure=tenure)
        if self.user_loan.min_multiplier:
            min_scheduled = min_scheduled * self.user_loan.min_multiplier
        if to_round:
            min_scheduled = round(min_scheduled, 2)
        return min_scheduled

    def get_instalment_amount(
        self,
        principal: Optional[Decimal] = None,
        tenure: Optional[int] = None,
        to_round: Optional[bool] = True,
    ):
        if not tenure:
            tenure = self.table.bill_tenure
        if not principal:
            principal = self.table.principal
        instalment = get_monthly_instalment(
            principal=principal,
            down_payment_percentage=self.user_loan.downpayment_percent,
            interest_type=self.user_loan.interest_type,
            interest_rate_monthly=self.user_loan.rc_rate_of_interest_monthly,
            number_of_instalments=tenure,
            to_round=to_round,
            round_to=self.round_emi_to,
        )
        return instalment

    def get_min_amount_to_add(self) -> Decimal:
        scheduled_minimum_amount = self.get_scheduled_min_amount()
        max_remaining_amount = self.get_remaining_max()
        amount_already_present_in_min = self.get_remaining_min()
        if amount_already_present_in_min == max_remaining_amount:
            return Decimal(0)
        amount_that_can_be_added_in_min = max_remaining_amount - amount_already_present_in_min
        return min(scheduled_minimum_amount, amount_that_can_be_added_in_min)

    def get_remaining_min(self, to_date: Optional[DateTime] = None) -> Decimal:
        _, min_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/min/a", to_date=to_date
        )
        return min_due

    def get_remaining_max(
        self, as_of_date: Optional[DateTime] = None, event_id: Optional[int] = None
    ) -> Decimal:
        _, max_amount = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/max/a", to_date=as_of_date, event_id=event_id
        )
        return max_amount

    def get_outstanding_amount(self, as_of_date: Optional[DateTime] = None) -> Decimal:
        # If not generated or as_of_date is less than bill close date, get the unbilled amount.
        if not self.table.is_generated or as_of_date and as_of_date.date() < self.table.bill_close_date:
            outstanding_balance = self.get_unbilled_amount(as_of_date)
        else:
            outstanding_balance = self.get_remaining_max(as_of_date)
        return outstanding_balance

    def get_unbilled_amount(self, to_date: Optional[DateTime] = None):
        _, unbilled = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/unbilled/a", to_date=to_date
        )
        return unbilled

    def is_bill_closed(self, to_date: Optional[DateTime] = None) -> bool:
        # Simply check if max balance is paid.
        _, total_remaining_amount = get_account_balance_from_str(
            self.session, book_string=f"{self.table.id}/bill/max/a", to_date=to_date
        )
        return total_remaining_amount == 0

    def sum_of_atm_transactions(self):
        atm_transactions_sum = (
            self.session.query(func.sum(CardTransaction.amount))
            .filter_by(loan_id=self.table.id, source="ATM", status="CONFIRMED")
            .group_by(CardTransaction.loan_id)
            .scalar()
        )
        return atm_transactions_sum or 0

    def get_interest_due(self):
        _, interest_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/interest_receivable/a"
        )
        return interest_due

    def get_principal_due(self):
        _, principal_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/principal_receivable/a"
        )
        return principal_due

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        return {"months": 1, "day": 15}

    def get_down_payment(self, include_first_emi=False) -> Decimal:
        down_payment = get_down_payment(
            principal=self.table.principal,
            down_payment_percentage=self.user_loan.downpayment_percent,
            interest_rate_monthly=self.user_loan.rc_rate_of_interest_monthly,
            interest_type=self.user_loan.interest_type,
            number_of_instalments=self.table.bill_tenure,
            include_first_emi_amount=include_first_emi,
        )
        return down_payment

    def get_interest_to_accrue(self, for_date: date):
        # Get the previous emi's interest for cards.
        interest_to_accrue = (
            self.session.query(LoanSchedule.interest_due)
            .filter(
                LoanSchedule.bill_id == self.table.id,
                LoanSchedule.due_date < for_date,
                LoanSchedule.due_date > for_date - relativedelta(months=1),  # Should be within a month
            )
            .order_by(LoanSchedule.due_date)
            .limit(1)
            .scalar()
        )
        return interest_to_accrue


B = TypeVar("B", bound=BaseBill)


def _convert_to_bill_class_decorator(function: Callable[["BaseLoan"], LoanData]) -> Callable:
    def f(self):
        bills = function(self)
        if not bills:
            return None
        if type(bills) is list:
            return [self.convert_to_bill_class(bill) for bill in bills]
        return self.convert_to_bill_class(bills)

    return f


class BaseLoan(Loan):
    should_reinstate_limit_on_payment: bool = False
    bill_class: Type[B] = BaseBill
    session: Session = None
    subclasses = []

    __mapper_args__ = {"polymorphic_identity": "base_loan"}

    def __init__(self, session: Session, **kwargs):
        self.session = session
        super().__init__(**kwargs)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses.append(cls)

    @hybrid_property
    def loan_id(self):
        return self.id

    @hybrid_property
    def card_activation_date(self):
        return self.amortization_date

    @staticmethod
    def get_limit_type(mcc: str) -> str:
        return "available_limit"

    def prepare(self, session: Session) -> None:
        self.session = session

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        user_product_id = kwargs.pop("user_product_id", None)
        card_type = kwargs.pop("card_type")
        if not user_product_id:
            from rush.card.utils import create_user_product_mapping

            user_product = create_user_product_mapping(
                session=session,
                user_id=kwargs["user_id"],
                product_type=card_type,
            )

            user_product_id = user_product.id

            from rush.card.utils import create_loan

            create_loan(session=session, user_product=user_product, lender_id=kwargs["lender_id"])

        loan = session.query(cls).filter(cls.user_product_id == user_product_id).one()
        loan.prepare(session=session)

        loan.rc_rate_of_interest_monthly = kwargs.get("rc_rate_of_interest_monthly")
        loan.lender_rate_of_interest_annual = kwargs.get("lender_rate_of_interest_annual", Decimal(18))
        loan.amortization_date = kwargs.get("card_activation_date")
        loan.min_tenure = kwargs.get("min_tenure")
        loan.min_multiplier = kwargs.get("min_multiplier")
        loan.interest_type = kwargs.get("interest_type", "flat")
        loan.tenure_in_months = kwargs.get("tenure")
        loan.loan_status = kwargs.get("loan_status", "NOT STARTED")
        # Don't want to overwrite default value in case of None.
        if kwargs.get("interest_free_period_in_days"):
            loan.interest_free_period_in_days = kwargs.get("interest_free_period_in_days")
        loan.sub_product_type = "card"
        return loan

    def reinstate_limit_on_payment(self, event: LedgerTriggerEvent, amount: Decimal) -> None:
        assert self.should_reinstate_limit_on_payment == True

        from rush.ledger_events import limit_assignment_event

        limit_assignment_event(session=self.session, loan_id=self.loan_id, event=event, amount=amount)

    def convert_to_bill_class(self, bill: LoanData) -> BaseBill:
        if not bill:
            return None
        return self.bill_class(session=self.session, user_loan=self, loan_data=bill)

    def create_bill(
        self,
        bill_start_date: Date,
        bill_close_date: Date,
        bill_due_date: Date,
        lender_id: int,
        is_generated: bool,
    ) -> BaseBill:
        new_bill = LoanData(
            user_id=self.user_id,
            loan_id=self.loan_id,
            bill_start_date=bill_start_date,
            bill_close_date=bill_close_date,
            bill_due_date=bill_due_date,
            is_generated=is_generated,
            bill_tenure=self.tenure_in_months,
        )
        self.session.add(new_bill)
        self.session.flush()
        return self.convert_to_bill_class(new_bill)

    def get_unpaid_generated_bills(self) -> List[BaseBill]:
        return self.get_all_bills(are_generated=True, only_unpaid_bills=True)

    def get_unpaid_bills(self) -> List[BaseBill]:
        return self.get_all_bills(are_generated=False, only_unpaid_bills=True)

    def get_closed_bills(self) -> List[BaseBill]:
        return self.get_all_bills(are_generated=True, only_closed_bills=True)

    def get_all_bills(
        self,
        are_generated: bool = False,
        only_unpaid_bills: bool = False,
        only_closed_bills: bool = False,
    ) -> List[BaseBill]:
        all_bills_query = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.loan_id)
            .order_by(LoanData.bill_start_date)
        )
        if are_generated:
            all_bills_query = all_bills_query.filter(LoanData.is_generated.is_(True))
        query_result = all_bills_query.all()
        all_bills = [self.convert_to_bill_class(bill) for bill in query_result]
        if only_unpaid_bills:
            all_bills = [bill for bill in all_bills if not bill.is_bill_closed()]
        elif only_closed_bills:
            all_bills = [bill for bill in all_bills if bill.is_bill_closed()]
        return all_bills

    def get_all_bills_post_date(self, post_date: DateTime) -> List[BaseBill]:
        all_bills = (
            self.session.query(LoanData)
            .filter(
                LoanData.user_id == self.user_id,
                LoanData.is_generated.is_(True),
                LoanData.bill_start_date >= post_date,
            )
            .order_by(LoanData.bill_start_date)
            .all()
        )
        all_bills = [self.convert_to_bill_class(bill) for bill in all_bills]
        return all_bills

    def get_last_unpaid_bill(self) -> BaseBill:
        all_bills = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.loan_id)
            .order_by(LoanData.bill_start_date)
            .all()
        )
        all_bills = [self.convert_to_bill_class(bill) for bill in all_bills]
        unpaid_bills = [bill for bill in all_bills if not bill.is_bill_closed()]
        if unpaid_bills:
            return unpaid_bills[0]
        return None

    @_convert_to_bill_class_decorator
    def get_latest_generated_bill(self) -> LoanData:
        latest_bill = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.loan_id, LoanData.is_generated.is_(True))
            .order_by(LoanData.bill_start_date.desc())
            .first()
        )
        return latest_bill

    @_convert_to_bill_class_decorator
    def get_latest_bill_to_generate(self) -> LoanData:
        loan_data = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.loan_id, LoanData.is_generated.is_(False))
            .order_by(LoanData.bill_start_date)
            .first()
        )
        return loan_data

    @_convert_to_bill_class_decorator
    def get_latest_bill(self) -> LoanData:
        loan_data = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.id)
            .order_by(LoanData.bill_start_date.desc())
            .first()
        )
        return loan_data

    def get_remaining_min(
        self,
        date_to_check_against: Optional[DateTime] = None,
        include_child_loans: Optional[bool] = True,
    ) -> Decimal:
        # if user is in moratorium then return 0
        if LoanMoratorium.is_in_moratorium(self.session, self.id, date_to_check_against):
            return Decimal(0)

        unpaid_bills = self.get_unpaid_generated_bills()
        remaining_min_of_all_bills = sum(
            bill.get_remaining_min(date_to_check_against) for bill in unpaid_bills
        )

        if include_child_loans:
            child_loans = self.get_child_loans()
            child_loans_min = sum(
                loan.get_remaining_min(date_to_check_against=date_to_check_against)
                for loan in child_loans
            )
            remaining_min_of_all_bills += child_loans_min

        return remaining_min_of_all_bills

    def get_remaining_max(
        self,
        date_to_check_against: Optional[DateTime] = None,
        event_id: int = None,
        include_child_loans: Optional[bool] = True,
    ) -> Decimal:
        bills = self.get_all_bills()
        remaining_max_of_all_bills = sum(
            bill.get_remaining_max(date_to_check_against, event_id) for bill in bills
        )

        if include_child_loans:
            child_loans = self.get_child_loans()
            child_loans_min = sum(
                loan.get_remaining_min(date_to_check_against, event_id) for loan in child_loans
            )
            remaining_max_of_all_bills += child_loans_min

        return remaining_max_of_all_bills

    def get_total_outstanding(self, date_to_check_against: DateTime = None) -> Decimal:
        all_bills = self.get_all_bills()
        total_outstanding = sum(bill.get_outstanding_amount(date_to_check_against) for bill in all_bills)
        return total_outstanding

    def get_loan_schedule(
        self, only_unpaid_emis=False, only_emis_after_date: Optional[date] = None
    ) -> List[LoanSchedule]:
        q = self.session.query(LoanSchedule).filter(
            LoanSchedule.loan_id == self.loan_id, LoanSchedule.bill_id.is_(None)
        )
        if only_unpaid_emis:
            # Status doesn't determine if emi is completely settled or not.
            q = q.filter(LoanSchedule.remaining_amount != 0)
        if only_emis_after_date:
            q = q.filter(LoanSchedule.due_date >= only_emis_after_date)
        emis = q.order_by(LoanSchedule.emi_number).all()
        return emis

    def get_child_loans(self) -> List["BaseLoan"]:
        return []

    def cancel(self) -> bool:
        if self.loan_status not in ("NOT STARTED", "FEE PAID"):
            return False

        self.loan_status = "CANCELLED"
        LedgerTriggerEvent.new(
            self.session,
            name="cancel_loan",
            loan_id=self.loan_id,
            post_date=get_current_ist_time(),
        )
        self.session.flush()
        return True

    def get_emi_to_accrue_interest(self, post_date: Date):
        loan_schedule = (
            self.session.query(LoanSchedule)
            .filter(
                LoanSchedule.loan_id == self.loan_id,
                LoanSchedule.bill_id.is_(None),
                LoanSchedule.due_date < post_date,
                LoanSchedule.due_date > post_date - relativedelta(months=1),  # Should be within a month
            )
            .order_by(LoanSchedule.due_date.desc())
            .limit(1)
            .scalar()
        )
        return loan_schedule

    def can_close_loan(self, as_of_event_id: int) -> bool:
        max_remaining = self.get_remaining_max(event_id=as_of_event_id, include_child_loans=False)
        return max_remaining == 0
