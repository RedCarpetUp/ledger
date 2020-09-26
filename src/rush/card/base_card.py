from decimal import Decimal
from typing import (
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Session

from rush.card.utils import create_user_product_mapping
from rush.ledger_utils import (
    get_account_balance_from_str,
    is_bill_closed,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    Loan,
    LoanData,
    LoanMoratorium,
    LoanSchedule,
    UserCard,
)
from rush.utils import (
    div,
    get_current_ist_time,
    mul,
    round_up_decimal_to_nearest,
)


class BaseBill:
    session: Session = None
    table: LoanData = None
    round_emi_to_nearest: Decimal = Decimal("1")

    def __init__(self, session: Session, loan_data: LoanData):
        self.session = session
        self.table = loan_data
        self.__dict__.update(loan_data.__dict__)

    def get_interest_to_charge(
        self, rate_of_interest: Decimal, product_price: Optional[Decimal] = None
    ) -> Decimal:
        # TODO get tenure from table.
        principal = self.table.principal
        if product_price:
            principal = product_price

        interest_on_principal = mul(principal, div(rate_of_interest, 100))
        not_rounded_emi = self.table.principal_instalment + interest_on_principal
        rounded_emi = round_up_decimal_to_nearest(not_rounded_emi, to_nearest=self.round_emi_to_nearest)

        rounding_difference = rounded_emi - not_rounded_emi

        new_interest = interest_on_principal + rounding_difference
        return new_interest

    def get_scheduled_min_amount(self) -> Decimal:
        if not self.table.is_generated:
            return Decimal(0)
        user_loan = self.session.query(Loan).filter_by(id=self.table.loan_id).one_or_none()
        if user_loan.min_tenure:
            new_instalment = div(self.table.principal, user_loan.min_tenure)
            min_scheduled = new_instalment + self.table.interest_to_charge
        elif user_loan.min_multiplier:
            min_without_scheduled_multiplier = (
                self.table.principal_instalment + self.table.interest_to_charge
            )
            min_scheduled = min_without_scheduled_multiplier * user_loan.min_multiplier
        else:
            min_scheduled = self.table.principal_instalment + self.table.interest_to_charge
        return min_scheduled

    def get_min_amount_to_add(
        self, date_to_check_against: DateTime = get_current_ist_time().date()
    ) -> Decimal:
        # only charge interest if in moratorium.
        if LoanMoratorium.is_in_moratorium(self.session, self.loan_id, date_to_check_against):
            scheduled_minimum_amount = self.table.interest_to_charge
        else:
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

    def get_remaining_max(self, as_of_date: Optional[DateTime] = None) -> Decimal:
        _, max_amount = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/max/a", to_date=as_of_date
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
        return is_bill_closed(self.session, self.table, to_date)

    def sum_of_atm_transactions(self):
        atm_transactions_sum = (
            self.session.query(func.sum(CardTransaction.amount))
            .filter_by(loan_id=self.table.id, source="ATM")
            .group_by(CardTransaction.loan_id)
            .scalar()
        )
        return atm_transactions_sum or 0

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        return {"months": 1, "days": 15}


B = TypeVar("B", bound=BaseBill)


class BaseLoan(Loan):
    should_reinstate_limit_on_payment: bool = False
    bill_class: Type[B] = BaseBill
    session: Session = None
    downpayment_perc: Optional[Decimal] = None

    __mapper_args__ = {"polymorphic_identity": "base_loan"}

    def __init__(self, session: Session, **kwargs):
        self.session = session
        super().__init__(**kwargs)

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
        user_product_id = kwargs.get("user_product_id")
        card_type = kwargs.pop("card_type")
        if not user_product_id:
            user_product_id = create_user_product_mapping(
                session=session, user_id=kwargs["user_id"], product_type=card_type
            ).id

        loan = cls(
            session=session,
            user_id=kwargs["user_id"],
            user_product_id=user_product_id,
            lender_id=kwargs.pop("lender_id"),
            rc_rate_of_interest_monthly=Decimal(3),
            lender_rate_of_interest_annual=Decimal(18),  # this is hardcoded for one lender.
            amortization_date=kwargs.get("card_activation_date"),  # TODO: change this later.
            min_tenure=kwargs.pop("min_tenure", None),
            min_multiplier=kwargs.pop("min_multiplier", None),
        )

        # Don't want to overwrite default value in case of None.
        if kwargs.get("interest_free_period_in_days"):
            loan.interest_free_period_in_days = kwargs.pop("interest_free_period_in_days")

        session.add(loan)
        session.flush()

        kwargs["loan_id"] = loan.id

        kwargs["card_name"] = kwargs.get("card_name", "ruby")  # TODO: change this later.
        kwargs["activation_type"] = kwargs.get("activation_type", "V")  # TODO: change this later.
        kwargs["kit_number"] = kwargs.get("kit_number", "00000")  # TODO: change this later.

        user_card = UserCard(**kwargs)
        session.add(user_card)
        session.flush()

        return loan

    def reinstate_limit_on_payment(self, event: LedgerTriggerEvent, amount: Decimal) -> None:
        assert self.should_reinstate_limit_on_payment == True

        from rush.ledger_events import limit_assignment_event

        limit_assignment_event(session=self.session, loan_id=self.loan_id, event=event, amount=amount)

    def _convert_to_bill_class_decorator(func) -> BaseBill:
        def f(self):
            bills = func(self)
            if not bills:
                return None
            if type(bills) is List:
                return [self.convert_to_bill_class(bill) for bill in bills]
            return self.convert_to_bill_class(bills)

        return f

    def convert_to_bill_class(self, bill: LoanData):
        if not bill:
            return None
        return self.bill_class(self.session, bill)

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
            # lender_id=lender_id,
            bill_start_date=bill_start_date,
            bill_close_date=bill_close_date,
            bill_due_date=bill_due_date,
            is_generated=is_generated,
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
    def get_latest_generated_bill(self) -> BaseBill:
        latest_bill = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.loan_id, LoanData.is_generated.is_(True))
            .order_by(LoanData.bill_start_date.desc())
            .first()
        )
        return latest_bill

    @_convert_to_bill_class_decorator
    def get_latest_bill_to_generate(self) -> BaseBill:
        loan_data = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.loan_id, LoanData.is_generated.is_(False))
            .order_by(LoanData.bill_start_date)
            .first()
        )
        return loan_data

    @_convert_to_bill_class_decorator
    def get_latest_bill(self) -> BaseBill:
        loan_data = (
            self.session.query(LoanData)
            .filter(LoanData.loan_id == self.id)
            .order_by(LoanData.bill_start_date.desc())
            .first()
        )
        return loan_data

    def get_remaining_min(self, date_to_check_against: DateTime = None) -> Decimal:
        # if user is in moratorium then return 0
        if LoanMoratorium.is_in_moratorium(self.session, self.id, date_to_check_against):
            return Decimal(0)
        unpaid_bills = self.get_unpaid_generated_bills()
        remaining_min_of_all_bills = sum(
            bill.get_remaining_min(date_to_check_against) for bill in unpaid_bills
        )
        return remaining_min_of_all_bills

    def get_remaining_max(self, date_to_check_against: DateTime = None) -> Decimal:
        unpaid_bills = self.get_unpaid_generated_bills()
        total_max_amount = sum(bill.get_remaining_max(date_to_check_against) for bill in unpaid_bills)
        return total_max_amount

    def get_total_outstanding(self, date_to_check_against: DateTime = None) -> Decimal:
        all_bills = self.get_all_bills()
        total_outstanding = sum(bill.get_outstanding_amount(date_to_check_against) for bill in all_bills)
        return total_outstanding

    def get_loan_schedule(self, only_unpaid_emis=False) -> List[LoanSchedule]:
        q = self.session.query(LoanSchedule).filter(
            LoanSchedule.loan_id == self.loan_id, LoanSchedule.bill_id.is_(None)
        )
        if only_unpaid_emis:
            q = q.filter(LoanSchedule.payment_status == "UnPaid")
        emis = q.order_by(LoanSchedule.emi_number).all()
        return emis
