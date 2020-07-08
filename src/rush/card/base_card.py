from datetime import timedelta
from decimal import Decimal
from typing import (
    List,
    Optional,
    Type,
    TypeVar,
)

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    LoanData,
    UserCard,
)
from rush.utils import (
    div,
    mul,
    round_up_decimal,
)


class BaseBill:
    session: Session = None
    table: LoanData = None

    def __init__(self, session: Session, loan_data: LoanData):
        self.session = session
        self.table = loan_data
        self.__dict__.update(loan_data.__dict__)

    def get_interest_to_charge(self):
        # TODO get tenure from table.
        interest_on_principal = mul(
            self.table.principal, div(div(self.rc_rate_of_interest_annual, 12), 100)
        )
        not_rounded_emi = self.table.principal_instalment + interest_on_principal
        rounded_emi = round_up_decimal(not_rounded_emi)

        rounding_difference = rounded_emi - not_rounded_emi

        new_interest = interest_on_principal + rounding_difference
        return new_interest

    def get_min_per_month(self):
        return self.table.principal_instalment + self.table.interest_to_charge

    def get_minimum_amount_to_pay(self, to_date: Optional[DateTime] = None) -> Decimal:
        from rush.ledger_utils import get_account_balance_from_str

        _, min_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/min/a", to_date=to_date
        )
        return min_due

    def is_bill_closed(self, to_date: Optional[DateTime] = None) -> bool:
        # Check if principal is paid. If not, return false.
        _, principal_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/principal_receivable/a", to_date=to_date
        )
        if principal_due != 0:
            return False

        # Check if interest is paid. If not, return false.
        _, interest_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/interest_receivable/a", to_date=to_date
        )
        if interest_due != 0:
            return False

        # Check if late fine is paid. If not, return false.
        _, late_fine_due = get_account_balance_from_str(
            self.session, book_string=f"{self.id}/bill/late_fine_receivable/a", to_date=to_date
        )
        if late_fine_due != 0:
            return False
        return True


B = TypeVar("B", bound=BaseBill)


class BaseCard:
    session: Session = None
    table: UserCard = None

    def __init__(self, session: Session, bill_class: Type[B], user_card: UserCard):
        self.session = session
        self.bill_class = bill_class
        self.table = user_card
        self.__dict__.update(user_card.__dict__)

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
        new_bill_date: Date,
        lender_id: int,
        rc_rate_of_interest_annual: Decimal,
        lender_rate_of_interest_annual: Decimal,
        is_generated: bool,
    ) -> BaseBill:
        new_bill = LoanData(
            user_id=self.user_id,
            card_id=self.id,
            lender_id=lender_id,
            agreement_date=new_bill_date,
            rc_rate_of_interest_annual=rc_rate_of_interest_annual,
            lender_rate_of_interest_annual=lender_rate_of_interest_annual,
            is_generated=is_generated,
        )
        self.session.add(new_bill)
        self.session.flush()
        return self.convert_to_bill_class(new_bill)

    def get_unpaid_bills(self) -> List[BaseBill]:
        all_bills = (
            self.session.query(LoanData)
            .filter(LoanData.user_id == self.user_id, LoanData.is_generated.is_(True))
            .order_by(LoanData.agreement_date)
            .all()
        )
        all_bills = [self.convert_to_bill_class(bill) for bill in all_bills]
        unpaid_bills = [bill for bill in all_bills if not bill.is_bill_closed()]
        return unpaid_bills

    @_convert_to_bill_class_decorator
    def get_latest_generated_bill(self) -> BaseBill:
        latest_bill = (
            self.session.query(LoanData)
            .filter(LoanData.card_id == self.id, LoanData.is_generated.is_(True))
            .order_by(LoanData.agreement_date.desc())
            .first()
        )
        return latest_bill

    @_convert_to_bill_class_decorator
    def get_latest_bill_to_generate(self) -> BaseBill:
        loan_data = (
            self.session.query(LoanData)
            .filter(LoanData.card_id == self.id, LoanData.is_generated.is_(False))
            .order_by(LoanData.agreement_date)
            .first()
        )
        return loan_data