from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from dateutil.relativedelta import relativedelta
from pendulum import Date
from sqlalchemy import func
from sqlalchemy.orm.session import Session

from rush.card.base_card import (
    B,
    BaseLoan,
)
from rush.card.term_loan import TermLoanBill
from rush.ledger_events import (
    add_max_amount_event,
    loan_disbursement_event,
)
from rush.ledger_utils import create_ledger_entry_from_str
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
    LoanSchedule,
    ProductFee,
)


class ResetBill(TermLoanBill):
    round_emi_to_nearest: Decimal = Decimal("10")
    add_emi_one_to_downpayment: bool = False

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        if emi_number == 1:
            return {"months": 0, "days": 0}
        return {"months": 1, "days": 0}


class ResetCard(BaseLoan):
    bill_class: Type[B] = ResetBill
    can_generate_bill: bool = False

    __mapper_args__ = {"polymorphic_identity": "term_loan_reset"}

    @staticmethod
    def calculate_first_emi_date(product_order_date: Date) -> Date:
        return product_order_date.add(months=1)

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        user_product_id = kwargs["user_product_id"]

        # assert joining fees.
        joining_fees = (
            session.query(ProductFee.id)
            .filter(
                ProductFee.user_id == kwargs["user_id"],
                ProductFee.identifier_id == user_product_id,
                ProductFee.name == "reset_joining_fees",
                ProductFee.fee_status == "PAID",
            )
            .scalar()
        )

        assert joining_fees is not None

        # create loan.
        loan = cls(
            session=session,
            user_id=kwargs["user_id"],
            user_product_id=user_product_id,
            lender_id=kwargs["lender_id"],
            rc_rate_of_interest_monthly=Decimal(
                kwargs["interest_rate"]
            ),  # this will probably come from user's end.
            lender_rate_of_interest_annual=Decimal(18),
            amortization_date=kwargs["product_order_date"],
            downpayment_percent=Decimal("0"),
        )
        session.add(loan)
        session.flush()

        kwargs["loan_id"] = loan.id

        bill_start_date, bill_close_date = cls.bill_class.calculate_bill_start_and_close_date(
            first_bill_date=cls.calculate_first_emi_date(product_order_date=loan.amortization_date),
            tenure=kwargs["tenure"],
        )

        principal_instalment = cls.bill_class.calculate_principal_instalment(
            product_price=kwargs["amount"],
            tenure=kwargs["tenure"],
            downpayment_perc=loan.downpayment_percent,
        )

        loan_data = LoanData(
            user_id=kwargs["user_id"],
            loan_id=kwargs["loan_id"],
            bill_start_date=bill_start_date,
            bill_close_date=bill_close_date,
            bill_due_date=bill_start_date + relativedelta(days=kwargs["interest_free_period_in_days"]),
            is_generated=True,
            bill_tenure=kwargs["tenure"],
            principal=kwargs["amount"],
            principal_instalment=principal_instalment,
        )
        session.add(loan_data)
        session.flush()

        event = LedgerTriggerEvent(
            performed_by=kwargs["user_id"],
            name="reset_disbursal_event",
            loan_id=kwargs["loan_id"],
            post_date=kwargs["product_order_date"],  # what is post_date?
            amount=kwargs["amount"],
        )

        session.add(event)
        session.flush()

        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{loan.id}/card/locked_limit/a",
            credit_book_str=f"{loan.id}/card/locked_limit/l",
            amount=kwargs["amount"],
        )

        loan_disbursement_event(
            session=session,
            loan=loan,
            event=event,
            bill_id=loan_data.id,
        )

        # unlock some limit if required.

        # create emis for term loan.
        from rush.loan_schedule.loan_schedule import create_bill_schedule

        bill = cls.bill_class(session=session, loan_data=loan_data)
        loan_data.interest_to_charge = bill.get_interest_to_charge(
            rate_of_interest=loan.rc_rate_of_interest_monthly,
            principal=kwargs["amount"],
        )

        create_bill_schedule(
            session=session,
            user_loan=loan,
            bill=bill,
        )

        # Due to the way code is written, if max amount is not added first then
        # bill will be treated as closed. I am not sure of the exact reason behind it.

        total_billed_amount = (
            session.query(func.sum(LoanSchedule.total_due_amount()))
            .filter(
                LoanSchedule.loan_id == loan.id,
                LoanSchedule.bill_id.is_(None),
                LoanSchedule.payment_status == "UnPaid",
            )
            .group_by(LoanSchedule.loan_id)
            .scalar()
        )

        assert total_billed_amount >= kwargs["amount"]

        add_max_amount_event(session=session, bill=bill, event=event, amount=total_billed_amount)

        add_min_to_all_bills(session=session, post_date=kwargs["product_order_date"], user_loan=loan)
        return loan
