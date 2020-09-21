from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from dateutil.relativedelta import relativedelta
from pendulum import Date
from sqlalchemy.orm.session import Session

from rush.card.base_card import (
    B,
    BaseLoan,
)
from rush.card.term_loan import TermLoanBill

# from rush.card.utils import add_instrument_to_loan
from rush.ledger_events import loan_disbursement_event
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
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
    def calculate_amortization_date(product_order_date: Date) -> Date:
        return product_order_date.add(months=1)

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        user_product_id = kwargs["user_product_id"]

        # assert joining fees.

        loan = cls(
            session=session,
            user_id=kwargs["user_id"],
            user_product_id=user_product_id,
            lender_id=kwargs["lender_id"],
            rc_rate_of_interest_monthly=Decimal(3),  # this will probably come from user's end.
            lender_rate_of_interest_annual=Decimal(18),
            amortization_date=kwargs.get(
                "loan_creation_date",
                cls.calculate_amortization_date(product_order_date=kwargs["product_order_date"]),
            ),
            downpayment_percent=Decimal("0"),
        )
        session.add(loan)
        session.flush()

        kwargs["loan_id"] = loan.id

        bill_start_date, bill_close_date = cls.bill_class.calculate_bill_start_and_close_date(
            amortization_date=loan.amortization_date, tenure=kwargs["tenure"]
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
        from rush.create_emi import create_emis_for_bill

        bill = cls.bill_class(session=session, loan_data=loan_data)
        loan_data.interest_to_charge = bill.get_interest_to_charge(
            rate_of_interest=loan.rc_rate_of_interest_monthly,
            product_price=kwargs["amount"],
        )

        create_emis_for_bill(
            session=session,
            user_loan=loan,
            bill=bill,
        )

        # add_instrument_to_loan(
        #     session=session,
        #     instrument_type="card",
        #     loan=loan,
        #     instrument_info={
        #         "kit_number": kwargs["kit_number"],
        #         "activation_type": kwargs["activation_type"],
        #         "activation_date": kwargs["activation_date"],
        #         "card_name": kwargs["card_name"]
        #     }
        # )

        return loan
