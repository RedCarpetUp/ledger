from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy import or_
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql import func

from rush.anomaly_detection import get_payment_events
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.models import (
    BillFee,
    BookAccount,
    CardEmis,
    EmiPaymentMapping,
    EventDpd,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
)
from rush.utils import (
    div,
    mul,
)


def create_emis_for_bill(
    session: Session,
    user_loan: BaseLoan,
    bill: BaseBill,
    last_emi: Optional[CardEmis] = None,
    bill_accumalation_till_date: Optional[Decimal] = None,
) -> None:
    assert bill.table.id is not None
    bill_data = bill.table
    if not last_emi:
        due_date = bill_data.bill_start_date
        if "term_loan" in user_loan.product_type:
            principal_due = bill_data.principal - bill.get_downpayment_amount(
                product_price=bill_data.principal, downpayment_perc=user_loan.downpayment_percent
            )
        else:
            principal_due = Decimal(bill_data.principal)
        due_amount = bill_data.principal_instalment
        start_emi_number = difference_counter = 1
    else:
        due_date = last_emi.due_date
        principal_due = Decimal(bill_data.principal - bill_accumalation_till_date)
        due_amount = div(principal_due, bill_data.bill_tenure - last_emi.emi_number)
        start_emi_number = last_emi.emi_number + 1
        difference_counter = last_emi.emi_number
    total_interest = current_interest = next_interest = Decimal(0)
    for i in range(start_emi_number, bill_data.bill_tenure + 1):
        deltas_for_due_date = bill.get_relative_delta_for_emi(
            emi_number=i, amortization_date=user_loan.amortization_date
        )

        # in relativedelta, `days` arg is for interval
        # whereas `day` arg is for replace functionality.
        if deltas_for_due_date["days"] < 0:
            due_date += relativedelta(
                months=deltas_for_due_date["months"], days=deltas_for_due_date["days"]
            )
        else:
            due_date += relativedelta(
                months=deltas_for_due_date["months"], day=deltas_for_due_date["days"]
            )

        total_due_amount = due_amount

        # if term-loan and first emi, downpayment is also added in total_due_amount.
        if i == 1 and "term_loan" in user_loan.product_type:
            total_due_amount += bill.get_downpayment_amount(
                product_price=bill_data.principal, downpayment_perc=user_loan.downpayment_percent
            )
        total_closing_balance = (
            principal_due - mul(due_amount, (i - difference_counter))
            if principal_due - mul(due_amount, (i - difference_counter)) > 0
            else due_amount
        )
        total_closing_balance_post_due_date = (
            principal_due - mul(due_amount, (i - difference_counter))
            if principal_due - mul(due_amount, (i - difference_counter)) > 0
            else due_amount
        )
        interest = bill_data.interest_to_charge if bill_data.interest_to_charge else Decimal(0)
        current_interest = div(mul(interest, (30 - due_date.day)), 30)
        next_interest = interest - current_interest
        total_interest = current_interest + next_interest
        total_due_amount += interest
        total_closing_balance_post_due_date += interest
        new_emi = CardEmis(
            loan_id=user_loan.loan_id,
            bill_id=bill_data.id,
            emi_number=i,
            total_closing_balance=total_closing_balance,
            total_closing_balance_post_due_date=total_closing_balance_post_due_date,
            due_amount=due_amount,
            interest=total_interest,
            interest_current_month=current_interest,
            interest_next_month=next_interest,
            total_due_amount=total_due_amount,
            due_date=due_date,
        )
        session.add(new_emi)
    session.flush()

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)


def slide_payments(user_loan: BaseLoan, payment_event: Optional[LedgerTriggerEvent] = None) -> None:
    def slide_payments_repeated_logic(
        all_emis,
        payment_received_and_adjusted,
        payment_request_id,
        last_payment_date,
        last_paid_emi_number,
        all_paid=False,
    ) -> None:
        last_emi_number = all_emis[-1].emi_number
        for emi in all_emis:
            if (
                emi.emi_number <= last_paid_emi_number
                or emi.total_due_amount <= Decimal(0)
                or emi.payment_status == "Paid"
            ):
                continue
            if last_payment_date:
                emi.last_payment_date = last_payment_date
            payment_received_and_adjusted += (
                emi.payment_received
                + emi.atm_fee_received
                + emi.late_fee_received
                + emi.interest_received
            )
            emi.dpd = (last_payment_date.date() - emi.due_date).days
            if all_paid:
                emi.payment_received = (
                    emi.atm_fee_received
                ) = (
                    emi.late_fee_received
                ) = (
                    emi.interest_received
                ) = (
                    emi.due_amount
                ) = (
                    emi.total_due_amount
                ) = (
                    emi.total_closing_balance
                ) = (
                    emi.total_closing_balance_post_due_date
                ) = emi.interest_current_month = emi.interest_next_month = emi.interest = Decimal(0)
                emi.payment_status = "Paid"
                emi.dpd = 0
                last_paid_emi_number = emi.emi_number
                continue
            if payment_received_and_adjusted:
                actual_closing_balance = emi.total_closing_balance_post_due_date
                if last_payment_date.date() <= emi.due_date:
                    actual_closing_balance = emi.total_closing_balance
                if payment_received_and_adjusted >= actual_closing_balance > 0 and (
                    (
                        emi.due_date
                        >= last_payment_date.date()
                        > (emi.due_date + relativedelta(months=-1))
                    )
                    or emi.emi_number == last_emi_number
                ):
                    all_paid = True
                    emi.late_fee_received = emi.late_fee
                    emi.atm_fee_received = emi.atm_fee
                    # Edge case of last emi
                    if emi.emi_number == last_emi_number and last_payment_date.date() > emi.due_date:
                        emi.interest_received = emi.interest
                        emi.payment_received = actual_closing_balance - emi.late_fee - emi.interest - emi.atm_fee
                        emi.total_closing_balance = emi.total_closing_balance_post_due_date = 0
                    else:
                        emi.payment_received = actual_closing_balance - emi.late_fee - emi.atm_fee
                        emi.total_closing_balance = (
                            emi.total_closing_balance_post_due_date
                        ) = emi.interest = emi.interest_current_month = emi.interest_next_month = 0
                    if last_payment_date.date() > emi.due_date:
                        only_principal = actual_closing_balance - (
                            emi.interest + emi.atm_fee + emi.late_fee
                        )
                    else:
                        only_principal = actual_closing_balance - emi.atm_fee
                    emi.total_due_amount = actual_closing_balance
                    emi.due_amount = only_principal
                    last_paid_emi_number = emi.emi_number
                    emi.payment_status = "Paid"
                    emi.dpd = 0
                    # Create payment mapping
                    create_emi_payment_mapping(
                        session=session,
                        user_loan=user_loan,
                        emi_number=emi.emi_number,
                        payment_date=last_payment_date,
                        payment_request_id=payment_request_id,
                        interest_received=emi.interest_received,
                        late_fee_received=emi.late_fee_received,
                        atm_fee_received=emi.atm_fee_received,
                        principal_received=emi.payment_received,
                    )
                    continue
                diff = emi.total_due_amount - payment_received_and_adjusted
                if diff >= 0:
                    if diff == 0:
                        last_paid_emi_number = emi.emi_number
                        emi.payment_status = "Paid"
                        emi.dpd = 0
                    if (
                        emi.atm_fee > 0
                        and (emi.atm_fee_received + payment_received_and_adjusted) <= emi.atm_fee
                    ):
                        emi.atm_fee_received += payment_received_and_adjusted
                        emi.total_closing_balance -= payment_received_and_adjusted
                        emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                        # Create payment mapping
                        create_emi_payment_mapping(
                            session=session,
                            user_loan=user_loan,
                            emi_number=emi.emi_number,
                            payment_date=last_payment_date,
                            payment_request_id=payment_request_id,
                            interest_received=emi.interest_received,
                            late_fee_received=emi.late_fee_received,
                            atm_fee_received=emi.atm_fee_received,
                            principal_received=emi.payment_received,
                        )
                        break
                    else:
                        if 0 < emi.atm_fee < (emi.atm_fee_received + payment_received_and_adjusted):
                            atm_fee_actually_received = emi.atm_fee - emi.atm_fee_received
                            emi.atm_fee_received = emi.atm_fee
                            payment_received_and_adjusted -= atm_fee_actually_received
                        if (
                            emi.late_fee > 0
                            and (emi.late_fee_received + payment_received_and_adjusted) <= emi.late_fee
                        ):
                            emi.late_fee_received += payment_received_and_adjusted
                            emi.total_closing_balance -= payment_received_and_adjusted
                            emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                            # Create payment mapping
                            create_emi_payment_mapping(
                                session=session,
                                user_loan=user_loan,
                                emi_number=emi.emi_number,
                                payment_date=last_payment_date,
                                payment_request_id=payment_request_id,
                                interest_received=emi.interest_received,
                                late_fee_received=emi.late_fee_received,
                                atm_fee_received=emi.atm_fee_received,
                                principal_received=emi.payment_received,
                            )
                            break
                        else:
                            if (
                                0
                                < emi.late_fee
                                < (emi.late_fee_received + payment_received_and_adjusted)
                            ):
                                late_fee_actually_received = emi.late_fee - emi.late_fee_received
                                emi.late_fee_received = emi.late_fee
                                payment_received_and_adjusted -= late_fee_actually_received
                            if (
                                last_payment_date.date() > emi.due_date
                                and emi.interest > 0
                                and (emi.interest_received + payment_received_and_adjusted)
                                <= emi.interest
                            ):
                                emi.interest_received += payment_received_and_adjusted
                                emi.total_closing_balance -= payment_received_and_adjusted
                                emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                                # Create payment mapping
                                create_emi_payment_mapping(
                                    session=session,
                                    user_loan=user_loan,
                                    emi_number=emi.emi_number,
                                    payment_date=last_payment_date,
                                    payment_request_id=payment_request_id,
                                    interest_received=emi.interest_received,
                                    late_fee_received=emi.late_fee_received,
                                    atm_fee_received=emi.atm_fee_received,
                                    principal_received=emi.payment_received,
                                )
                                break
                            else:
                                if last_payment_date.date() > emi.due_date and 0 < emi.interest < (
                                    emi.interest_received + payment_received_and_adjusted
                                ):
                                    interest_actually_received = emi.interest - emi.interest_received
                                    emi.interest_received = emi.interest
                                    payment_received_and_adjusted -= interest_actually_received
                                if payment_received_and_adjusted <= emi.due_amount:
                                    emi.payment_received = payment_received_and_adjusted
                                    emi.total_closing_balance -= payment_received_and_adjusted
                                    emi.total_closing_balance_post_due_date -= (
                                        payment_received_and_adjusted
                                    )
                                    # Create payment mapping
                                    create_emi_payment_mapping(
                                        session=session,
                                        user_loan=user_loan,
                                        emi_number=emi.emi_number,
                                        payment_date=last_payment_date,
                                        payment_request_id=payment_request_id,
                                        interest_received=emi.interest_received,
                                        late_fee_received=emi.late_fee_received,
                                        atm_fee_received=emi.atm_fee_received,
                                        principal_received=emi.payment_received,
                                    )
                                    break
                emi.late_fee_received = emi.late_fee
                emi.interest_received = emi.interest
                emi.payment_received = emi.due_amount
                emi.payment_status = "Paid"
                emi.dpd = 0
                last_paid_emi_number = emi.emi_number
                # Create payment mapping
                create_emi_payment_mapping(
                    session=session,
                    user_loan=user_loan,
                    emi_number=emi.emi_number,
                    payment_date=last_payment_date,
                    payment_request_id=payment_request_id,
                    interest_received=emi.interest_received,
                    late_fee_received=emi.late_fee_received,
                    atm_fee_received=emi.atm_fee_received,
                    principal_received=emi.payment_received,
                )
                payment_received_and_adjusted = abs(diff)

        # Got to close all bill if all payment is done
        if all_paid:
            from rush.create_bill import close_bills

            close_bills(user_loan, last_payment_date)

    session = user_loan.session
    all_emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    if not all_emis:
        # Success and Error handling later
        return
    payment_request_id = None
    last_paid_emi_number = 0
    last_payment_date = None
    all_paid = False
    events = get_payment_events(session=session, user_loan=user_loan)
    if not payment_event:
        for event in events:
            payment_received_and_adjusted = Decimal(0)
            payment_received_and_adjusted += event.amount
            payment_request_id = event.extra_details.get("payment_request_id")
            last_payment_date = event.post_date
            slide_payments_repeated_logic(
                all_emis=all_emis,
                payment_received_and_adjusted=payment_received_and_adjusted,
                payment_request_id=payment_request_id,
                last_payment_date=last_payment_date,
                last_paid_emi_number=last_paid_emi_number,
                all_paid=all_paid,
            )
    else:
        payment_received_and_adjusted = Decimal(0)
        payment_received_and_adjusted += payment_event.amount
        payment_request_id = payment_event.extra_details.get("payment_request_id")
        last_payment_date = payment_event.post_date
        slide_payments_repeated_logic(
            all_emis=all_emis,
            payment_received_and_adjusted=payment_received_and_adjusted,
            payment_request_id=payment_request_id,
            last_payment_date=last_payment_date,
            last_paid_emi_number=last_paid_emi_number,
            all_paid=all_paid,
        )
    session.flush()


def adjust_late_fee_in_emis(session: Session, user_loan: BaseLoan, bill: LoanData) -> None:
    emi = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.bill_id == bill.id,
            CardEmis.emi_number == 1,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.emi_number.asc())
        .first()
    )
    late_fee = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "late_fee")
        .one_or_none()
    )
    if late_fee and late_fee.gross_amount > 0:
        emi.total_closing_balance_post_due_date += late_fee.gross_amount
        emi.total_due_amount += late_fee.gross_amount
        emi.late_fee += late_fee.gross_amount

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)


def adjust_atm_fee_in_emis(session: Session, user_loan: BaseLoan, bill: LoanData) -> None:
    emi = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.bill_id == bill.id,
            CardEmis.emi_number == 1,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.emi_number.asc())
        .first()
    )
    atm_fee = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "atm_fee")
        .one_or_none()
    )
    if atm_fee and atm_fee.gross_amount > 0:
        emi.total_closing_balance_post_due_date += atm_fee.gross_amount
        emi.total_closing_balance += atm_fee.gross_amount
        emi.total_due_amount += atm_fee.gross_amount
        emi.atm_fee += atm_fee.gross_amount
        session.flush()

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)


def create_emi_payment_mapping(
    session: Session,
    user_loan: BaseLoan,
    emi_number: int,
    payment_date: DateTime,
    payment_request_id: str,
    interest_received: Decimal,
    late_fee_received: Decimal,
    atm_fee_received: Decimal,
    principal_received: Decimal,
) -> None:
    new_payment_mapping = EmiPaymentMapping(
        loan_id=user_loan.loan_id,
        emi_number=emi_number,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
        interest_received=interest_received,
        late_fee_received=late_fee_received,
        atm_fee_received=atm_fee_received,
        principal_received=principal_received,
    )
    session.add(new_payment_mapping)
    session.flush()
    return new_payment_mapping


def add_moratorium_to_loan_emi(
    session: Session, user_loan, loan_emis, start_date, months_to_be_inserted: int, bill_id: int
):
    if not loan_emis:
        return {"result": "error", "message": "loan emis required"}
    dict_to_be_shifted_from = next((d for d in loan_emis if d.due_date >= start_date), False)
    final_emi_due_date_difference_with_start = (start_date - loan_emis[len(loan_emis) - 1].due_date).days
    if final_emi_due_date_difference_with_start > 90:
        return {"result": "error", "message": "incorrect start date given"}
    is_insertion_happening_in_the_end = False
    if dict_to_be_shifted_from:
        emi_number_to_begin_insertion_from = dict_to_be_shifted_from.emi_number
    else:
        is_insertion_happening_in_the_end = True
        emi_number_to_begin_insertion_from = loan_emis[len(loan_emis) - 1].emi_number
    if not is_insertion_happening_in_the_end:
        # Get values from shift emi
        shift_emi = loan_emis[emi_number_to_begin_insertion_from - 1]
        shift_emi_due_date = shift_emi.due_date
        shift_emi_closing_balance = shift_emi.total_closing_balance
        shift_emi_closing_balance_post_due_date = shift_emi.total_closing_balance_post_due_date

        for emi in loan_emis:
            if emi.emi_number >= emi_number_to_begin_insertion_from:
                emi.emi_number += months_to_be_inserted
                emi.due_date += relativedelta(months=+months_to_be_inserted)
            elif emi.emi_number != emi_number_to_begin_insertion_from:
                continue

        for i in range(months_to_be_inserted):
            # Need to just update emi related fields because
            # late fine and interest will be handled through events
            new_emi = CardEmis(
                loan_id=user_loan.loan_id,
                bill_id=bill_id,
                emi_number=(emi_number_to_begin_insertion_from + i),
                total_closing_balance=shift_emi_closing_balance,
                total_closing_balance_post_due_date=shift_emi_closing_balance_post_due_date,
                due_amount=Decimal(0),
                late_fee=Decimal(0),
                interest=Decimal(0),
                interest_current_month=Decimal(0),
                interest_next_month=Decimal(0),
                total_due_amount=Decimal(0),
                due_date=(shift_emi_due_date + relativedelta(months=+i)),
                extra_details={"moratorium": True},
                payment_status="Paid",
                row_status="inactive",
            )
            session.add(new_emi)
    else:
        # TODO, Do we even need this case?
        last_emi = loan_emis[-1]
        # for i in range(months_to_be_inserted):
        #     # Need to just update emi related fields because
        #     # late fine and interest will be handled through events
        #     new_emi = CardEmis(
        #         loan_id=user_loan.loan_id,
        #         bill_id=last_emi.bill_id,
        #         emi_number=(emi_number_to_begin_insertion_from + i + 1),
        #         total_closing_balance=last_emi.total_closing_balance,
        #         total_closing_balance_post_due_date=last_emi.total_closing_balance_post_due_date,
        #         due_amount=last_emi.due_amount,
        #         late_fee=last_emi.late_fee,
        #         interest=last_emi.interest,
        #         interest_current_month=last_emi.interest_current_month,
        #         interest_next_month=last_emi.interest_next_month,
        #         total_due_amount=last_emi.total_due_amount,
        #         due_date=last_emi.due_date + relativedelta(months=+(i + 1)),
        #         extra_details={"moratorium": True},
        #         payment_status="Paid",
        #     )
        #     session.add(new_emi)

    # Get bill schedule again
    bill_emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "inactive",
            CardEmis.bill_id == bill_id,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    # Total due amount adjustment
    total_due_amount_addition_interest = 0
    shifted_emi_now_number = emi_number_to_begin_insertion_from + months_to_be_inserted
    for i in range(shifted_emi_now_number, shifted_emi_now_number + months_to_be_inserted):
        total_due_amount_addition_interest += bill_emis[i - 1].interest
    bill_emis[
        emi_number_to_begin_insertion_from + months_to_be_inserted - 1
    ].total_due_amount += total_due_amount_addition_interest

    # Reactivate all emis
    for emi in bill_emis:
        emi.row_status = "active"

    session.flush()
    return {"result": "success"}


def check_moratorium_eligibility(user_loan: BaseLoan):
    session = user_loan.session

    # Check if user has opted for moratorium and adjust that in schedule
    moratorium = (
        session.query(LoanMoratorium).filter(LoanMoratorium.loan_id == user_loan.loan_id).first()
    )
    if moratorium:
        start_date = moratorium.start_date.date()
        end_date = moratorium.end_date.date()
        months_to_be_inserted = (
            (moratorium.end_date.year - moratorium.start_date.year) * 12
            + moratorium.end_date.month
            - moratorium.start_date.month
        )

        all_bills = user_loan.get_all_bills()

        for bill in all_bills:
            bill_emis = (
                session.query(CardEmis)
                .filter(
                    CardEmis.loan_id == user_loan.loan_id,
                    CardEmis.row_status == "active",
                    CardEmis.bill_id == bill.id,
                )
                .order_by(CardEmis.emi_number.asc())
                .all()
            )
            has_any_emis_to_apply_moratorium = [
                emi for emi in bill_emis if emi.due_date >= start_date and emi.due_date < end_date
            ]
            if has_any_emis_to_apply_moratorium:
                # Mark all rows_inactive
                for emi in bill_emis:
                    emi.row_status = "inactive"

                # Process for moratorium
                try:
                    moratorium_start_emi = next(emi for emi in bill_emis if emi.due_date >= start_date)
                except:
                    moratorium_start_emi = None
                if not moratorium_start_emi:
                    return {"result": "error", "message": "Not eligible for moratorium"}

                resp = add_moratorium_to_loan_emi(
                    session=session,
                    user_loan=user_loan,
                    loan_emis=bill_emis,
                    start_date=start_date,
                    months_to_be_inserted=months_to_be_inserted,
                    bill_id=bill.id,
                )
                if resp["result"] == "error":
                    return resp

        # Recreate loan level emis
        group_bills_to_create_loan_schedule(user_loan=user_loan)


def group_bills_to_create_loan_schedule(user_loan: BaseLoan):
    session = user_loan.session

    # Get all loan level emis of the user
    all_emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    for emi in all_emis:
        emi.row_status = "inactive"

    all_payment_mappings = (
        session.query(EmiPaymentMapping)
        .filter(EmiPaymentMapping.loan_id == user_loan.loan_id, EmiPaymentMapping.row_status == "active")
        .all()
    )
    for mapping in all_payment_mappings:
        mapping.row_status = "inactive"

    grouped_values = (
        session.query(
            CardEmis.due_date,
            func.sum(CardEmis.due_amount).label("sum_due_amount"),
            func.sum(CardEmis.total_due_amount).label("sum_total_due_amount"),
            func.sum(CardEmis.interest_current_month).label("sum_interest_current_month"),
            func.sum(CardEmis.interest_next_month).label("sum_interest_next_month"),
            func.sum(CardEmis.interest).label("sum_interest"),
            func.sum(CardEmis.late_fee).label("sum_late_fee"),
            func.sum(CardEmis.atm_fee).label("sum_atm_fee"),
            func.sum(CardEmis.total_closing_balance).label("sum_total_closing_balance"),
            func.sum(CardEmis.total_closing_balance_post_due_date).label(
                "sum_total_closing_balance_post_due_date"
            ),
            func.jsonb_object_agg(CardEmis.bill_id, CardEmis.total_due_amount).label(
                "total_extra_details"
            ),
        )
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id != None,
        )
        .group_by(CardEmis.due_date)
        .order_by(CardEmis.due_date.asc())
        .all()
    )

    emi_number = 1
    for cumulative_emi in grouped_values:
        new_emi = CardEmis(
            loan_id=user_loan.loan_id,
            bill_id=None,
            emi_number=emi_number,
            total_closing_balance=cumulative_emi.sum_total_closing_balance,
            total_closing_balance_post_due_date=cumulative_emi.sum_total_closing_balance_post_due_date,
            due_amount=cumulative_emi.sum_due_amount,
            interest=cumulative_emi.sum_interest,
            late_fee=cumulative_emi.sum_late_fee,
            atm_fee=cumulative_emi.sum_atm_fee,
            interest_current_month=cumulative_emi.sum_interest_current_month,
            interest_next_month=cumulative_emi.sum_interest_next_month,
            total_due_amount=cumulative_emi.sum_total_due_amount,
            due_date=cumulative_emi.due_date,
            extra_details=cumulative_emi.total_extra_details,
        )
        session.add(new_emi)
        emi_number += 1

    # Slide all payments
    slide_payments(user_loan=user_loan)
    session.flush()


def entry_checks(
    ledger_trigger_event,
    ledger_entry,
    debit_account,
    credit_account,
    event_id,
    event_type,
    event_amount,
    bills_touched,
) -> bool:
    verdict = False

    # These are various cases to get the exact events affecting specific bills
    if ledger_trigger_event.name == event_type and ledger_trigger_event.id == event_id:
        verdict = True
    else:
        verdict = False

    if event_amount and ledger_entry.amount + event_amount == ledger_trigger_event.amount:
        return False

    # If the specific bill has already been logged, we skip
    if (credit_account.identifier_type == "bill" and credit_account.identifier in bills_touched) and (
        debit_account.identifier_type == "bill" and debit_account.identifier not in bills_touched
    ):
        return False
    elif (
        credit_account.identifier_type == "bill" and credit_account.identifier not in bills_touched
    ) and (debit_account.identifier_type == "bill" and debit_account.identifier in bills_touched):
        return False
    elif (
        credit_account.identifier_type == "bill" and credit_account.identifier not in bills_touched
    ) and (debit_account.identifier_type == "bill" and debit_account.identifier not in bills_touched):
        return False
    elif (
        verdict
        and (credit_account.identifier_type == "bill" and credit_account.identifier in bills_touched)
        and (debit_account.identifier_type == "bill" and debit_account.identifier in bills_touched)
    ):
        verdict = True

    return verdict


def update_event_with_dpd(
    user_loan: BaseLoan, post_date: DateTime = None, event: LedgerTriggerEvent = None
) -> None:
    def actual_event_update(
        session: Session, is_debit: bool, ledger_trigger_event, ledger_entry, account
    ):
        if is_debit:
            debit_amount = ledger_entry.amount
            credit_amount = Decimal(0)
        else:
            debit_amount = Decimal(0)
            credit_amount = ledger_entry.amount
        bills_touched.append(account.identifier)
        bill = user_loan.convert_to_bill_class(
            (
                session.query(LoanData)
                .filter(
                    LoanData.loan_id == user_loan.loan_id,
                    LoanData.id == account.identifier,
                )
                .first()
            )
        )
        event_post_date = ledger_trigger_event.post_date.date()
        # In case of moratorium reset all post dates to start of moratorium
        if LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=event_post_date
        ):
            moratorium = (
                session.query(LoanMoratorium).filter(LoanMoratorium.loan_id == user_loan.loan_id).first()
            )
            event_post_date = moratorium.start_date
        dpd = (event_post_date - bill.bill_due_date).days
        new_event = EventDpd(
            bill_id=account.identifier,
            loan_id=user_loan.loan_id,
            event_id=ledger_trigger_event.id,
            credit=credit_amount,
            debit=debit_amount,
            balance=bill.get_outstanding_amount(ledger_trigger_event.post_date),
            dpd=dpd,
        )
        session.add(new_event)

    session = user_loan.session

    debit_book_account = aliased(BookAccount)
    credit_book_account = aliased(BookAccount)
    if event:
        events_list = (
            session.query(LedgerTriggerEvent, LedgerEntry, debit_book_account, credit_book_account)
            .filter(
                LedgerTriggerEvent.id == LedgerEntry.event_id,
                LedgerEntry.debit_account == debit_book_account.id,
                LedgerEntry.credit_account == credit_book_account.id,
                LedgerTriggerEvent.id == event.id,
                or_(
                    debit_book_account.identifier_type == "bill",
                    credit_book_account.identifier_type == "bill",
                ),
            )
            .order_by(LedgerTriggerEvent.post_date.asc())
            .all()
        )
    else:
        events_list = (
            session.query(LedgerTriggerEvent, LedgerEntry, debit_book_account, credit_book_account)
            .filter(
                LedgerTriggerEvent.id == LedgerEntry.event_id,
                LedgerEntry.debit_account == debit_book_account.id,
                LedgerEntry.credit_account == credit_book_account.id,
                LedgerTriggerEvent.post_date <= post_date,
                or_(
                    debit_book_account.identifier_type == "bill",
                    credit_book_account.identifier_type == "bill",
                ),
            )
            .order_by(LedgerTriggerEvent.post_date.asc())
            .all()
        )

    event_id = event_type = event_amount = None
    bills_touched = []
    for ledger_trigger_event, ledger_entry, debit_account, credit_account in events_list:
        if entry_checks(
            ledger_trigger_event,
            ledger_entry,
            debit_account,
            credit_account,
            event_id,
            event_type,
            event_amount,
            bills_touched,
        ):
            continue
        bills_touched = []
        if ledger_trigger_event.name in [
            "accrue_interest",
            "accrue_late_fine",
            "card_transaction",
            "daily_dpd",
            "atm_fee_added",
        ]:
            if debit_account.identifier_type == "bill" and debit_account.identifier not in bills_touched:
                event_id = ledger_trigger_event.id
                event_type = ledger_trigger_event.name
                event_amount = ledger_entry.amount
                actual_event_update(session, False, ledger_trigger_event, ledger_entry, debit_account)

            if (
                credit_account.identifier_type == "bill"
                and credit_account.identifier not in bills_touched
            ):
                event_id = ledger_trigger_event.id
                event_type = ledger_trigger_event.name
                event_amount = ledger_entry.amount
                actual_event_update(session, False, ledger_trigger_event, ledger_entry, credit_account)

        elif ledger_trigger_event.name in [
            "reverse_interest_charges",
            "reverse_late_charges",
            "payment_received",
            "transaction_refund",
        ]:
            if debit_account.identifier_type == "bill" and debit_account.identifier not in bills_touched:
                event_id = ledger_trigger_event.id
                event_type = ledger_trigger_event.name
                event_amount = ledger_entry.amount
                actual_event_update(session, True, ledger_trigger_event, ledger_entry, debit_account)

            if (
                credit_account.identifier_type == "bill"
                and credit_account.identifier not in bills_touched
            ):
                event_id = ledger_trigger_event.id
                event_type = ledger_trigger_event.name
                event_amount = ledger_entry.amount
                actual_event_update(session, True, ledger_trigger_event, ledger_entry, credit_account)

    # Adjust dpd in schedule
    # TODO Introduce schedule level updation when this converts to a DAG system
    # all_emis = (
    #     session.query(CardEmis)
    #     .filter(CardEmis.loan_id == user_card.loan_id, CardEmis.row_status == "active")
    #     .order_by(CardEmis.emi_number.asc())
    #     .all()
    # )
    # for emi in all_emis:
    #     if emi.payment_status != "Paid":
    #         emi.dpd = (post_date.date() - emi.due_date).days

    max_dpd = session.query(func.max(EventDpd.dpd).label("max_dpd")).one()
    user_loan.dpd = max_dpd.max_dpd
    if not user_loan.ever_dpd or max_dpd.max_dpd > user_loan.ever_dpd:
        user_loan.ever_dpd = max_dpd.max_dpd

    session.flush()
