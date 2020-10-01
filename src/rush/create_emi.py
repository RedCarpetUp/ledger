from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from datetime import datetime
from pendulum import DateTime
from sqlalchemy import or_, and_
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


def slide_payments(
    user_loan: BaseLoan,
    payment_event: Optional[LedgerTriggerEvent] = None,
) -> None:
    def slide_payments_repeated_logic(
        all_emis,
        payment_received_and_adjusted,
        payment_request_id,
        last_payment_date,
        last_paid_emi_number,
    ) -> None:
        total_payment_till_now = payment_received_and_adjusted
        for emi in all_emis:
            total_payment_till_now += (
                emi.payment_received
                + emi.interest_received
                + emi.atm_fee_received
                + emi.late_fee_received
            )
            if (
                emi.emi_number <= last_paid_emi_number
                or emi.extra_details.get("moratorium")
                or emi.payment_status == "Paid"
            ):
                continue

            if payment_received_and_adjusted:
                if last_payment_date:
                    emi.last_payment_date = last_payment_date
                diff = emi.total_due_amount - (
                    payment_received_and_adjusted
                    + emi.payment_received
                    + emi.atm_fee_received
                    + emi.late_fee_received
                    + emi.interest_received
                )
                # Because rounding of balances has happened previously we should round the diff ~ Ananth
                if -1 < diff < 1:
                    diff = 0
                interest_actually_received = (
                    late_fee_actually_received
                ) = atm_fee_actually_received = principal_actually_received = Decimal(0)
                if diff >= 0:
                    emi.dpd = (last_payment_date.date() - emi.due_date).days
                    if diff == 0:
                        last_paid_emi_number = emi.emi_number
                        emi.payment_status = "Paid"
                    if (
                        emi.atm_fee > 0
                        and (emi.atm_fee_received + payment_received_and_adjusted) <= emi.atm_fee
                    ):
                        emi.atm_fee_received += payment_received_and_adjusted
                        # Maybe will require this later
                        # emi.total_closing_balance -= payment_received_and_adjusted
                        # emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                        # Create payment mapping
                        create_emi_payment_mapping(
                            session=session,
                            user_loan=user_loan,
                            emi_number=emi.emi_number,
                            payment_date=last_payment_date,
                            payment_request_id=payment_request_id,
                            interest_received=Decimal(0),
                            late_fee_received=Decimal(0),
                            atm_fee_received=payment_received_and_adjusted,
                            principal_received=Decimal(0),
                        )
                        break
                    else:
                        if 0 < emi.atm_fee < (emi.atm_fee_received + payment_received_and_adjusted):
                            atm_fee_actually_received = emi.atm_fee - emi.atm_fee_received
                            emi.atm_fee_received = emi.atm_fee
                            # Maybe will require this later
                            # emi.total_closing_balance -= atm_fee_actually_received
                            # emi.total_closing_balance_post_due_date -= atm_fee_actually_received
                            payment_received_and_adjusted -= (
                                atm_fee_actually_received
                                if atm_fee_actually_received > 0
                                else emi.atm_fee_received
                            )
                        if (
                            emi.late_fee > 0
                            and (emi.late_fee_received + payment_received_and_adjusted) <= emi.late_fee
                        ):
                            emi.late_fee_received += payment_received_and_adjusted
                            # Maybe will require this later
                            # emi.total_closing_balance -= payment_received_and_adjusted
                            # emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                            # Create payment mapping
                            create_emi_payment_mapping(
                                session=session,
                                user_loan=user_loan,
                                emi_number=emi.emi_number,
                                payment_date=last_payment_date,
                                payment_request_id=payment_request_id,
                                interest_received=Decimal(0),
                                late_fee_received=payment_received_and_adjusted,
                                atm_fee_received=atm_fee_actually_received,
                                principal_received=Decimal(0),
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
                                # Maybe will require this later
                                # emi.total_closing_balance -= late_fee_actually_received
                                # emi.total_closing_balance_post_due_date -= late_fee_actually_received
                                payment_received_and_adjusted -= (
                                    late_fee_actually_received
                                    if late_fee_actually_received > 0
                                    else emi.late_fee_received
                                )
                            if (
                                emi.interest > 0
                                and (emi.interest_received + payment_received_and_adjusted)
                                <= emi.interest
                            ):
                                emi.interest_received += payment_received_and_adjusted
                                # Maybe will require this later
                                # emi.total_closing_balance -= payment_received_and_adjusted
                                # emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                                # Create payment mapping
                                create_emi_payment_mapping(
                                    session=session,
                                    user_loan=user_loan,
                                    emi_number=emi.emi_number,
                                    payment_date=last_payment_date,
                                    payment_request_id=payment_request_id,
                                    interest_received=payment_received_and_adjusted,
                                    late_fee_received=late_fee_actually_received,
                                    atm_fee_received=atm_fee_actually_received,
                                    principal_received=Decimal(0),
                                )
                                break
                            else:
                                if (
                                    0
                                    < emi.interest
                                    < (emi.interest_received + payment_received_and_adjusted)
                                ):
                                    interest_actually_received = emi.interest - emi.interest_received
                                    emi.interest_received = emi.interest
                                    # Maybe will require this later
                                    # emi.total_closing_balance -= interest_actually_received
                                    # emi.total_closing_balance_post_due_date -= interest_actually_received
                                    payment_received_and_adjusted -= (
                                        interest_actually_received
                                        if interest_actually_received > 0
                                        else emi.interest_received
                                    )
                                if (payment_received_and_adjusted <= emi.due_amount) or (
                                    -1 < (payment_received_and_adjusted - emi.due_amount) < 1
                                ):
                                    principal_actually_received = (
                                        payment_received_and_adjusted - emi.payment_received
                                    )
                                    emi.payment_received = payment_received_and_adjusted
                                    # Maybe will require this later
                                    # emi.total_closing_balance -= payment_received_and_adjusted
                                    # emi.total_closing_balance_post_due_date -= (
                                    #     payment_received_and_adjusted
                                    # )
                                    # Create payment mapping
                                    create_emi_payment_mapping(
                                        session=session,
                                        user_loan=user_loan,
                                        emi_number=emi.emi_number,
                                        payment_date=last_payment_date,
                                        payment_request_id=payment_request_id,
                                        interest_received=interest_actually_received,
                                        late_fee_received=late_fee_actually_received,
                                        atm_fee_received=atm_fee_actually_received,
                                        principal_received=principal_actually_received,
                                    )
                                    break

                principal_actually_received = emi.due_amount - emi.payment_received
                emi.payment_received = emi.due_amount

                # In case the internal loops are missed mapping has to be created correctly
                if atm_fee_actually_received == Decimal(0) and emi.atm_fee > Decimal(0):
                    atm_fee_actually_received = emi.atm_fee - emi.atm_fee_received
                if late_fee_actually_received == Decimal(0) and emi.late_fee > Decimal(0):
                    late_fee_actually_received = emi.late_fee - emi.late_fee_received
                if interest_actually_received == Decimal(0) and emi.interest > Decimal(0):
                    interest_actually_received = emi.interest - emi.interest_received

                # At this point we can assume all amounts were received
                emi.late_fee_received = emi.late_fee
                emi.interest_received = emi.interest
                emi.atm_fee_received = emi.atm_fee

                emi.payment_status = "Paid"
                last_paid_emi_number = emi.emi_number
                # Create payment mapping
                create_emi_payment_mapping(
                    session=session,
                    user_loan=user_loan,
                    emi_number=emi.emi_number,
                    payment_date=last_payment_date,
                    payment_request_id=payment_request_id,
                    interest_received=interest_actually_received,
                    late_fee_received=late_fee_actually_received,
                    atm_fee_received=atm_fee_actually_received,
                    principal_received=principal_actually_received,
                )
                payment_received_and_adjusted = abs(diff)

            else:
                # If no payment is left to adjust, it is safe to break
                break

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
    events = get_payment_events(session=session, user_loan=user_loan)
    if not payment_event:
        for event in events:
            payment_received_and_adjusted = Decimal(0)
            payment_request_id = event.extra_details.get("payment_request_id")
            last_payment_date = event.post_date
            # Mark all mappings inactive
            all_payment_mappings = (
                session.query(EmiPaymentMapping)
                .filter(
                    EmiPaymentMapping.loan_id == user_loan.loan_id,
                    EmiPaymentMapping.payment_request_id == payment_request_id,
                    EmiPaymentMapping.row_status == "active",
                )
                .all()
            )
            # The exception of checking length is for cases in which
            # the users closes his entire loan with the very first payment
            if not all_payment_mappings and (
                user_loan.get_total_outstanding() == 0 and len(events) == 1
            ):
                payment_received_and_adjusted += event.amount
            elif not all_payment_mappings:
                continue
            else:
                for mapping in all_payment_mappings:
                    # We have to mark is inactive because new mapping can be something else altogether
                    mapping.row_status = "inactive"
                    payment_received_and_adjusted += (
                        mapping.principal_received
                        + mapping.interest_received
                        + mapping.late_fee_received
                        + mapping.atm_fee_received
                    )
            assert payment_received_and_adjusted <= event.amount

            slide_payments_repeated_logic(
                all_emis=all_emis,
                payment_received_and_adjusted=payment_received_and_adjusted,
                payment_request_id=payment_request_id,
                last_payment_date=last_payment_date,
                last_paid_emi_number=last_paid_emi_number,
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
        )
    session.flush()


def adjust_late_fee_in_emis(session: Session, user_loan: BaseLoan, bill: LoanData) -> None:
    emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.bill_id == bill.id,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    late_fee = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "late_fee")
        .one_or_none()
    )
    for emi in emis:
        if late_fee and late_fee.gross_amount > 0 and emi.emi_number == 1:
            emi.total_due_amount += late_fee.gross_amount
            emi.late_fee += late_fee.gross_amount
        if emi.emi_number != 1:
            emi.total_closing_balance += late_fee.gross_amount
        emi.total_closing_balance_post_due_date += late_fee.gross_amount

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)


def adjust_atm_fee_in_emis(session: Session, user_loan: BaseLoan, bill: LoanData) -> None:
    emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.bill_id == bill.id,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    atm_fee = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "atm_fee")
        .one_or_none()
    )
    for emi in emis:
        if atm_fee and atm_fee.gross_amount > 0 and emi.emi_number == 1:
            emi.total_due_amount += atm_fee.gross_amount
            emi.atm_fee += atm_fee.gross_amount
        emi.total_closing_balance_post_due_date += atm_fee.gross_amount
        emi.total_closing_balance += atm_fee.gross_amount

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


def update_event_with_dpd(
    user_loan: BaseLoan,
    to_date: DateTime = None,
    from_date: DateTime = None,
    event: LedgerTriggerEvent = None,
) -> None:
    def actual_event_update(
        session: Session,
        is_debit: bool,
        ledger_trigger_event,
        ledger_entry,
        account,
    ):
        if not is_debit:
            debit_amount = Decimal(0)
            credit_amount = ledger_entry.amount
            account_id = account.identifier
            account_balance = ledger_entry.debit_account_balance
        else:
            debit_amount = ledger_entry.amount
            credit_amount = Decimal(0)
            account_id = account.identifier
            account_balance = ledger_entry.credit_account_balance

        if isinstance(ledger_trigger_event.post_date, datetime):
            event_post_date = ledger_trigger_event.post_date.date()
        else:
            event_post_date = ledger_trigger_event.post_date
        # In case of moratorium reset all post dates to start of moratorium
        if LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=event_post_date
        ):
            moratorium = (
                session.query(LoanMoratorium).filter(LoanMoratorium.loan_id == user_loan.loan_id).first()
            )
            event_post_date = moratorium.start_date

        # We need to get the bill because we have to check if min is paid
        bill = user_loan.convert_to_bill_class(
            (
                session.query(LoanData)
                .filter(
                    LoanData.loan_id == user_loan.loan_id,
                    LoanData.id == account_id,
                )
                .first()
            )
        )

        # Adjust dpd in loan schedule
        first_unpaid_mark = False
        bill_dpd = 0
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
        for emi in all_emis:
            if emi.payment_status != "Paid":
                if not first_unpaid_mark:
                    first_unpaid_mark = True
                    # Bill dpd
                    # Only calculate bill dpd is min is not 0
                    if bill and bill.get_remaining_min() > 0:
                        bill_dpd = (event_post_date - emi.due_date).days
                # Schedule dpd
                schedule_dpd = (event_post_date - emi.due_date).days
                # We should only consider the daily dpd event for increment
                if schedule_dpd >= emi.dpd:
                    emi.dpd = schedule_dpd

        new_event = EventDpd(
            bill_id=account_id,
            loan_id=user_loan.loan_id,
            event_id=ledger_trigger_event.id,
            credit=credit_amount,
            debit=debit_amount,
            balance=account_balance,
            dpd=bill_dpd,
        )
        session.add(new_event)

    session = user_loan.session

    debit_book_account = aliased(BookAccount)
    credit_book_account = aliased(BookAccount)
    events_list = session.query(
        LedgerTriggerEvent, LedgerEntry, debit_book_account, credit_book_account
    ).filter(
        LedgerEntry.event_id == LedgerTriggerEvent.id,
        LedgerEntry.debit_account == debit_book_account.id,
        LedgerEntry.credit_account == credit_book_account.id,
    )
    if from_date and to_date:
        events_list = events_list.filter(
            LedgerTriggerEvent.post_date > from_date,
            LedgerTriggerEvent.post_date <= to_date,
        )
    elif to_date:
        events_list = events_list.filter(
            LedgerTriggerEvent.post_date <= to_date,
        )

    if event:
        events_list = events_list.filter(LedgerTriggerEvent.id == event.id)
    events_list = events_list.order_by(LedgerTriggerEvent.post_date.asc()).all()

    for ledger_trigger_event, ledger_entry, debit_account, credit_account in events_list:
        if (
            ledger_trigger_event.name
            in [
                "accrue_interest",
                "charge_late_fine",
                "atm_fee_added",
            ]
            and debit_account.identifier_type == "bill"
            and debit_account.book_name == "max"
        ):
            actual_event_update(session, False, ledger_trigger_event, ledger_entry, debit_account)

        elif (
            ledger_trigger_event.name
            in [
                "card_transaction",
            ]
            and debit_account.identifier_type == "bill"
            and debit_account.book_name == "unbilled"
        ):
            actual_event_update(session, False, ledger_trigger_event, ledger_entry, debit_account)

        elif (
            ledger_trigger_event.name
            in [
                "reverse_interest_charges",
                "reverse_late_charges",
                "payment_received",
                "transaction_refund",
            ]
            and credit_account.identifier_type == "bill"
            and credit_account.book_name == "max"
        ):
            actual_event_update(session, True, ledger_trigger_event, ledger_entry, credit_account)

        else:
            continue

    # Calculate card level dpd
    max_dpd = session.query(func.max(EventDpd.dpd).label("max_dpd")).one()
    user_loan.dpd = max_dpd.max_dpd
    if not user_loan.ever_dpd or max_dpd.max_dpd > user_loan.ever_dpd:
        user_loan.ever_dpd = max_dpd.max_dpd

    session.flush()


def daily_dpd_update(session, user_loan, post_date):
    first_unpaid_mark = False
    loan_level_due_date = None
    event = LedgerTriggerEvent(name="daily_dpd_update", loan_id=user_loan.loan_id, post_date=post_date)
    session.add(event)
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
    for emi in all_emis:
        if emi.payment_status != "Paid":
            if not first_unpaid_mark:
                first_unpaid_mark = True
                loan_level_due_date = emi.due_date
            # Schedule dpd
            dpd = (post_date.date() - emi.due_date).days
            # We should only consider the daily dpd event for increment
            if dpd >= emi.dpd:
                emi.dpd = dpd

    unpaid_bills = user_loan.get_unpaid_bills()
    for bill in unpaid_bills:
        if first_unpaid_mark and loan_level_due_date:
            bill_dpd = (post_date.date() - loan_level_due_date).days
            new_event = EventDpd(
                bill_id=bill.id,
                loan_id=user_loan.loan_id,
                event_id=event.id,
                credit=Decimal(0),
                debit=Decimal(0),
                balance=bill.get_remaining_max(),
                dpd=bill_dpd,
            )
            session.add(new_event)

    # Calculate card level dpd
    max_dpd = session.query(func.max(EventDpd.dpd).label("max_dpd")).one()
    user_loan.dpd = max_dpd.max_dpd
    if not user_loan.ever_dpd or max_dpd.max_dpd > user_loan.ever_dpd:
        user_loan.ever_dpd = max_dpd.max_dpd
    session.flush()
