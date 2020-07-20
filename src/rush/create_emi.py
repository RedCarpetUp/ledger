from datetime import timedelta
from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import (
    Date,
    DateTime,
)
from pendulum import parse as parse_date
from sqlalchemy.orm import (
    Session,
    session,
)

from rush.anomaly_detection import get_payment_events
from rush.card import get_user_card
from rush.card.base_card import BaseBill
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardEmis,
    EmiPaymentMapping,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
    UserCard,
)
from rush.utils import (
    EMI_FORMULA_DICT,
    div,
    get_current_ist_time,
    mul,
)


def create_emis_for_card(
    session: Session,
    user_card: UserCard,
    last_bill: BaseBill,
    late_fee: Decimal = None,
    interest: Decimal = None,
    bill_tenure: Decimal = 12,
) -> None:
    first_emi_due_date = user_card.card_activation_date + timedelta(
        days=user_card.interest_free_period_in_days + 1
    )
    principal_due = Decimal(last_bill.table.principal)
    due_amount = last_bill.table.principal_instalment
    due_date = new_emi = None
    late_fine = total_interest = current_interest = next_interest = Decimal(0)
    for i in range(1, bill_tenure + 1):
        due_date = (
            first_emi_due_date
            if i == 1
            else due_date + timedelta(days=user_card.statement_period_in_days + 1)
        )
        late_fine = late_fee if late_fee and i == 1 else Decimal(0)
        total_due_amount = due_amount
        total_closing_balance = (
            principal_due - mul(due_amount, (i - 1))
            if principal_due - mul(due_amount, (i - 1)) > 0
            else due_amount
        )
        total_closing_balance_post_due_date = (
            principal_due - mul(due_amount, (i - 1))
            if principal_due - mul(due_amount, (i - 1)) > 0
            else due_amount
        )
        if interest:
            current_interest = div(mul(interest, (30 - due_date.day)), 30)
            next_interest = interest - current_interest
            total_interest = current_interest + next_interest
            total_due_amount += interest
            total_closing_balance_post_due_date += interest
        new_emi = CardEmis(
            card_id=user_card.id,
            emi_number=i,
            total_closing_balance=total_closing_balance,
            total_closing_balance_post_due_date=total_closing_balance_post_due_date,
            due_amount=due_amount,
            late_fee=late_fine,
            interest=total_interest,
            interest_current_month=current_interest,
            interest_next_month=next_interest,
            total_due_amount=total_due_amount,
            due_date=due_date,
        )
        session.add(new_emi)
    session.flush()


def add_emi_on_new_bill(
    session: Session,
    user_card: UserCard,
    last_bill: BaseBill,
    last_emi_number: int,
    bill_number: int,
    late_fee: Decimal = None,
    interest: Decimal = None,
    bill_tenure: Decimal = 12,
) -> None:
    if bill_tenure < last_emi_number:
        emis_to_be_inserted = 0
    else:
        emis_to_be_inserted = (bill_tenure - last_emi_number) + 1
    principal_due = Decimal(last_bill.table.principal)
    due_amount = last_bill.table.principal_instalment
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    user_card_wrapped = get_user_card(session, user_card.user_id)
    total_interest = current_interest = next_interest = Decimal(0)
    min_due = user_card_wrapped.get_min_for_schedule()
    for emi in all_emis:
        # We consider 12 because the first insertion had 12 emis
        if emi.emi_number < bill_number:
            continue
        elif late_fee and emi.emi_number == bill_number:
            emi.late_fee += late_fee
        emi.due_amount += due_amount
        emi.total_due_amount = (
            min_due if emi.emi_number == bill_number else emi.total_due_amount + due_amount
        )
        emi.total_closing_balance += principal_due - (mul(due_amount, (emi.emi_number - bill_number)))
        emi.total_closing_balance_post_due_date += principal_due - (
            mul(due_amount, (emi.emi_number - bill_number))
        )
        emi.payment_status = "UnPaid"
        if interest:
            emi.total_closing_balance_post_due_date += interest
            emi.total_due_amount = (
                emi.total_due_amount + interest if emi.total_due_amount != min_due else min_due
            )
            emi.interest_current_month += div(mul(interest, (30 - emi.due_date.day)), 30)
            emi.interest_next_month = (interest + emi.interest) - emi.interest_current_month
            emi.interest = emi.interest_current_month + emi.interest_next_month
    session.flush()
    if emis_to_be_inserted > 0:
        for i in range(0, emis_to_be_inserted):
            # Get the second last emi for calculating values of the last emi
            last_emi_due_date = None
            second_last_emi = all_emis[-1]
            if not last_emi_due_date:
                last_emi_due_date = second_last_emi.due_date + timedelta(
                    days=user_card.statement_period_in_days + 1
                )
            else:
                last_emi_due_date += timedelta(days=user_card.statement_period_in_days + 1)
            total_due_amount = due_amount
            total_closing_balance = (
                (principal_due - mul(due_amount, (last_emi_number + i)))
                if (principal_due - mul(due_amount, (last_emi_number + i))) > 0
                else due_amount
            )
            total_closing_balance_post_due_date = (
                (principal_due - mul(due_amount, (last_emi_number + i)))
                if (principal_due - mul(due_amount, (last_emi_number + i))) > 0
                else due_amount
            )
            if interest:
                current_interest += div(mul(interest, (30 - last_emi_due_date.day)), 30)
                next_interest += interest - current_interest
                total_interest = current_interest + next_interest
                total_due_amount += interest
                total_closing_balance_post_due_date += interest
            new_emi = CardEmis(
                card_id=user_card.id,
                emi_number=(last_emi_number + i + 1),
                due_amount=due_amount,
                interest=total_interest,
                interest_current_month=current_interest,
                interest_next_month=next_interest,
                total_due_amount=total_due_amount,
                total_closing_balance=total_closing_balance,
                total_closing_balance_post_due_date=total_closing_balance_post_due_date,
                due_date=last_emi_due_date,
            )
            session.add(new_emi)
    session.flush()


def slide_payments(session: Session, user_id: int, payment_event: LedgerTriggerEvent = None) -> None:
    def slide_payments_repeated_logic(
        all_emis,
        payment_received_and_adjusted,
        payment_request_id,
        last_payment_date,
        last_paid_emi_number,
        all_paid=False,
    ) -> None:
        for emi in all_emis:
            if emi.emi_number <= last_paid_emi_number or emi.total_due_amount <= Decimal(0):
                continue
            if last_payment_date:
                emi.last_payment_date = last_payment_date
            if all_paid:
                emi.payment_received = (
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
                last_paid_emi_number = emi.emi_number
                continue
            if payment_received_and_adjusted:
                # Why did I get current date here? For dpd? Can't remember.
                current_date = get_current_ist_time().date()
                actual_closing_balance = emi.total_closing_balance_post_due_date
                if last_payment_date.date() <= emi.due_date:
                    actual_closing_balance = emi.total_closing_balance
                if (
                    payment_received_and_adjusted >= actual_closing_balance
                    and actual_closing_balance > 0
                    and (
                        last_payment_date.date() <= emi.due_date
                        and last_payment_date.date() > (emi.due_date + relativedelta(months=-1))
                    )
                ):
                    all_paid = True
                    emi.late_fee_received = emi.late_fee
                    emi.payment_received = actual_closing_balance - emi.late_fee
                    emi.due_amount = emi.total_due_amount = actual_closing_balance
                    emi.total_closing_balance = (
                        emi.total_closing_balance_post_due_date
                    ) = emi.interest = emi.interest_current_month = emi.interest_next_month = 0
                    last_paid_emi_number = emi.emi_number
                    emi.payment_status = "Paid"
                    # Create payment mapping
                    create_emi_payment_mapping(
                        session,
                        user_card,
                        emi.emi_number,
                        last_payment_date,
                        payment_request_id,
                        emi.interest_received,
                        emi.late_fee_received,
                        emi.payment_received,
                    )
                    continue
                diff = emi.total_due_amount - payment_received_and_adjusted
                # -99 dpd if you can't figure out
                # I don't need to calculate this anyways here
                emi.dpd = -99 if diff == 0 else (last_payment_date.date() - emi.due_date).days
                if diff >= 0:
                    if diff == 0:
                        last_paid_emi_number = emi.emi_number
                        emi.payment_status = "Paid"
                    if payment_received_and_adjusted <= emi.late_fee:
                        emi.late_fee_received = payment_received_and_adjusted
                        emi.total_closing_balance -= payment_received_and_adjusted
                        emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                        # Create payment mapping
                        create_emi_payment_mapping(
                            session,
                            user_card,
                            emi.emi_number,
                            last_payment_date,
                            payment_request_id,
                            emi.interest_received,
                            emi.late_fee_received,
                            emi.payment_received,
                        )
                        break
                    else:
                        emi.late_fee_received = emi.late_fee
                        payment_received_and_adjusted -= emi.late_fee
                        if payment_received_and_adjusted <= emi.interest:
                            emi.interest_received = payment_received_and_adjusted
                            emi.total_closing_balance -= payment_received_and_adjusted
                            emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                            # Create payment mapping
                            create_emi_payment_mapping(
                                session,
                                user_card,
                                emi.emi_number,
                                last_payment_date,
                                payment_request_id,
                                emi.interest_received,
                                emi.late_fee_received,
                                emi.payment_received,
                            )
                            break
                        else:
                            emi.interest_received = emi.interest
                            payment_received_and_adjusted -= emi.interest
                            if payment_received_and_adjusted <= emi.due_amount:
                                emi.payment_received = payment_received_and_adjusted
                                emi.total_closing_balance -= payment_received_and_adjusted
                                emi.total_closing_balance_post_due_date -= payment_received_and_adjusted
                                # Create payment mapping
                                create_emi_payment_mapping(
                                    session,
                                    user_card,
                                    emi.emi_number,
                                    last_payment_date,
                                    payment_request_id,
                                    emi.interest_received,
                                    emi.late_fee_received,
                                    emi.payment_received,
                                )
                                break
                emi.late_fee_received = emi.late_fee
                emi.interest_received = emi.interest
                emi.payment_received = emi.due_amount
                emi.payment_status = "Paid"
                last_paid_emi_number = emi.emi_number
                # Create payment mapping
                create_emi_payment_mapping(
                    session,
                    user_card,
                    emi.emi_number,
                    last_payment_date,
                    payment_request_id,
                    emi.interest_received,
                    emi.late_fee_received,
                    emi.payment_received,
                )
                payment_received_and_adjusted = abs(diff)

    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).first()
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    if not all_emis:
        # Success and Error handling later
        return
    # To run test, remove later
    # first_emi = emis_dict[0]
    # return first_emi
    payment_request_id = None
    last_paid_emi_number = 0
    last_payment_date = None
    all_paid = False
    events = get_payment_events(session, user_card)
    if not payment_event:
        for event in events:
            payment_received_and_adjusted = Decimal(0)
            payment_received_and_adjusted += event.amount
            payment_request_id = event.extra_details.get("payment_request_id")
            last_payment_date = event.post_date
            slide_payments_repeated_logic(
                all_emis,
                payment_received_and_adjusted,
                payment_request_id,
                last_payment_date,
                last_paid_emi_number,
                all_paid=all_paid,
            )
    else:
        payment_received_and_adjusted = Decimal(0)
        payment_received_and_adjusted += payment_event.amount
        payment_request_id = payment_event.extra_details.get("payment_request_id")
        last_payment_date = payment_event.post_date
        slide_payments_repeated_logic(
            all_emis,
            payment_received_and_adjusted,
            payment_request_id,
            last_payment_date,
            last_paid_emi_number,
            all_paid=all_paid,
        )

    session.flush()


def adjust_interest_in_emis(session: Session, user_id: int, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id, LoanData.bill_start_date <= post_date)
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    user_card = get_user_card(session, user_id)
    emis_for_this_bill = (
        session.query(CardEmis)
        .filter(
            CardEmis.card_id == user_card.id,
            CardEmis.due_date >= post_date,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    if latest_bill.interest_to_charge:
        interest_due = Decimal(latest_bill.interest_to_charge)
        min_due = user_card.get_min_for_schedule()
        emi_count = 0
        if interest_due and interest_due > 0:
            for emi in emis_for_this_bill:
                emi.total_closing_balance_post_due_date += interest_due
                emi.total_due_amount = min_due if emi_count == 0 else emi.total_due_amount + interest_due
                emi.interest_current_month += div(mul(interest_due, (30 - emi.due_date.day)), 30)
                emi.interest_next_month = (interest_due + emi.interest) - emi.interest_current_month
                emi.interest = emi.interest_current_month + emi.interest_next_month
                emi_count += 1
            session.flush()
            # session.bulk_update_mappings(CardEmis, emis_dict)


def adjust_late_fee_in_emis(session: Session, user_id: int, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id, LoanData.bill_start_date < post_date)
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    user_card = get_user_card(session, user_id)
    emi = (
        session.query(CardEmis)
        .filter(
            CardEmis.card_id == user_card.id,
            CardEmis.due_date < post_date,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.due_date.desc())
        .first()
    )
    min_due = user_card.get_min_for_schedule()
    if not emi:
        emi = (
            session.query(CardEmis)
            .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
            .order_by(CardEmis.emi_number.asc())
            .first()
        )
    _, late_fee = get_account_balance_from_str(session, f"{latest_bill.id}/bill/late_fine/r")
    if late_fee and late_fee > 0:
        emi.total_closing_balance_post_due_date += late_fee
        emi.total_due_amount = min_due if min_due else emi.total_due_amount + late_fee
        emi.late_fee += late_fee
        session.flush()
        # session.bulk_update_mappings(CardEmis, [emi_dict])


def create_emi_payment_mapping(
    session: Session,
    user_card: UserCard,
    emi_number: int,
    payment_date: DateTime,
    payment_request_id: str,
    interest_received: Decimal,
    late_fee_received: Decimal,
    principal_received: Decimal,
) -> None:
    new_payment_mapping = EmiPaymentMapping(
        card_id=user_card.id,
        emi_number=emi_number,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
        interest_received=interest_received,
        late_fee_received=late_fee_received,
        principal_received=principal_received,
    )
    session.add(new_payment_mapping)
    session.flush()
    return new_payment_mapping


def add_moratorium_to_loan_emi(
    session: Session, user_card, loan_emis, start_date, months_to_be_inserted: int
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
        for emi in loan_emis:
            # temp_emi = emi.copy()
            if emi.emi_number == emi_number_to_begin_insertion_from:
                for i in range(months_to_be_inserted + 1):
                    # insert_emi = temp_emi.copy()
                    # Need to just update emi related fields because
                    # late fine and interest will be handled through events
                    if i != months_to_be_inserted:
                        new_emi = CardEmis(
                            card_id=user_card.table.id,
                            emi_number=(emi.emi_number + i),
                            total_closing_balance=emi.total_closing_balance,
                            total_closing_balance_post_due_date=emi.total_closing_balance_post_due_date,
                            due_amount=Decimal(0),
                            late_fee=Decimal(0),
                            interest=Decimal(0),
                            interest_current_month=Decimal(0),
                            interest_next_month=Decimal(0),
                            total_due_amount=Decimal(0),
                            due_date=(emi.due_date + relativedelta(months=+i)),
                            extra_details={"moratorium": True},
                            payment_status="Paid",
                        )
                        session.add(new_emi)
                        continue
                    emi.emi_number += i
                    emi.due_date += relativedelta(months=+i)
            elif emi.emi_number > emi_number_to_begin_insertion_from:
                emi.emi_number += months_to_be_inserted
                emi.due_date += relativedelta(months=+months_to_be_inserted)
            if emi.emi_number != emi_number_to_begin_insertion_from:
                continue
    else:
        last_emi = loan_emis[-1]
        for i in range(months_to_be_inserted):
            # Need to just update emi related fields because
            # late fine and interest will be handled through events
            new_emi = CardEmis(
                card_id=user_card.table.id,
                emi_number=(emi_number_to_begin_insertion_from + i + 1),
                total_closing_balance=last_emi.total_closing_balance,
                total_closing_balance_post_due_date=last_emi.total_closing_balance_post_due_date,
                due_amount=last_emi.due_amount,
                late_fee=last_emi.late_fee,
                interest=last_emi.interest,
                interest_current_month=last_emi.interest_current_month,
                interest_next_month=last_emi.interest_next_month,
                total_due_amount=last_emi.total_due_amount,
                due_date=last_emi.due_date + relativedelta(months=+(i + 1)),
                extra_details={"moratorium": True},
                payment_status="Paid",
            )
            session.add(new_emi)
    session.commit()
    return {"result": "success"}


def check_moratorium_eligibility(session: Session, data):
    user_id = int(data["user_id"])
    start_date = parse_date(data["start_date"]).date()
    months_to_be_inserted = int(data["months_to_be_inserted"])
    user_card = get_user_card(session, user_id)
    emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    try:
        moratorium_start_emi = next(emi for emi in emis if emi.due_date >= start_date)
    except:
        moratorium_start_emi = None
    if not moratorium_start_emi:
        return {"result": "error", "message": "Not eligible for moratorium"}

    resp = add_moratorium_to_loan_emi(session, user_card, emis, start_date, months_to_be_inserted)
    if resp["result"] == "error":
        return resp

    # Updation of emis in schedule
    # to_update_emi_list = [i for i in resp["data"] if not (i["extra_details"].get("moratorium"))]
    # session.bulk_update_mappings(CardEmis, to_update_emi_list)
    # session.flush()
    # for emi in resp["data"]:
    #     if emi["extra_details"].get("moratorium"):
    #         new_emi = CardEmis(
    #             card_id=user_card.table.id,
    #             emi_number=emi["emi_number"],
    #             total_closing_balance=emi["total_closing_balance"],
    #             total_closing_balance_post_due_date=emi["total_closing_balance_post_due_date"],
    #             due_amount=emi["due_amount"],
    #             late_fee=emi["late_fee"],
    #             interest=emi["interest"],
    #             interest_current_month=emi["interest_current_month"],
    #             interest_next_month=emi["interest_next_month"],
    #             total_due_amount=emi["total_due_amount"],
    #             due_date=emi["due_date"],
    #             extra_details=emi["extra_details"],
    #         )
    #         session.add(new_emi)
    # session.flush()


def refresh_schedule(session: Session, user_id: int):
    # Get user card
    user_card = get_user_card(session, user_id)

    # Get all generated bills of the user
    all_bills = user_card.get_all_bills()

    # Set all previous emis as inactive
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.table.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    for emi in all_emis:
        emi.row_status = "inactive"
        session.flush()

    # Re-Create schedule from all the bills
    bill_number = 1
    for bill in all_bills:
        _, late_fine_due = get_account_balance_from_str(session, f"{bill.table.id}/bill/late_fine/r")
        interest_due = Decimal(bill.table.interest_to_charge)
        last_emi = (
            session.query(CardEmis)
            .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
            .order_by(CardEmis.due_date.desc())
            .first()
        )
        if not last_emi:
            create_emis_for_card(session, user_card.table, bill, late_fine_due, interest_due)
        else:
            add_emi_on_new_bill(
                session,
                user_card.table,
                bill,
                last_emi.emi_number,
                bill_number,
                late_fine_due,
                interest_due,
            )
        bill_number += 1

    # Check if user has opted for moratorium and adjust that in schedule
    moratorium = (
        session.query(LoanMoratorium).filter(LoanMoratorium.card_id == user_card.table.id).first()
    )
    if moratorium:
        start_date = moratorium.start_date
        months_to_be_inserted = (
            (moratorium.end_date.year - moratorium.start_date.year) * 12
            + moratorium.end_date.month
            - moratorium.start_date.month
        )
        check_moratorium_eligibility(
            session,
            {
                "user_id": user_id,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "months_to_be_inserted": months_to_be_inserted,
            },
        )

    # Slide all payments
    slide_payments(session, user_id)
