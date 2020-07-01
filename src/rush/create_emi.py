from datetime import timedelta
from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.anomaly_detection import get_affected_events
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardEmis,
    LoanData,
    UserCard,
)
from rush.utils import (
    div,
    get_current_ist_time,
    mul,
)


def create_emis_for_card(session: Session, user_card: UserCard, last_bill: LoanData) -> CardEmis:
    first_emi_due_date = user_card.card_activation_date + timedelta(
        days=user_card.interest_free_period_in_days + 1
    )
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/principal_receivable/a"
    )
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/late_fine_receivable/a"
    )
    due_amount = div(principal_due, 12)
    # We will firstly create only 12 emis
    for i in range(1, 13):
        due_date = (
            first_emi_due_date
            if i == 1
            else due_date + timedelta(days=user_card.statement_period_in_days + 1)
        )
        late_fee = late_fine_due if i == 1 else 0
        new_emi = CardEmis(
            card_id=user_card.id,
            emi_number=i,
            total_closing_balance=(principal_due - mul(due_amount, (i - 1))),
            total_closing_balance_post_due_date=(principal_due - mul(due_amount, (i - 1))),
            due_amount=due_amount,
            total_due_amount=due_amount,
            due_date=due_date,
            late_fee=late_fine_due,
        )
        session.add(new_emi)
    session.flush()
    return new_emi


def add_emi_on_new_bill(
    session: Session, user_card: UserCard, last_bill: LoanData, last_emi_number: int
) -> CardEmis:
    new_end_emi_number = last_emi_number + 1
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/principal_receivable/a"
    )
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/late_fine_receivable/a"
    )
    due_amount = div(principal_due, 12)
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id)
        .order_by(CardEmis.due_date.asc())
    )
    new_emi_list = []
    for emi in all_emis:
        emi_dict = emi.as_dict()
        # We consider 12 because the first insertion had 12 emis
        if emi_dict["emi_number"] <= new_end_emi_number - 12:
            new_emi_list.append(emi_dict)
            continue
        elif emi_dict["emi_number"] == ((new_end_emi_number - 12) + 1):
            emi_dict["late_fee"] += late_fine_due
        emi_dict["due_amount"] += due_amount
        emi_dict["total_due_amount"] += due_amount
        emi_dict["total_closing_balance"] += principal_due - (
            mul(due_amount, (emi_dict["emi_number"] - (new_end_emi_number - 12) - 1))
        )
        emi_dict["total_closing_balance_post_due_date"] += principal_due - (
            mul(due_amount, (emi_dict["emi_number"] - (new_end_emi_number - 12) - 1))
        )
        new_emi_list.append(emi_dict)
    session.bulk_update_mappings(CardEmis, new_emi_list)
    # Get the second last emi for calculating values of the last emi
    second_last_emi = all_emis[last_emi_number - 1]
    last_emi_due_date = second_last_emi.due_date + timedelta(days=user_card.statement_period_in_days + 1)
    late_fee = 0
    new_emi = CardEmis(
        card_id=user_card.id,
        emi_number=new_end_emi_number,
        due_amount=due_amount,
        total_due_amount=due_amount,
        total_closing_balance=(principal_due - mul(due_amount, (new_end_emi_number - 1))),
        total_closing_balance_post_due_date=(principal_due - mul(due_amount, (new_end_emi_number - 1))),
        due_date=last_emi_due_date,
        late_fee=late_fee,
    )
    session.add(new_emi)
    session.flush()
    return new_emi


def refresh_schedule(session: Session, user_id: int) -> None:
    all_bills = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.asc())
        .all()
    )
    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).first()
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id)
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in all_emis_query.all()]
    # To run test, remove later
    # first_emi = emis_dict[0]
    # return first_emi
    payment_received_and_adjusted = Decimal(0)
    last_paid_emi_number = 0
    last_payment_date = None
    all_paid = False
    for bill in all_bills:
        events = get_affected_events(session, user_card)
        for event in events:
            if event.name == "payment_received":
                payment_received_and_adjusted += event.amount
                last_payment_date = event.post_date
        for emi in emis_dict:
            if emi["emi_number"] <= last_paid_emi_number:
                continue
            if last_payment_date:
                emi["last_payment_date"] = last_payment_date
            if all_paid:
                emi["payment_received"] = emi["late_fee_received"] = emi["interest_received"] = emi[
                    "due_amount"
                ] = emi["total_due_amount"] = emi["total_closing_balance"] = emi[
                    "total_closing_balance_post_due_date"
                ] = emi[
                    "interest_current_month"
                ] = emi[
                    "interest_next_month"
                ] = emi[
                    "interest"
                ] = Decimal(
                    0
                )
                emi["payment_status"] = "Paid"
                last_paid_emi_number = emi["emi_number"]
                continue
            if payment_received_and_adjusted:
                current_date = get_current_ist_time().date()
                actual_closing_balance = emi["total_closing_balance_post_due_date"]
                if current_date <= emi["due_date"]:
                    actual_closing_balance = emi["total_closing_balance"]
                if payment_received_and_adjusted >= actual_closing_balance:
                    all_paid = True
                    emi["late_fee_received"] = emi["late_fee"]
                    emi["interest_received"] = emi["interest"]
                    emi["payment_received"] = payment_received_and_adjusted - (
                        emi["late_fee"] + emi["interest"]
                    )
                    emi["total_closing_balance"] = 0
                    emi["total_closing_balance_post_due_date"] = 0
                    last_paid_emi_number = emi["emi_number"]
                    emi["payment_status"] = "Paid"
                    continue
                diff = emi["total_due_amount"] - payment_received_and_adjusted
                # -99 dpd if you can't figure out
                emi["dpd"] = -99 if diff == 0 else (current_date - emi["due_date"]).days
                if diff >= 0:
                    if diff == 0:
                        last_paid_emi_number = emi["emi_number"]
                        emi["payment_status"] = "Paid"
                    if payment_received_and_adjusted <= emi["late_fee"]:
                        emi["late_fee_received"] = payment_received_and_adjusted
                        emi["total_closing_balance"] -= payment_received_and_adjusted
                        emi["total_closing_balance_post_due_date"] -= payment_received_and_adjusted
                        break
                    else:
                        emi["late_fee_received"] = emi["late_fee"]
                        payment_received_and_adjusted -= emi["late_fee"]
                        if payment_received_and_adjusted <= emi["interest"]:
                            emi["interest_received"] = payment_received_and_adjusted
                            emi["total_closing_balance"] -= payment_received_and_adjusted
                            emi["total_closing_balance_post_due_date"] -= payment_received_and_adjusted
                            break
                        else:
                            emi["interest_received"] = emi["interest"]
                            payment_received_and_adjusted -= emi["interest"]
                            if payment_received_and_adjusted <= emi["due_amount"]:
                                emi["payment_received"] = payment_received_and_adjusted
                                emi["total_closing_balance"] -= payment_received_and_adjusted
                                emi[
                                    "total_closing_balance_post_due_date"
                                ] -= payment_received_and_adjusted
                                break
                emi["late_fee_received"] = emi["late_fee"]
                emi["interest_received"] = emi["interest"]
                emi["payment_received"] = emi["due_amount"]
                emi["payment_status"] = "Paid"
                last_paid_emi_number = emi["emi_number"]
                payment_received_and_adjusted = abs(diff)
    session.bulk_update_mappings(CardEmis, emis_dict)


def adjust_interest_in_emis(session: Session, user_id: int, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id, LoanData.agreement_date <= post_date)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).first()
    emis_for_this_bill = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.due_date >= post_date)
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in emis_for_this_bill.all()]
    _, interest_due = get_account_balance_from_str(
        session=session, book_string=f"{latest_bill.id}/bill/interest_receivable/a"
    )
    if interest_due > 0:
        for emi in emis_dict:
            emi["total_closing_balance_post_due_date"] += interest_due
            emi["total_due_amount"] += interest_due
            emi["interest_current_month"] += div(mul(interest_due, (30 - emi["due_date"].day)), 30)
            emi["interest_next_month"] += interest_due - emi["interest_current_month"]
            emi["interest"] += emi["interest_current_month"] + emi["interest_next_month"]
        session.bulk_update_mappings(CardEmis, emis_dict)


def adjust_late_fee_in_emis(session: Session, user_id: int, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id, LoanData.agreement_date < post_date)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).first()
    emi = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.due_date < post_date)
        .order_by(CardEmis.due_date.desc())
        .first()
    )
    if not emi:
        emi = session.query(CardEmis).order_by(CardEmis.due_date.asc()).first()
    emi_dict = emi.as_dict()
    _, late_fee = get_account_balance_from_str(
        session=session, book_string=f"{latest_bill.id}/bill/late_fine_receivable/a"
    )
    if late_fee > 0:
        emi_dict["total_closing_balance_post_due_date"] += late_fee
        emi_dict["total_due_amount"] += late_fee
        emi_dict["late_fee"] += late_fee
        session.bulk_update_mappings(CardEmis, [emi_dict])
