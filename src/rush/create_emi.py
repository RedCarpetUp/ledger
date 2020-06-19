from datetime import timedelta
from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session
from rush.utils import get_current_ist_time
from rush.ledger_utils import get_account_balance_from_str
from rush.anomaly_detection import get_affected_events
from rush.models import CardEmis, UserCard, LoanData


def create_emis_for_card(session: Session, user_card: UserCard, last_bill: LoanData) -> CardEmis:
    first_emi_due_date = user_card.card_activation_date + timedelta(
        days=user_card.interest_free_period_in_days + 1
    )
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/principal_due/a"
    )
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/interest_due/a"
    )
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/late_fine_due/a"
    )
    due_amount = Decimal(principal_due / 12)
    # We will firstly create only 12 emis
    for i in range(1, 13):
        due_date = (
            first_emi_due_date
            if i == 1
            else due_date + timedelta(days=user_card.statement_period_in_days + 1)
        )
        late_fee = late_fine_due if i == 1 else 0
        interest_current_month = round(interest_due * (30 - due_date.day) / 30, 2)
        interest_next_month = round(interest_due - interest_current_month, 2)
        new_emi = CardEmis(
            card_id=user_card.id,
            emi_number=i,
            interest_current_month=interest_current_month,
            interest_next_month=interest_next_month,
            due_amount=due_amount,
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
        session, book_string=f"{last_bill.id}/bill/principal_due/a"
    )
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/interest_due/a"
    )
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{last_bill.id}/bill/late_fine_due/a"
    )
    due_amount = Decimal(principal_due / 12)
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
        new_emi_list.append(emi_dict)
    session.bulk_update_mappings(CardEmis, new_emi_list)
    # Get the second last emi for calculating values of the last emi
    second_last_emi = all_emis[last_emi_number - 1]
    last_emi_due_date = second_last_emi.due_date + timedelta(days=user_card.statement_period_in_days + 1)
    late_fee = 0
    interest_current_month = round(interest_due * (30 - last_emi_due_date.day) / 30, 2)
    interest_next_month = round(interest_due - interest_current_month, 2)
    new_emi = CardEmis(
        card_id=user_card.id,
        emi_number=new_end_emi_number,
        interest_current_month=interest_current_month,
        interest_next_month=interest_next_month,
        due_amount=due_amount,
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
    for bill in all_bills:
        events = get_affected_events(session, bill.id)
        for event in events:
            if event.name == "bill_close":
                payment_received_and_adjusted += event.amount
                last_payment_date = event.post_date
        total_bill_principal = bill.total_principal  # To be received later from Raghavs method
        for emi in emis_dict:
            if emi["emi_number"] <= last_paid_emi_number:
                continue
            if not last_payment_date:
                emi["last_payment_date"] = last_payment_date
            if total_bill_principal and payment_received_and_adjusted:
                diff = total_bill_principal - payment_received_and_adjusted
                emi["dpd"] = -99 if diff == 0 else (get_current_ist_time() - emi["due_date"]).days
                if diff >= 0:
                    emi["payment_received"] = payment_received_and_adjusted
                    if diff == 0:
                        last_paid_emi_number = emi["emi_number"]
                        emi["payment_status"] = "Paid"
                    break
                emi["payment_received"] = total_bill_principal
                payment_received_and_adjusted = abs(diff)
    session.bulk_update_mappings(CardEmis, emis_dict)
