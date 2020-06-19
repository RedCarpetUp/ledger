from datetime import timedelta
from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.ledger_utils import get_account_balance_from_str

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
    print("PRINCIPAL DUE")
    print(principal_due)
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
