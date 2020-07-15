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
) -> CardEmis:
    first_emi_due_date = user_card.card_activation_date + timedelta(
        days=user_card.interest_free_period_in_days + 1
    )
    principal_due = Decimal(last_bill.table.principal)
    due_amount = div(principal_due, 12)
    due_date = new_emi = None
    late_fine = total_interest = current_interest = next_interest = Decimal(0)
    # We will firstly create only 12 emis
    for i in range(1, 13):
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
            else Decimal(0)
        )
        total_closing_balance_post_due_date = (
            principal_due - mul(due_amount, (i - 1))
            if principal_due - mul(due_amount, (i - 1)) > 0
            else Decimal(0)
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
    return new_emi


def add_emi_on_new_bill(
    session: Session,
    user_card: UserCard,
    last_bill: BaseBill,
    last_emi_number: int,
    late_fee: Decimal = None,
    interest: Decimal = None,
) -> CardEmis:
    new_end_emi_number = last_emi_number + 1
    principal_due = Decimal(last_bill.table.principal)
    due_amount = div(principal_due, 12)
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
    )
    user_card_wrapped = get_user_card(session, user_card.user_id)
    total_interest = current_interest = next_interest = Decimal(0)
    min_due = user_card_wrapped.get_min_for_schedule()
    new_emi_list = []
    for emi in all_emis:
        emi_dict = emi.as_dict_for_json()
        # We consider 12 because the first insertion had 12 emis
        if emi_dict["emi_number"] <= new_end_emi_number - 12:
            new_emi_list.append(emi_dict)
            continue
        elif late_fee and emi_dict["emi_number"] == ((new_end_emi_number - 12) + 1):
            emi_dict["late_fee"] += late_fee
        emi_dict["due_amount"] += due_amount
        emi_dict["total_due_amount"] = (
            min_due
            if emi_dict["emi_number"] == ((new_end_emi_number - 12) + 1)
            else emi_dict["total_due_amount"] + due_amount
        )
        emi_dict["total_closing_balance"] += principal_due - (
            mul(due_amount, (emi_dict["emi_number"] - (new_end_emi_number - 12) - 1))
        )
        emi_dict["total_closing_balance_post_due_date"] += principal_due - (
            mul(due_amount, (emi_dict["emi_number"] - (new_end_emi_number - 12) - 1))
        )
        emi_dict["payment_status"] = "UnPaid"
        if interest:
            emi_dict["total_closing_balance_post_due_date"] += interest
            emi_dict["total_due_amount"] = (
                emi_dict["total_due_amount"] + interest
                if emi_dict["total_due_amount"] != min_due
                else min_due
            )
            emi_dict["interest_current_month"] += div(mul(interest, (30 - emi_dict["due_date"].day)), 30)
            emi_dict["interest_next_month"] = (interest + emi_dict["interest"]) - emi_dict[
                "interest_current_month"
            ]
            emi_dict["interest"] = emi_dict["interest_current_month"] + emi_dict["interest_next_month"]
        new_emi_list.append(emi_dict)
    session.bulk_update_mappings(CardEmis, new_emi_list)
    # Get the second last emi for calculating values of the last emi
    second_last_emi = all_emis[last_emi_number - 1]
    last_emi_due_date = second_last_emi.due_date + timedelta(days=user_card.statement_period_in_days + 1)
    if interest:
        current_interest += div(mul(interest, (30 - last_emi_due_date.day)), 30)
        next_interest += interest - current_interest
        total_interest = current_interest + next_interest
    total_closing_balance = (
        (principal_due - mul(due_amount, (new_end_emi_number - 1)))
        if (principal_due - mul(due_amount, (new_end_emi_number - 1))) > 0
        else Decimal(0)
    )
    total_closing_balance_post_due_date = (
        (principal_due - mul(due_amount, (new_end_emi_number - 1)))
        if (principal_due - mul(due_amount, (new_end_emi_number - 1))) > 0
        else Decimal(0)
    )
    new_emi = CardEmis(
        card_id=user_card.id,
        emi_number=new_end_emi_number,
        due_amount=due_amount,
        interest=total_interest,
        interest_current_month=current_interest,
        interest_next_month=next_interest,
        total_due_amount=due_amount,
        total_closing_balance=total_closing_balance,
        total_closing_balance_post_due_date=total_closing_balance_post_due_date,
        due_date=last_emi_due_date,
    )
    session.add(new_emi)
    session.flush()
    return new_emi


def slide_payments(session: Session, user_id: int, payment_event: LedgerTriggerEvent = None) -> None:
    def slide_payments_repeated_logic(
        emis_dict,
        payment_received_and_adjusted,
        payment_request_id,
        last_payment_date,
        last_paid_emi_number,
        all_paid=False,
    ) -> None:
        for emi in emis_dict:
            if emi["emi_number"] <= last_paid_emi_number or emi["total_due_amount"] <= Decimal(0):
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
                if (
                    payment_received_and_adjusted >= actual_closing_balance
                    and actual_closing_balance > 0
                ):
                    all_paid = True
                    emi["late_fee_received"] = emi["late_fee"]
                    emi["interest_received"] = emi["interest"]
                    emi["payment_received"] = emi["due_amount"]
                    emi["total_closing_balance"] = 0
                    emi["total_closing_balance_post_due_date"] = 0
                    last_paid_emi_number = emi["emi_number"]
                    emi["payment_status"] = "Paid"
                    # Create payment mapping
                    create_emi_payment_mapping(
                        session,
                        user_card,
                        emi["emi_number"],
                        last_payment_date,
                        payment_request_id,
                        emi["interest_received"],
                        emi["late_fee_received"],
                        emi["payment_received"],
                    )
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
                        # Create payment mapping
                        create_emi_payment_mapping(
                            session,
                            user_card,
                            emi["emi_number"],
                            last_payment_date,
                            payment_request_id,
                            emi["interest_received"],
                            emi["late_fee_received"],
                            emi["payment_received"],
                        )
                        break
                    else:
                        emi["late_fee_received"] = emi["late_fee"]
                        payment_received_and_adjusted -= emi["late_fee"]
                        if payment_received_and_adjusted <= emi["interest"]:
                            emi["interest_received"] = payment_received_and_adjusted
                            emi["total_closing_balance"] -= payment_received_and_adjusted
                            emi["total_closing_balance_post_due_date"] -= payment_received_and_adjusted
                            # Create payment mapping
                            create_emi_payment_mapping(
                                session,
                                user_card,
                                emi["emi_number"],
                                last_payment_date,
                                payment_request_id,
                                emi["interest_received"],
                                emi["late_fee_received"],
                                emi["payment_received"],
                            )
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
                                # Create payment mapping
                                create_emi_payment_mapping(
                                    session,
                                    user_card,
                                    emi["emi_number"],
                                    last_payment_date,
                                    payment_request_id,
                                    emi["interest_received"],
                                    emi["late_fee_received"],
                                    emi["payment_received"],
                                )
                                break
                emi["late_fee_received"] = emi["late_fee"]
                emi["interest_received"] = emi["interest"]
                emi["payment_received"] = emi["due_amount"]
                emi["payment_status"] = "Paid"
                last_paid_emi_number = emi["emi_number"]
                # Create payment mapping
                create_emi_payment_mapping(
                    session,
                    user_card,
                    emi["emi_number"],
                    last_payment_date,
                    payment_request_id,
                    emi["interest_received"],
                    emi["late_fee_received"],
                    emi["payment_received"],
                )
                payment_received_and_adjusted = abs(diff)

    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).first()
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
        .all()
    )
    if not all_emis:
        # Success and Error handling later
        return
    emis_dict = [u.__dict__ for u in all_emis]
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
                emis_dict,
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
            emis_dict,
            payment_received_and_adjusted,
            payment_request_id,
            last_payment_date,
            last_paid_emi_number,
            all_paid=all_paid,
        )

    session.bulk_update_mappings(CardEmis, emis_dict)


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
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in emis_for_this_bill.all()]
    if latest_bill.interest_to_charge:
        interest_due = Decimal(latest_bill.interest_to_charge)
        min_due = user_card.get_min_for_schedule()
        emi_count = 0
        if interest_due and interest_due > 0:
            for emi in emis_dict:
                emi["total_closing_balance_post_due_date"] += interest_due
                emi["total_due_amount"] = (
                    min_due if emi_count == 0 else emi["total_due_amount"] + interest_due
                )
                emi["interest_current_month"] += div(mul(interest_due, (30 - emi["due_date"].day)), 30)
                emi["interest_next_month"] = (interest_due + emi["interest"]) - emi[
                    "interest_current_month"
                ]
                emi["interest"] = emi["interest_current_month"] + emi["interest_next_month"]
                emi_count += 1
            session.bulk_update_mappings(CardEmis, emis_dict)


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
            .order_by(CardEmis.due_date.asc())
            .first()
        )
    emi_dict = emi.as_dict_for_json()
    _, late_fee = get_account_balance_from_str(session, f"{latest_bill.id}/bill/late_fine/r")
    if late_fee and late_fee > 0:
        emi_dict["total_closing_balance_post_due_date"] += late_fee
        emi_dict["total_due_amount"] = min_due if min_due else emi_dict["total_due_amount"] + late_fee
        emi_dict["late_fee"] += late_fee
        session.bulk_update_mappings(CardEmis, [emi_dict])


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


def add_moratorium_to_loan_emi(loan_emis, start_date, months_to_be_inserted: int):
    if not loan_emis:
        return {"result": "error", "message": "loan emis required"}
    dict_to_be_shifted_from = next((d for d in loan_emis if d["due_date"] >= start_date), False)
    final_emi_due_date_difference_with_start = (
        start_date - loan_emis[len(loan_emis) - 1]["due_date"]
    ).days
    if final_emi_due_date_difference_with_start > 90:
        return {"result": "error", "message": "incorrect start date given"}
    is_insertion_happening_in_the_end = False
    if dict_to_be_shifted_from:
        emi_number_to_begin_insertion_from = dict_to_be_shifted_from["emi_number"]
    else:
        is_insertion_happening_in_the_end = True
        emi_number_to_begin_insertion_from = loan_emis[len(loan_emis) - 1]["emi_number"]
    final_emi_list = []
    if not is_insertion_happening_in_the_end:
        for emi in loan_emis:
            temp_emi = emi.copy()
            if emi["emi_number"] == emi_number_to_begin_insertion_from:
                for i in range(months_to_be_inserted + 1):
                    insert_emi = temp_emi.copy()
                    # Need to just update emi related fields because
                    # late fine and interest will be handled through events
                    if i != months_to_be_inserted:
                        insert_emi["extra_details"] = {"moratorium": True}
                        insert_emi["payment_status"] = "Paid"
                        insert_emi.update(
                            insert_emi.fromkeys(
                                [
                                    "due_amount",
                                    "total_due_amount",
                                    "interest_current_month",
                                    "interest_next_month",
                                    "interest",
                                    "late_fee",
                                    "late_fee_received",
                                    "interest_received",
                                    "principal_received",
                                ],
                                Decimal(0),
                            )
                        )
                    insert_emi["emi_number"] += i
                    insert_emi["due_date"] += relativedelta(months=+i)
                    final_emi_list.append(insert_emi)
            elif emi["emi_number"] > emi_number_to_begin_insertion_from:
                temp_emi["emi_number"] += months_to_be_inserted
                temp_emi["due_date"] += relativedelta(months=+months_to_be_inserted)
            if emi["emi_number"] != emi_number_to_begin_insertion_from:
                final_emi_list.append(temp_emi)
    else:
        final_emi_list = loan_emis
        for i in range(months_to_be_inserted):
            # Need to just update emi related fields because
            # late fine and interest will be handled through events
            emi_data = {key: val for key, val in EMI_FORMULA_DICT.items()}
            emi_data["emi_number"] = emi_number_to_begin_insertion_from + i + 1
            emi_data["due_date"] = loan_emis[len(loan_emis) - 1]["due_date"] + relativedelta(
                months=+months_to_be_inserted
            )
            emi_data["extra_details"] = {"moratorium": True}
            final_emi_list.append(emi_data)
    return {"result": "success", "data": final_emi_list}


def check_moratorium_eligibility(session: Session, data):
    user_id = int(data["user_id"])
    start_date = parse_date(data["start_date"]).date()
    months_to_be_inserted = int(data["months_to_be_inserted"])
    user_card = get_user_card(session, user_id)
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
    )
    emis = [u.__dict__ for u in all_emis_query.all()]
    try:
        moratorium_start_emi = next(emi for emi in emis if emi["due_date"] >= start_date)
    except:
        moratorium_start_emi = None
    if not moratorium_start_emi:
        return {"result": "error", "message": "Not eligible for moratorium"}

    resp = add_moratorium_to_loan_emi(emis, start_date, months_to_be_inserted)
    if resp["result"] == "error":
        return resp

    # Updation of emis in schedule
    for emi in resp["data"]:
        if emi["extra_details"].get("moratorium"):
            new_emi = CardEmis(
                card_id=user_card.table.id,
                emi_number=emi["emi_number"],
                total_closing_balance=emi["total_closing_balance"],
                total_closing_balance_post_due_date=emi["total_closing_balance_post_due_date"],
                due_amount=emi["due_amount"],
                late_fee=emi["late_fee"],
                interest=emi["interest"],
                interest_current_month=emi["interest_current_month"],
                interest_next_month=emi["interest_next_month"],
                total_due_amount=emi["total_due_amount"],
                due_date=emi["due_date"],
                extra_details=emi["extra_details"],
            )
            session.add(new_emi)
    session.flush()
    session.bulk_update_mappings(CardEmis, resp["data"])


def refresh_schedule(session: Session, user_id: int):
    # Get user card
    user_card = get_user_card(session, user_id)

    # Get all generated bills of the user
    all_bills = user_card.get_all_bills()

    # Set all previous emis as inactive
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.table.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
        .all()
    )
    for emi in all_emis:
        emi.row_status = "inactive"
        session.flush()

    # Re-Create schedule from all the bills
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
                session, user_card.table, bill, last_emi.emi_number, late_fine_due, interest_due
            )

    # Check if user has opted for moratorium and adjust that in schedule
    moratorium = (
        session.query(LoanMoratorium).filter(LoanMoratorium.card_id == user_card.table.id).first()
    )
    if moratorium:
        all_emis_query = (
            session.query(CardEmis)
            .filter(CardEmis.card_id == user_card.table.id, CardEmis.row_status == "active")
            .order_by(CardEmis.due_date.asc())
        )
        all_emis = [u.__dict__ for u in all_emis_query.all()]
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
