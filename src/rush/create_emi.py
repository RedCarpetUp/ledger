from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum import parse as parse_date
from sqlalchemy import or_
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql import func
from sqlalchemy.orm.attributes import flag_modified

from rush.anomaly_detection import get_payment_events
from rush.card import (
    BaseCard,
    get_user_card,
)
from rush.card.base_card import BaseBill
from rush.ledger_utils import (
    get_account_balance_from_str,
    get_remaining_bill_balance,
)
from rush.models import (
    BookAccount,
    CardEmis,
    EmiPaymentMapping,
    EventDpd,
    Fee,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
    UserCard,
)
from rush.utils import (
    div,
    mul,
)


def create_emis_for_card(
    session: Session,
    user_card: BaseCard,
    bill: BaseBill,
    late_fee: Decimal = None,
    interest: Decimal = None,
    atm_fee: Decimal = None,
    last_emi: CardEmis = None,
    bill_accumalation_till_date: Decimal = None,
) -> None:
    bill_tenure = bill.table.bill_tenure
    if not last_emi:
        due_date = user_card.card_activation_date
        principal_due = Decimal(bill.table.principal)
        due_amount = bill.table.principal_instalment
        start_emi_number = difference_counter = 1
    else:
        due_date = last_emi.due_date
        principal_due = Decimal(bill.table.principal - bill_accumalation_till_date)
        due_amount = div(principal_due, bill_tenure - last_emi.emi_number)
        start_emi_number = last_emi.emi_number + 1
        difference_counter = last_emi.emi_number
    late_fine = total_interest = current_interest = next_interest = Decimal(0)
    for i in range(start_emi_number, bill_tenure + 1):
        due_date += relativedelta(months=1, day=15)
        # A bill's late fee/atm fee will only go on first emi.
        late_fine = late_fee if late_fee and i == 1 else Decimal(0)
        atm_fine = atm_fee if atm_fee and i == 1 else Decimal(0)
        total_due_amount = due_amount
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
        if interest:
            current_interest = div(mul(interest, (30 - due_date.day)), 30)
            next_interest = interest - current_interest
            total_interest = current_interest + next_interest
            total_due_amount += interest
            total_closing_balance_post_due_date += interest
        extra_details = {str(bill.id): str(due_amount)}
        new_emi = CardEmis(
            card_id=user_card.id,
            emi_number=i,
            total_closing_balance=total_closing_balance,
            total_closing_balance_post_due_date=total_closing_balance_post_due_date,
            due_amount=due_amount,
            late_fee=late_fine,
            atm_fee=atm_fine,
            interest=total_interest,
            interest_current_month=current_interest,
            interest_next_month=next_interest,
            total_due_amount=total_due_amount,
            due_date=due_date,
            extra_details=extra_details,
        )
        session.add(new_emi)
    session.flush()


def add_emi_on_new_bill(
    session: Session,
    user_card: BaseCard,
    bill: BaseBill,
    last_emi: CardEmis,
    bill_number: int,
    late_fee: Decimal = None,
    interest: Decimal = None,
    atm_fee_due: Decimal = None,
    last_bill_tenure: Decimal = 12,
    post_date: DateTime = None,
    bill_accumalation_till_date: Decimal = None,
    last_old_bill_emi_number: int = None,
) -> None:
    last_emi_number = last_emi.emi_number
    bill_tenure = bill.table.bill_tenure
    if bill_tenure < last_bill_tenure:
        emis_to_be_inserted = 0
    else:
        emis_to_be_inserted = (bill_tenure - last_bill_tenure) + 1
    if bill_tenure < last_emi_number:
        emis_to_be_inserted = 0
    else:
        emis_to_be_inserted = (bill_tenure - last_emi_number) + 1
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )
    total_interest = current_interest = next_interest = Decimal(0)
    min_due = user_card.get_min_for_schedule()

    # Adjust due in accordance with extension
    if not post_date:
        principal_due = Decimal(bill.table.principal)
        due_amount = bill.table.principal_instalment
        difference_counter = bill_number
    else:
        principal_due = Decimal(bill.table.principal - bill_accumalation_till_date)
        due_amount = div(principal_due, bill_tenure - last_old_bill_emi_number)
        difference_counter = last_old_bill_emi_number + 1

    for emi in all_emis:
        # We consider 12 because the first insertion had 12 emis
        if emi.emi_number < bill_number or (post_date and emi.due_date < post_date.date()):
            continue
        elif late_fee and emi.emi_number == bill_number:
            emi.late_fee += late_fee
        elif atm_fee_due and emi.emi_number == bill_number:
            emi.atm_fee += atm_fee_due
        emi.due_amount += due_amount
        emi.total_due_amount = (
            min_due if emi.emi_number == bill_number else emi.total_due_amount + due_amount
        )
        emi.total_closing_balance += principal_due - (
            mul(due_amount, (emi.emi_number - difference_counter))
        )
        emi.total_closing_balance_post_due_date += principal_due - (
            mul(due_amount, (emi.emi_number - difference_counter))
        )
        emi.payment_status = "UnPaid"
        emi.extra_details[str(bill.id)] = str(due_amount)
        flag_modified(emi, "extra_details")
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
                last_emi_due_date = second_last_emi.due_date + relativedelta(months=1, day=15)
            else:
                last_emi_due_date += relativedelta(months=1, day=15)
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
            extra_details = {str(bill.id): str(due_amount)}
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
                extra_details=extra_details,
            )
            session.add(new_emi)
    session.flush()


def slide_payments(user_card: BaseCard, payment_event: LedgerTriggerEvent = None) -> None:
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
            emi.dpd = (last_payment_date.date() - emi.due_date).days
            if emi.emi_number <= last_paid_emi_number or emi.total_due_amount <= Decimal(0):
                continue
            if last_payment_date:
                emi.last_payment_date = last_payment_date
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
                        emi.payment_received = actual_closing_balance - emi.late_fee - emi.interest
                        emi.total_closing_balance = emi.total_closing_balance_post_due_date = 0
                    else:
                        emi.payment_received = actual_closing_balance - emi.late_fee
                        emi.total_closing_balance = (
                            emi.total_closing_balance_post_due_date
                        ) = emi.interest = emi.interest_current_month = emi.interest_next_month = 0
                    emi.due_amount = emi.total_due_amount = actual_closing_balance
                    last_paid_emi_number = emi.emi_number
                    emi.payment_status = "Paid"
                    emi.dpd = 0
                    # Create payment mapping
                    create_emi_payment_mapping(
                        session,
                        user_card,
                        emi.emi_number,
                        last_payment_date,
                        payment_request_id,
                        emi.interest_received,
                        emi.late_fee_received,
                        emi.atm_fee_received,
                        emi.payment_received,
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
                            session,
                            user_card,
                            emi.emi_number,
                            last_payment_date,
                            payment_request_id,
                            emi.interest_received,
                            emi.late_fee_received,
                            emi.atm_fee_received,
                            emi.payment_received,
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
                                session,
                                user_card,
                                emi.emi_number,
                                last_payment_date,
                                payment_request_id,
                                emi.interest_received,
                                emi.late_fee_received,
                                emi.atm_fee_received,
                                emi.payment_received,
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
                                    session,
                                    user_card,
                                    emi.emi_number,
                                    last_payment_date,
                                    payment_request_id,
                                    emi.interest_received,
                                    emi.late_fee_received,
                                    emi.atm_fee_received,
                                    emi.payment_received,
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
                                        session,
                                        user_card,
                                        emi.emi_number,
                                        last_payment_date,
                                        payment_request_id,
                                        emi.interest_received,
                                        emi.late_fee_received,
                                        emi.atm_fee_received,
                                        emi.payment_received,
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
                    session,
                    user_card,
                    emi.emi_number,
                    last_payment_date,
                    payment_request_id,
                    emi.interest_received,
                    emi.late_fee_received,
                    emi.atm_fee_received,
                    emi.payment_received,
                )
                payment_received_and_adjusted = abs(diff)

    session = user_card.session
    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
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


def adjust_interest_in_emis(session: Session, user_card: BaseCard, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(
            LoanData.card_id == user_card.id,
            LoanData.bill_start_date <= post_date,
            LoanData.is_generated.is_(True),
        )
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
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


def adjust_late_fee_in_emis(session: Session, user_card: BaseCard, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(
            LoanData.card_id == user_card.id,
            LoanData.bill_start_date < post_date,
            LoanData.is_generated.is_(True),
        )
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
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


def adjust_atm_fee_in_emis(session: Session, user_card: BaseCard, post_date: DateTime) -> None:
    latest_bill = (
        session.query(LoanData)
        .filter(
            LoanData.card_id == user_card.id,
            LoanData.bill_start_date < post_date,
            LoanData.is_generated.is_(True),
        )
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
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
    _, atm_fee = get_account_balance_from_str(session, f"{latest_bill.id}/bill/atm_fee_accrued/r")
    if atm_fee and atm_fee > 0:
        emi.total_closing_balance_post_due_date += atm_fee
        emi.total_due_amount = min_due if min_due else emi.total_due_amount + atm_fee
        emi.atm_fee += atm_fee
        session.flush()


def create_emi_payment_mapping(
    session: Session,
    user_card: UserCard,
    emi_number: int,
    payment_date: DateTime,
    payment_request_id: str,
    interest_received: Decimal,
    late_fee_received: Decimal,
    atm_fee_received: Decimal,
    principal_received: Decimal,
) -> None:
    new_payment_mapping = EmiPaymentMapping(
        card_id=user_card.id,
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
    # Total due amount adjustment
    total_due_amount_addition_interest = 0
    for i in range(
        emi_number_to_begin_insertion_from, emi_number_to_begin_insertion_from + months_to_be_inserted
    ):
        total_due_amount_addition_interest += loan_emis[i - 1].interest
    loan_emis[
        emi_number_to_begin_insertion_from - 1
    ].total_due_amount += total_due_amount_addition_interest
    session.flush()
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


def refresh_schedule(user_card: BaseCard, post_date: DateTime = None):
    session = user_card.session
    # Get all generated bills of the user
    all_bills = user_card.get_all_bills()

    pre_post_date_emis = None
    # Considering the post_date case only for extension
    if not post_date:
        # Set all previous emis as inactive
        all_emis = (
            session.query(CardEmis)
            .filter(CardEmis.card_id == user_card.table.id, CardEmis.row_status == "active")
            .order_by(CardEmis.emi_number.asc())
            .all()
        )
    else:
        # Set all emis after post date as inactive
        all_emis = (
            session.query(CardEmis)
            .filter(
                CardEmis.card_id == user_card.table.id,
                CardEmis.row_status == "active",
                CardEmis.due_date >= post_date,
            )
            .order_by(CardEmis.emi_number.asc())
            .all()
        )
        # Get emis pre post date and set payments to zero. This is done to get per bill amount as well
        pre_post_date_emis = (
            session.query(CardEmis)
            .filter(
                CardEmis.card_id == user_card.table.id,
                CardEmis.row_status == "active",
                CardEmis.due_date < post_date,
            )
            .order_by(CardEmis.emi_number.asc())
            .all()
        )
        for emi in pre_post_date_emis:
            emi.atm_fee_received = (
                emi.interest_received
            ) = emi.late_fee_received = emi.principal_received = Decimal(0)
    for emi in all_emis:
        emi.row_status = "inactive"
        session.flush()

    all_payment_mappings = (
        session.query(EmiPaymentMapping)
        .filter(
            EmiPaymentMapping.card_id == user_card.table.id, EmiPaymentMapping.row_status == "active"
        )
        .all()
    )
    for mapping in all_payment_mappings:
        mapping.row_status = "inactive"
        session.flush()

    # Re-Create schedule from all the bills
    bill_number = 1
    last_bill_tenure = 0
    for bill in all_bills:
        bill_accumalation_till_date = Decimal(0)
        if post_date and pre_post_date_emis:
            for emi in pre_post_date_emis:
                if not emi.extra_details.get("moratorium"):
                    bill_accumalation_till_date += (
                        Decimal(emi.extra_details.get(str(bill.id)))
                        if emi.extra_details.get(str(bill.id))
                        else Decimal(0)
                    )
        fees = session.query(Fee).filter(Fee.bill_id == bill.id, Fee.fee_status != "REVERSED").all()
        late_fine_due = atm_fee_due = 0
        for fee in fees:
            if fee.name == "late_fee":
                late_fine_due = fee.gross_amount
            elif fee.name == "atm_fee":
                atm_fee_due = fee.gross_amount
        interest_due = bill.table.interest_to_charge
        last_emi = (
            session.query(CardEmis)
            .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
            .order_by(CardEmis.due_date.desc())
            .first()
        )
        if not last_emi or (last_emi and last_emi.emi_number < bill.bill_tenure):
            create_emis_for_card(
                session,
                user_card,
                bill,
                late_fine_due,
                interest_due,
                atm_fee_due,
                last_emi,
                bill_accumalation_till_date,
            )
        else:
            add_emi_on_new_bill(
                session,
                user_card,
                bill,
                last_emi,
                bill_number,
                late_fine_due,
                interest_due,
                atm_fee_due,
                last_bill_tenure,
                post_date,
                bill_accumalation_till_date,
                # The last old bill emi number can only exist in case of post date existence
                pre_post_date_emis[-1].emi_number if post_date else 0,
            )
        last_bill_tenure = bill.table.bill_tenure
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
                "user_id": user_card.user_id,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "months_to_be_inserted": months_to_be_inserted,
            },
        )

    # Slide all payments
    slide_payments(user_card=user_card)


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

    if ledger_trigger_event.name == event_type and ledger_trigger_event.id == event_id:
        verdict = True
    else:
        verdict = False

    if event_amount and ledger_entry.amount + event_amount == ledger_trigger_event.amount:
        return False

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


def update_event_with_dpd(user_card: BaseCard, post_date: DateTime = None) -> None:
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
        bill = (
            session.query(LoanData)
            .filter(LoanData.card_id == user_card.id, LoanData.id == account.identifier,)
            .first()
        )
        dpd = (event_post_date - bill.bill_due_date).days
        new_event = EventDpd(
            bill_id=account.identifier,
            card_id=user_card.id,
            event_id=ledger_trigger_event.id,
            credit=credit_amount,
            debit=debit_amount,
            balance=get_remaining_bill_balance(session, bill, ledger_trigger_event.post_date)[
                "total_due"
            ],
            dpd=dpd,
        )
        session.add(new_event)

    session = user_card.session

    # TODO Need to bring the start and end into context later
    # if not post_date:
    #     start_time = pendulum.today("Asia/Kolkata").replace(tzinfo=None)
    #     end_time = pendulum.yesterday("Asia/Kolkata").replace(tzinfo=None)
    # else:
    #     start_time = post_date + relativedelta(days=-1)
    #     end_time = post_date

    debit_book_account = aliased(BookAccount)
    credit_book_account = aliased(BookAccount)
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
        event_post_date = ledger_trigger_event.post_date.date()
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
    #     .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
    #     .order_by(CardEmis.emi_number.asc())
    #     .all()
    # )
    # for emi in all_emis:
    #     if emi.payment_status != "Paid":
    #         emi.dpd = (post_date.date() - emi.due_date).days

    max_dpd = session.query(func.max(EventDpd.dpd).label("max_dpd")).one()
    user_card.table.dpd = max_dpd.max_dpd
    if not user_card.table.ever_dpd or max_dpd.max_dpd > user_card.table.ever_dpd:
        user_card.table.ever_dpd = max_dpd.max_dpd

    session.flush()
