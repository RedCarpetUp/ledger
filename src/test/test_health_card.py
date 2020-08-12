from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import create_user_card
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardKitNumbers,
    CardNames,
    Lenders,
    User,
)


def create_lenders(session: Session) -> None:
    dmi = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(dmi)

    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def card_db_updates(session: Session) -> None:
    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()

    ckn = CardKitNumbers(kit_number="10000", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()


def create_user(session: Session) -> None:
    u = User(id=3, performed_by=123,)
    session.add(u)
    session.flush()


def test_create_health_card(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)
    uc = create_user_card(
        session=session,
        user_id=3,
        card_activation_date=parse_date("2020-08-11").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    assert uc.card_type == "health_card"


def test_medical_health_card_swipe(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)
    uc = create_user_card(
        session=session,
        user_id=3,
        card_activation_date=parse_date("2020-08-11").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-08-11 18:30:10"),
        amount=Decimal(700),
        description="Amazon.com",
        mcc="8011",
    )
    swipe_loan_id = swipe["data"].loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{swipe_loan_id}/bill/unbilled/a")
    assert unbilled_balance == 700

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.id}/card/health_limit/l")
    assert medical_limit_balance == -700

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == 0

    _, lender_payable = get_account_balance_from_str(session, f"{uc.id}/card/lender_payable/l")
    assert lender_payable == 700


def test_mixed_health_card_swipe(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)
    uc = create_user_card(
        session=session,
        user_id=3,
        card_activation_date=parse_date("2020-08-11").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-08-11 18:30:10"),
        amount=Decimal(1500),
        description="Max Hospital",
        mcc="8011",
    )
    medical_swipe_loan_id = medical_swipe["data"].loan_id

    non_medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-08-11 18:30:10"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    non_medical_swipe_loan_id = non_medical_swipe["data"].loan_id

    assert non_medical_swipe_loan_id == medical_swipe_loan_id

    swipe_loan_id = medical_swipe["data"].loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{swipe_loan_id}/bill/unbilled/a")
    assert unbilled_balance == 2200

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.id}/card/health_limit/l")
    assert medical_limit_balance == -1500

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == -700

    _, lender_payable = get_account_balance_from_str(session, f"{uc.id}/card/lender_payable/l")
    assert lender_payable == 2200


def test_generate_health_card_bill_1(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)

    user_id = 3

    # assign card
    uc = create_user_card(
        session=session,
        user_id=user_id,
        card_activation_date=parse_date("2020-06-01").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-08 19:23:11"),
        amount=Decimal(1000),
        description="Amazon.com",
        mcc="8011",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(uc)
    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), uc)

    assert bill.bill_start_date == parse_date("2020-06-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 1000

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 114

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("30.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("30.67")


def test_generate_health_card_bill_2(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)

    user_id = 3

    # assign card
    uc = create_user_card(
        session=session,
        user_id=user_id,
        card_activation_date=parse_date("2020-06-01").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-08 19:23:11"),
        amount=Decimal(1000),
        description="Amazon.com",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(uc)
    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), uc)

    assert bill.bill_start_date == parse_date("2020-06-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 1000

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 114

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("30.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("30.67")


def test_generate_health_card_bill_3(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)

    user_id = 3

    # assign card
    uc = create_user_card(
        session=session,
        user_id=user_id,
        card_activation_date=parse_date("2020-06-01").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-08 19:23:11"),
        amount=Decimal(1000),
        description="Apollo Hospital",
        mcc="8011",
    )
    medical_bill_id = medical_swipe["data"].loan_id

    non_medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-09 19:23:11"),
        amount=Decimal(1500),
        description="Amazon.com",
    )
    non_medical_bill_id = non_medical_swipe["data"].loan_id

    assert medical_bill_id == non_medical_bill_id

    bill_id = medical_bill_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 2500

    bill = bill_generate(uc)
    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), uc)

    assert bill.bill_start_date == parse_date("2020-06-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 2500

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 285

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("76.675")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("76.675")
