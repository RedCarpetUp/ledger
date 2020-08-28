from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.base_card import BaseBill
from rush.card.health_card import (
    HealthBill,
    HealthCard,
)
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardKitNumbers,
    CardNames,
    CardTransaction,
    Lenders,
    Product,
    User,
)
from rush.payments import payment_received


def create_lenders(session: Session) -> None:
    dmi = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(dmi)

    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def create_products(session: Session) -> None:
    hc_product = Product(product_name="health_card")
    session.add(hc_product)
    session.flush()


def card_db_updates(session: Session) -> None:
    create_products(session=session)

    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()

    ckn = CardKitNumbers(kit_number="10000", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()


def create_user(session: Session) -> None:
    u = User(
        id=3,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def create_test_user_card(session: Session) -> HealthCard:
    uc = create_user_product(
        session=session,
        user_id=3,
        card_activation_date=parse_date("2020-07-01").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    return uc


def test_create_health_card(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

    assert uc.product_type == "health_card"
    assert uc.get_limit_type(mcc="8011") == "health_limit"
    assert uc.get_limit_type(mcc="5555") == "available_limit"
    assert uc.should_reinstate_limit_on_payment == True

    user_card = get_user_product(session=session, user_id=uc.user_id, card_type="health_card")
    assert isinstance(user_card, HealthCard) == True


def test_medical_health_card_swipe(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-11 18:30:10"),
        amount=Decimal(700),
        description="Amazon.com",
        mcc="8011",
    )
    swipe_loan_id = swipe["data"].loan_id

    transaction = session.query(CardTransaction).filter(CardTransaction.mcc == "8011").first()

    assert transaction.amount == Decimal(700)
    assert transaction.description == "Amazon.com"

    _, unbilled_balance = get_account_balance_from_str(session, f"{swipe_loan_id}/bill/unbilled/a")
    assert unbilled_balance == 700

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == -700

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == 0

    _, lender_payable = get_account_balance_from_str(session, f"{uc.loan_id}/loan/lender_payable/l")
    assert lender_payable == 700


def test_mixed_health_card_swipe(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

    medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-11 18:30:10"),
        amount=Decimal(1500),
        description="Max Hospital",
        mcc="8011",
    )
    medical_swipe_loan_id = medical_swipe["data"].loan_id

    non_medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-11 18:30:10"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    non_medical_swipe_loan_id = non_medical_swipe["data"].loan_id

    assert non_medical_swipe_loan_id == medical_swipe_loan_id

    swipe_loan_id = medical_swipe["data"].loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{swipe_loan_id}/bill/unbilled/a")
    assert unbilled_balance == 2200

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == -1500

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == -700

    _, lender_payable = get_account_balance_from_str(session, f"{uc.loan_id}/loan/lender_payable/l")
    assert lender_payable == 2200


def test_generate_health_card_bill_1(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)

    uc = create_test_user_card(session=session)

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

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, parse_date("2020-07-31"), uc)

    assert bill.bill_start_date == parse_date("2020-07-01").date()
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
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

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

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, parse_date("2020-07-31"), uc)

    assert bill.bill_start_date == parse_date("2020-07-01").date()
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
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

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

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, parse_date("2020-07-31"), uc)

    assert bill.bill_start_date == parse_date("2020-07-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 2500

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 284

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("75.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("75.67")


def test_mixed_payment_received(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

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

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, parse_date("2020-07-31"), uc)

    assert bill.bill_start_date == parse_date("2020-07-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 2500

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 284

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("75.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("75.67")

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == -1000

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == -1500

    payment_date = parse_date("2020-08-03")
    amount = Decimal(2000)

    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="mixed_payment",
    )

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == Decimal(0)

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == -500


def test_medical_payment_received(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

    medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-08 19:23:11"),
        amount=Decimal(1000),
        description="Apollo Hospital",
        mcc="8011",
    )
    bill_id = medical_swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, parse_date("2020-07-31"), uc)

    assert bill.bill_start_date == parse_date("2020-07-01").date()
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

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == -1000

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == Decimal(0)

    payment_received(
        session=session,
        user_card=uc,
        payment_amount=Decimal(700),
        payment_date=parse_date("2020-08-03"),
        payment_request_id="medical_payment",
    )

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == Decimal(-300)

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == Decimal(0)


def test_non_medical_payment_received(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_card(session=session)

    non_medical_swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-07-09 19:23:11"),
        amount=Decimal(1500),
        description="Amazon.com",
    )
    bill_id = non_medical_swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1500

    bill = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, parse_date("2020-07-31"), uc)

    assert bill.bill_start_date == parse_date("2020-07-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 1500

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 170

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("45")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("45")

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == Decimal(0)

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == -1500

    payment_date = parse_date("2020-08-03")
    amount = Decimal(1200)

    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="non_medical_payment",
    )

    _, medical_limit_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/health_limit/l")
    assert medical_limit_balance == Decimal(0)

    _, non_medical_limit_balance = get_account_balance_from_str(
        session, f"{uc.loan_id}/card/available_limit/l"
    )
    assert non_medical_limit_balance == -300


def test_health_card_loan(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)

    from rush.card.utils import get_product_id_from_card_type

    loan = HealthCard(
        session=session,
        user_id=3,
        product_id=get_product_id_from_card_type(session=session, card_type="health_card"),
        lender_id=62311,
        rc_rate_of_interest_monthly=Decimal(3),
        lender_rate_of_interest_annual=Decimal(18),
        bill_class=HealthBill,
    )

    session.add(loan)
    session.flush()

    assert loan.id is not None
    assert loan.product_type == "health_card"

    new_loan = session.query(HealthCard).filter(HealthCard.id == loan.id).one()

    assert new_loan is not None
    assert new_loan.product_type == "health_card"
