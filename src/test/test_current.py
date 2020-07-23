import contextlib
from decimal import Decimal
from io import StringIO

import alembic
from _pytest.monkeypatch import MonkeyPatch
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_late_charges
from rush.anomaly_detection import run_anomaly
from rush.card import (
    create_user_card,
    get_user_card,
)
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.create_emi import (
    check_moratorium_eligibility,
    refresh_schedule,
)
from rush.ledger_utils import (
    get_account_balance_from_str,
    is_bill_closed,
)
from rush.lender_funds import (
    lender_disbursal,
    m2p_transfer,
)
from rush.models import (
    CardEmis,
    EmiPaymentMapping,
    LedgerTriggerEvent,
    LoanData,
    LoanMoratorium,
    User,
    UserPy,
)
from rush.payments import payment_received
from rush.views import (
    bill_view,
    user_view,
)


def test_current(get_alembic: alembic.config.Config) -> None:
    """Test that the alembic current command does not erorr"""
    # Runs with no error
    # output = run_alembic_command(pg["engine"], "current",  {})

    stdout = StringIO()
    with contextlib.redirect_stdout(stdout):
        # command_func(alembic_cfg, **command_kwargs)
        alembic_current(get_alembic, {})
    assert stdout.getvalue() == ""
    # assert output == ""


def test_user2(session: Session) -> None:
    u = User(performed_by=123, id=1, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    print(a.id)
    u = UserPy(id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",)


def test_user(session: Session) -> None:
    u = User(id=2, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    u = UserPy(id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",)


def test_lender_disbursal(session: Session) -> None:
    amount = 100000
    val = lender_disbursal(session, amount)
    assert val == Decimal(100000)


def test_m2p_transfer(session: Session) -> None:
    amount = 50000
    val = m2p_transfer(session, amount)
    assert val == Decimal(50000)


def test_card_swipe(session: Session) -> None:
    uc = create_user_card(
        session=session,
        user_id=2,
        card_activation_date=parse_date("2020-05-01").date(),
        card_type="ruby",
    )
    user_card_id = uc.id

    swipe1 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    swipe2 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )
    assert swipe1.loan_id == swipe2.loan_id
    bill_id = swipe1.loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{bill_id}/bill/unbilled/a")
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've not loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{user_card_id}/card/available_limit/l")
    assert card_balance == -900

    _, lender_payable = get_account_balance_from_str(session, f"{user_card_id}/card/lender_payable/l")
    assert lender_payable == 900


def test_card_swipe_2(session: Session) -> None:
    uc = create_user_card(
        session=session,
        user_id=2,
        card_activation_date=parse_date("2020-05-01").date(),
        card_type="ruby",
    )
    user_card_id = uc.id

    swipe1 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
    )

    swipe2 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-10-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )

    bill_id = swipe2.loan_id
    bill = session.query(LoanData).filter(LoanData.id == bill_id).all()
    assert str(bill[0].bill_start_date) == "2020-10-01"
    assert str(bill[0].bill_close_date) == "2020-11-01"


def test_card_swipe_3(session: Session) -> None:
    uc = create_user_card(
        session=session,
        user_id=2,
        card_activation_date=parse_date("2020-05-01").date(),
        statement_period_in_days=15,
        card_type="ruby",
    )
    swipe2 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-01 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )

    bill_id = swipe2.loan_id
    bill = session.query(LoanData).filter(LoanData.id == bill_id).all()
    assert str(bill[0].bill_close_date) == "2020-05-15"


def test_generate_bill_1(session: Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
    )

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )

    bill_id = swipe.loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(session=session, user_card=uc)

    assert bill.bill_start_date == parse_date("2020-04-02").date()
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
        session, book_string=f"{bill_id}/bill/interest_earned/r"
    )
    assert interest_due == Decimal("30.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(card_id=uc.id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event.post_date.date() == parse_date("2020-05-02").date()


def _partial_payment_bill_1(session: Session) -> None:
    user_card = get_user_card(session, 99)
    payment_date = parse_date("2020-05-03")
    amount = Decimal(100)
    unpaid_bills = user_card.get_unpaid_bills()
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("1000")
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    bill = unpaid_bills[0]
    _, gateway_expenses = get_account_balance_from_str(
        session, book_string="12345/redcarpet/gateway_expenses/e"
    )
    assert gateway_expenses == 0.5

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal("930.67")

    min_due = bill.get_remaining_min()
    assert min_due == 14

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")


def test_partial_payment_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)


def _accrue_late_fine_bill_1(session: Session) -> None:
    user_card = get_user_card(session, 99)
    event_date = parse_date("2020-05-16 00:00:00")
    bill = accrue_late_charges(session, user_card, event_date)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(100)

    _, late_fine_earned = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_earned == Decimal(100)

    min_due = bill.get_remaining_min()
    assert min_due == 114


def _accrue_late_fine_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    event_date = parse_date("2020-05-16 00:00:00")
    user_card = get_user_card(session, 99)
    bill = accrue_late_charges(session, user_card, event_date)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(100)

    min_due = bill.get_remaining_min()
    assert min_due == Decimal("270")


def test_accrue_late_fine_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    # did only partial payment so accrue late fee.
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)


def _pay_minimum_amount_bill_1(session: Session) -> None:
    user_card = get_user_card(session, 99)

    unpaid_bills = user_card.get_unpaid_bills()

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    # Pay 13.33 more. and 100 for late fee.
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal("114"),
        payment_date=parse_date("2020-05-20"),
        payment_request_id="a123",
    )
    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_remaining_min()
    assert min_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, late_fine_earned = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_earned == Decimal(100)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == Decimal("916.67")

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("787.0")


def test_accrue_interest_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)


def test_late_fee_reversal_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)

    user_card = get_user_card(session, 99)

    unpaid_bills = user_card.get_unpaid_bills()

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    # Pay 13.33 more. and 100 for late fee.
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal("114"),
        # Payment came before the due date.
        payment_date=parse_date("2020-06-14"),
        payment_request_id="a123",
    )
    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_remaining_min()
    assert min_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_due == Decimal("100")

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    # changed from 916 to 816, the late did not get settled.
    assert principal_due == Decimal("916.67")

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("787.0")


def test_is_bill_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)

    user_card = get_user_card(session, 99)

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_card.user_id)
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    # Should be false because min is 130 and payment made is 120
    is_it_paid = is_bill_closed(session, bill)
    assert is_it_paid is False
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("787")

    # Need to pay 916.67 more to close the bill.
    remaining_principal = Decimal("916.67")
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=remaining_principal,
        payment_date=parse_date("2020-05-05"),
        payment_request_id="a123",
    )
    is_it_paid_now = is_bill_closed(session, bill)
    assert is_it_paid_now is True

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-129.17")  # negative that implies prepaid


def _generate_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = get_user_card(session, 99)

    previous_bill = (  # get last generated bill.
        session.query(LoanData)
        .filter(LoanData.user_id == user.id, LoanData.is_generated.is_(True))
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    # Bill shouldn't be closed.
    assert is_bill_closed(session, previous_bill) is False

    # Do transaction to create new bill.
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(2000),
        description="BigBasket.com",
    )

    _, user_card_balance = get_account_balance_from_str(
        session=session, book_string=f"{uc.id}/card/available_limit/a"
    )
    assert user_card_balance == Decimal(-3000)

    bill_2 = bill_generate(session=session, user_card=uc)
    assert bill_2.bill_start_date == parse_date("2020-05-02").date()

    unpaid_bills = uc.get_unpaid_bills()
    assert len(unpaid_bills) == 2

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill_2.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(2000)

    min_due = bill_2.get_remaining_min()
    assert min_due == Decimal("227")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_2.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("60.33")

    first_bill = unpaid_bills[0]
    first_bill_min_due = first_bill.get_remaining_min()
    assert first_bill_min_due == Decimal("114")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("30.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_earned/r"
    )
    assert interest_due == Decimal("61.34")


def _generate_bill_3(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = get_user_card(session, 99)

    previous_bill = (  # new bill isn't generated yet so get latest.
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    # Bill shouldn't be closed.
    assert is_bill_closed(session, previous_bill) is False

    # Do transaction to create new bill.
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-06-08 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
    )

    _, user_card_balance = get_account_balance_from_str(
        session=session, book_string=f"{uc.id}/card/available_limit/a"
    )
    # previously 1000 now 2000 after a 1000 purchase
    assert user_card_balance == Decimal(-2000)

    bill = bill_generate(session=session, user_card=uc)

    assert bill.bill_start_date == parse_date("2020-06-02").date()
    unpaid_bills = uc.get_unpaid_bills()
    assert len(unpaid_bills) == 2

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(1000)

    min_due = bill.get_remaining_min()
    assert min_due == Decimal("114")


def _run_anomaly_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    run_anomaly(session, bill)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, late_fee_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fee_received/a"
    )
    assert late_fee_received == Decimal(0)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got moved from late received to principal received.
    assert principal_due == 970 - 100


# def test_anomaly_late_payment_received(session: Session) -> None:
#     test_generate_bill_1(session)
#     _accrue_late_fine_bill_1(session)  # Accrue late fine first.
#     _min_payment_delayed_bill_1(session)  # Payment comes in our system late.
#     _run_anomaly_bill_1(session)
#     _accrue_interest_bill_1(session)


def test_generate_bill_2(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)


def test_generate_bill_3(session: Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 20:23:11"),
        amount=Decimal(1500),
        description="Flipkart.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill = bill_generate(session=session, user_card=uc)

    _, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/unbilled_transactions/a"
    )
    assert unbilled_balance == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == 1500

    _, min_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/min/a")
    assert min_due == 170


def test_emi_creation(session: Session) -> None:
    a = User(id=108, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    # Generate bill
    bill_april = bill_generate(session=session, user_card=uc)

    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )  # Get the latest emi of that user.

    last_emi = all_emis[11]
    assert last_emi.emi_number == 12


def test_subsequent_emi_creation(session: Session) -> None:
    a = User(id=160, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    bill_april = bill_generate(session=session, user_card=uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )  # Get the latest emi of that user.

    last_emi = all_emis[12]
    first_emi = all_emis[0]
    second_emi = all_emis[1]
    assert first_emi.due_amount == 500
    assert last_emi.due_amount == 500
    assert second_emi.due_amount == 1000
    assert last_emi.emi_number == 13
    assert last_emi.due_date.strftime("%Y-%m-%d") == "2021-05-25"


def test_schedule_for_interest_and_payment(session: Session) -> None:
    a = User(id=1991, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-05-01").date(),
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Check calculated interest
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_may.id}/bill/interest_receivable/a"
    )
    assert interest_due == 180

    # Check if emi is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]
    first_emi = emis_dict[0]
    assert first_emi["interest_current_month"] == 84
    assert first_emi["interest_next_month"] == 96

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("6000")

    # Do Full Payment
    payment_date = parse_date("2020-07-30")
    amount = Decimal(6360)
    bill = payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-359.5")

    # Refresh Schedule
    # slide_payments(session, a.id)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    assert emis_dict[0]["due_date"] == parse_date("2020-06-16").date()
    assert emis_dict[0]["total_due_amount"] == 680
    assert emis_dict[0]["due_amount"] == 500
    assert emis_dict[0]["total_closing_balance"] == 6000
    assert emis_dict[0]["total_closing_balance_post_due_date"] == 6180
    assert emis_dict[0]["interest_received"] == 180
    assert emis_dict[0]["payment_received"] == 500
    assert emis_dict[0]["interest"] == 180
    assert emis_dict[0]["interest_current_month"] == 84
    assert emis_dict[0]["interest_next_month"] == 96
    assert emis_dict[1]["due_date"] == parse_date("2020-07-17").date()
    assert emis_dict[1]["total_due_amount"] == 680
    assert emis_dict[1]["due_amount"] == 500
    assert emis_dict[1]["total_closing_balance"] == 5500
    assert emis_dict[1]["total_closing_balance_post_due_date"] == 5680
    assert emis_dict[1]["interest_received"] == 180
    assert emis_dict[1]["payment_received"] == 500
    assert emis_dict[1]["interest"] == 180
    assert emis_dict[1]["interest_current_month"] == 78
    assert emis_dict[1]["interest_next_month"] == 102
    assert emis_dict[2]["due_date"] == parse_date("2020-08-17").date()
    assert emis_dict[2]["total_due_amount"] == 5000
    assert emis_dict[2]["due_amount"] == 5000
    assert emis_dict[2]["total_closing_balance"] == 0
    assert emis_dict[2]["total_closing_balance_post_due_date"] == 0
    assert emis_dict[2]["interest_received"] == 0
    assert emis_dict[2]["payment_received"] == 5000
    assert emis_dict[2]["interest"] == 0
    assert emis_dict[2]["interest_current_month"] == 0
    assert emis_dict[2]["interest_next_month"] == 0
    assert emis_dict[3]["due_date"] == parse_date("2020-09-17").date()
    assert emis_dict[3]["total_due_amount"] == 0
    assert emis_dict[3]["due_amount"] == 0
    assert emis_dict[3]["total_closing_balance"] == 0
    assert emis_dict[3]["total_closing_balance_post_due_date"] == 0
    assert emis_dict[3]["interest_received"] == 0
    assert emis_dict[3]["payment_received"] == 0
    assert emis_dict[3]["interest"] == 0
    assert emis_dict[3]["interest_current_month"] == 0
    assert emis_dict[3]["interest_next_month"] == 0


def test_with_live_user_loan_id_4134872(session: Session) -> None:
    a = User(
        id=1764433,
        performed_by=123,
        name="UPENDRA",
        fullname="UPENDRA SINGH",
        nickname="UPENDRA",
        email="upsigh921067@gmail.com",
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-05-01").date(),
    )

    # Card transactions
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-20 17:23:01"),
        amount=Decimal(129),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-22 09:33:18"),
        amount=Decimal(115),
        description="TPL*UDIO               MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-22 09:50:46"),
        amount=Decimal(500),
        description="AIRTELMONEY            MUMBAI        IND",
    )
    # This was refunded so can be used to test refund
    # create_card_swipe(
    #     session=session,
    #     user_card=uc,
    #     txn_time=parse_date("2020-05-22 12:50:05"),
    #     amount=Decimal(2),
    #     description="PHONEPE RECHARGE.      GURGAON       IND",
    # )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 01:18:54"),
        amount=Decimal(100),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 01:42:51"),
        amount=Decimal(54),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 01:49:44"),
        amount=Decimal(1100),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 13:12:33"),
        amount=Decimal(99),
        description="ULLU DIGITAL PRIVATE L MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-24 22:09:42"),
        amount=Decimal(99),
        description="PayTM*KookuDigitalPriP Mumbai        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 08:33:40"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 10:26:12"),
        amount=Decimal(380),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 11:40:05"),
        amount=Decimal(199),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 11:57:15"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 12:25:57"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 08:04:47"),
        amount=Decimal(1450),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 14:47:41"),
        amount=Decimal(110),
        description="TPL*UDIO               MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 16:37:27"),
        amount=Decimal(700),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 22:10:58"),
        amount=Decimal(160),
        description="Linkyun Technology Pri Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-27 12:25:25"),
        amount=Decimal(299),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-28 20:38:02"),
        amount=Decimal(199),
        description="Linkyun Technology Pri Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-28 21:45:55"),
        amount=Decimal(800),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-29 10:05:58"),
        amount=Decimal(525),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-30 16:04:21"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    # Generate bill
    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("12914")

    # Do Partial Payment
    payment_date = parse_date("2020-06-18 06:55:00")
    amount = Decimal(324)
    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("12590.5")

    # Refresh Schedule
    # slide_payments(session, a.id)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]
    first_emi = emis_dict[0]
    second_emi = emis_dict[1]

    assert first_emi["interest"] == Decimal("387.83")
    assert first_emi["interest_received"] == Decimal("324")


def test_interest_reversal_interest_already_settled(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)

    #  Pay 500 rupees
    user_card = get_user_card(session, 99)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("787")

    payment_date = parse_date("2020-05-14 19:23:11")
    amount = Decimal("886")
    unpaid_bills = user_card.get_unpaid_bills()
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-98.5")

    bill = unpaid_bills[0]

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 0

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(0)

    # TODO more testing scenarios.
    # 1. interest is not settled. 2. There are multiple bills.


def test_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    #  Pay 500 rupees
    user_card = get_user_card(session, 99)
    payment_date = parse_date("2020-06-14 19:23:11")
    amount = Decimal("3008.34")
    unpaid_bills = user_card.get_unpaid_bills()
    first_bill = unpaid_bills[0]
    second_bill = unpaid_bills[1]

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == Decimal("61.34")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == Decimal("60.33")

    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-220.84")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_earned/r"
    )
    # 30.67 Interest got removed from first bill.
    assert interest_earned == Decimal("30.67")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == Decimal(0)

    assert is_bill_closed(session, first_bill) is True
    # 90 got settled in new bill.
    assert is_bill_closed(session, second_bill) is True


def test_failed_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    user_card = get_user_card(session, 99)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("2787")

    payment_date = parse_date(
        "2020-06-18 19:23:11"
    )  # Payment came after due date. Interest won't get reversed.
    amount = Decimal("2916.67")
    unpaid_bills = user_card.get_unpaid_bills()
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-129.17")

    first_bill = unpaid_bills[0]
    second_bill = unpaid_bills[1]

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_earned/r"
    )
    # 30 Interest did not get removed.
    assert interest_earned == Decimal("61.34")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == Decimal("60.33")
    assert is_bill_closed(session, first_bill) is True
    assert is_bill_closed(session, second_bill) is False


def _pay_minimum_amount_bill_2(session: Session) -> None:
    user_card = get_user_card(session, 99)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("1500")

    # Pay 10 more. and 100 for late fee.
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal(110),
        payment_date=parse_date("2020-06-20"),
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("1390.5")

    balance_paid = (
        session.query(LedgerTriggerEvent)
        .order_by(LedgerTriggerEvent.post_date.desc())
        .filter(LedgerTriggerEvent.name == "payment_received")
        .first()
    )
    assert balance_paid.amount == Decimal(110)


def test_view(session: Session) -> None:
    test_generate_bill_3(session)
    # _partial_payment_bill_1(session)
    _accrue_late_fine_bill_2(session)
    _pay_minimum_amount_bill_2(session)
    # _accrue_interest_bill_1(session)
    # _generate_bill_2(session)
    user_card = get_user_card(session, 99)
    value = 1.1
    print(value)

    user_bill = user_view(session, user_card)
    assert user_bill["current_bill_balance"] == Decimal(1500)
    assert user_bill["current_bill_interest"] == Decimal(35)
    assert user_bill["min_to_pay"] == Decimal(35)  # sum of all interest and fines
    bill_details = bill_view(session, user_card)
    assert bill_details[0]["transactions"][0]["amount"] == Decimal(1500)
    # TODO bill = session.query(LoanData).filter(LoanData.user_id == user.id).first()
    # transactions = transaction_view(session, bill_id=bill.id)
    # assert transactions[0]["amount"] == Decimal(1500)


# def test_refund_1(session: Session) -> None:
#     test_generate_bill_1(session)
#     _generate_bill_3(session)
#     user_card = get_user_card(session, 99)
#     unpaid_bills = user_card.get_unpaid_bills()
#
#     status = refund_payment(session, 99, unpaid_bills[0].id)
#     assert status == True
#     _, amount = get_account_balance_from_str(session, book_string=f"62311/lender/merchant_refund/a")
#     assert amount == Decimal("1061.34")  # 1000 refunded with interest 60


# def test_lender_incur(session: Session) -> None:
#     test_refund_1(session)
#     status = lender_interest_incur(session)
#     uc = get_user_card(session, 99)
#     _, amount = get_account_balance_from_str(session, book_string=f"{uc.id}/card/lender_payable/l")
#     assert amount == Decimal("2054.74")  # on date 2020-06-28


def test_prepayment(session: Session) -> None:
    test_generate_bill_1(session)
    uc = get_user_card(session, 99)
    user_card_id = uc.id

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("1000")

    # prepayment of rs 2000 done
    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-999.5")

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/pre_payment/l"
    )
    assert prepayment_amount == Decimal("969.33")

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )
    bill_id = swipe.loan_id

    emi_payment_mapping = (
        session.query(EmiPaymentMapping).filter(EmiPaymentMapping.card_id == user_card_id).all()
    )
    first_payment_mapping = emi_payment_mapping[0]
    assert first_payment_mapping.emi_number == 1
    assert first_payment_mapping.interest_received == Decimal(0)
    assert first_payment_mapping.principal_received == Decimal(1000)

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(session=session, user_card=uc)

    assert bill.table.is_generated is True

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/pre_payment/l"
    )
    assert prepayment_amount == Decimal("0")

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == Decimal("30.67")

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("0.5")

    amount = Decimal(1000)
    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("-999")

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/pre_payment/l"
    )
    # left amount deducted from the payment
    assert prepayment_amount == Decimal("967.89")

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-06-08 19:23:11"),
        amount=Decimal(800),
        description="Myntra.com",
    )
    bill_id = swipe.loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 800

    bill = bill_generate(session=session, user_card=uc)

    assert bill.is_generated is False

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/pre_payment/l"
    )
    assert prepayment_amount == Decimal("167.89")  # 800 deducted from 967.89


def test_moratorium(session: Session) -> None:
    a = User(
        id=38612,
        performed_by=123,
        name="Ananth",
        fullname="Ananth Venkatesh",
        nickname="Ananth",
        email="ananth@redcarpetup.com",
    )
    session.add(a)
    session.flush()

    # assign card
    # 25 days to enforce 15th june as first due date
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-01-20").date(),
        interest_free_period_in_days=25,
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-01-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    # Generate bill
    generate_date = parse_date("2020-02-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    check_moratorium_eligibility(
        session, {"user_id": a.id, "start_date": "2020-03-01", "months_to_be_inserted": 3}
    )

    # Check if scehdule has been updated according to moratorium
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    last_emi = emis_dict[-1]
    assert last_emi["emi_number"] == 15


def test_refresh_schedule(session: Session) -> None:
    a = User(id=160, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    bill_april = bill_generate(session=session, user_card=uc)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("6000")

    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("4000.5")

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Get emi list post few bill creations
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    pre_emis_dict = [u.as_dict() for u in all_emis_query.all()]

    # Refresh schedule
    refresh_schedule(session, a.id)

    # Get list post refresh
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    post_emis_dict = [u.as_dict() for u in all_emis_query.all()]

    second_emi_pre_dict = pre_emis_dict[1]
    second_emi_post_dict = post_emis_dict[1]
    assert second_emi_pre_dict["interest_received"] == Decimal(360)
    assert second_emi_post_dict["interest_received"] == Decimal(360)


def test_moratorium_schedule(session: Session) -> None:
    a = User(id=160, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    bill_april = bill_generate(session=session, user_card=uc)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("6000")

    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    payment_received(
        session=session,
        user_card=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a123",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.id}/card/lender_payable/l"
    )
    assert lender_payable == Decimal("4000.5")

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Get emi list post few bill creations
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    pre_emis_dict = [u.as_dict() for u in all_emis_query.all()]

    # Give moratorium to user
    m = LoanMoratorium.new(
        session, card_id=uc.id, start_date=parse_date("2020-09-01"), end_date=parse_date("2020-12-01"),
    )

    # Refresh schedule
    refresh_schedule(session, a.id)

    # Get list post refresh
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    post_emis_dict = [u.as_dict() for u in all_emis_query.all()]

    second_emi_pre_dict = pre_emis_dict[1]
    second_emi_post_dict = post_emis_dict[1]
    assert second_emi_pre_dict["interest_received"] == Decimal(360)
    assert second_emi_post_dict["interest_received"] == Decimal(360)


def test_is_in_moratorium(session: Session, monkeypatch: MonkeyPatch) -> None:
    a = User(
        id=38613,
        performed_by=123,
        name="Ananth",
        fullname="Ananth Venkatesh",
        nickname="Ananth",
        email="ananth@redcarpetup.com",
    )
    session.add(a)
    session.flush()

    user_card = create_user_card(
        session,
        user_id=a.id,
        card_type="ruby",
        card_activation_date=parse_date("2020-01-20").date(),
        interest_free_period_in_days=25,
    )

    create_card_swipe(
        session=session,
        user_card=user_card,
        txn_time=parse_date("2020-01-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    # Generate bill
    bill_generate(session=session, user_card=user_card)

    assert (
        LoanMoratorium.is_in_moratorium(
            session, card_id=user_card.id, date_to_check_against=parse_date("2020-02-21")
        )
        is False
    )

    assert user_card.get_min_for_schedule(parse_date("2020-02-01")) == 284

    # Give moratorium
    m = LoanMoratorium.new(
        session,
        card_id=user_card.id,
        start_date=parse_date("2020-01-20"),
        end_date=parse_date("2020-03-20"),
    )

    assert (
        LoanMoratorium.is_in_moratorium(
            session, card_id=user_card.id, date_to_check_against=parse_date("2020-02-21")
        )
        is True
    )

    # Date is outside the moratorium period
    assert (
        LoanMoratorium.is_in_moratorium(
            session, card_id=user_card.id, date_to_check_against=parse_date("2020-03-21")
        )
        is False
    )
    assert user_card.get_min_for_schedule(parse_date("2020-02-01")) == 0  # 0 after moratorium


def test_moratorium_live_user_1836540(session: Session) -> None:
    a = User(
        id=1836540,
        performed_by=123,
        name="Mohammad Shahbaz Mohammad Shafi Qureshi",
        fullname="Mohammad Shahbaz Mohammad Shafi Qureshi",
        nickname="Mohammad Shahbaz Mohammad Shafi Qureshi",
        email="shahbazq797@gmail.com",
    )
    session.add(a)
    session.flush()

    # assign card
    user_card = create_user_card(
        session=session,
        card_type="ruby",
        user_id=a.id,
        # 16th March actual
        card_activation_date=parse_date("2020-03-01").date(),
    )

    # Give moratorium
    m = LoanMoratorium.new(
        session,
        card_id=user_card.id,
        start_date=parse_date("2020-04-01"),
        end_date=parse_date("2020-06-01"),
    )

    create_card_swipe(
        session=session,
        user_card=user_card,
        txn_time=parse_date("2020-03-19 21:33:53"),
        amount=Decimal(10),
        description="TRUEBALANCE IO         GURGAON       IND",
    )

    create_card_swipe(
        session=session,
        user_card=user_card,
        txn_time=parse_date("2020-03-24 14:01:35"),
        amount=Decimal(100),
        description="PAY*TRUEBALANCE IO     GURGAON       IND",
    )

    bill_april = bill_generate(session=session, user_card=user_card)

    create_card_swipe(
        session=session,
        user_card=user_card,
        txn_time=parse_date("2020-04-03 17:41:43"),
        amount=Decimal(4),
        description="TRUEBALANCE IO         GURGAON       IND",
    )

    create_card_swipe(
        session=session,
        user_card=user_card,
        txn_time=parse_date("2020-04-12 22:02:47"),
        amount=Decimal(52),
        description="PAYU PAYMENTS PVT LTD  0001243054000 IND",
    )

    bill_may = bill_generate(session=session, user_card=user_card)

    # bill_june = bill_generate(session=session, user_card=user_card)

    # Get emi list post few bill creations
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
        .all()
    )

    last_emi = all_emis_query[-1]
    out_of_moratorium_emi = all_emis_query[2]
    assert last_emi.emi_number == 15
    assert out_of_moratorium_emi.total_due_amount == Decimal("22.99")
