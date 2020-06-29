import contextlib
import datetime
import json
from decimal import Decimal
from io import StringIO

import alembic
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    accrue_interest_on_all_bills,
    accrue_late_charges,
)
from rush.anomaly_detection import run_anomaly
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import (  # get_interest_for_each_bill,
    get_account_balance_from_str,
    get_all_unpaid_bills,
    is_bill_closed,
)
from rush.lender_funds import (
    lender_disbursal,
    lender_interest_incur,
    m2p_transfer,
)
from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    User,
    UserCard,
    UserPy,
)
from rush.payments import (
    payment_received,
    refund_payment,
    writeoff_payment,
)
from rush.utils import get_current_ist_time
from rush.views import (
    bill_view,
    transaction_view,
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
    uc = UserCard(user_id=2, card_activation_date=parse_date("2020-05-01"))
    session.add(uc)
    session.flush()
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


def test_generate_bill_1(session: Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.add(uc)
    session.flush()

    user_card_id = uc.id

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

    assert bill.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 1000

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == Decimal("113.33")


def _partial_payment_bill_1(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(100)
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    payment_received(
        session=session, user_card=user_card, payment_amount=amount, payment_date=payment_date,
    )

    bill = unpaid_bills[0]
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == 1000 - amount

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("13.33")


def _partial_payment_bill_2(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    payment_received(
        session=session, user_card=user_card, payment_amount=amount, payment_date=payment_date,
    )

    bill = unpaid_bills[0]
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == 2000 - amount

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("0")


def _min_payment_delayed_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(130)
    bill = payment_received(
        session=session, user_id=user.id, payment_amount=amount, payment_date=payment_date,
    )

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == 970


def test_partial_payment_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)


def _accrue_late_fine_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    bill = accrue_late_charges(session, user.id)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(100)

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("113.33")


def _accrue_late_fine_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    bill = accrue_late_charges(session, user.id)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(100)

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("270")


def test_accrue_late_fine_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    # did only partial payment so accrue late fee.
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)


def _pay_minimum_amount_bill_1(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()

    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)

    # Pay 13.33 more. and 100 for late fee.
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal("113.33"),
        payment_date=parse_date("2020-05-20"),
    )
    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == Decimal("886.67")


def test_is_min_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    # did only partial payment so accrue late fee.
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)


def _accrue_interest_bill_1(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    bill = unpaid_bills[0]
    accrue_interest_on_all_bills(session, bill.agreement_date, user_card)
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 30


def test_accrue_interest_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _accrue_interest_bill_1(session)


def test_is_bill_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _accrue_interest_bill_1(session)

    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_card.user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    # Should be false because min is 130 and payment made is 120
    is_it_paid = is_bill_closed(session, bill)
    assert is_it_paid is False

    # Need to pay 870 more to close the bill. 30 more interest.
    remaining_principal = Decimal("916.67")
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=remaining_principal,
        payment_date=parse_date("2020-05-05"),
    )
    is_it_paid_now = is_bill_closed(session, bill)
    assert is_it_paid_now is True


def _generate_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = session.query(UserCard).filter_by(user_id=user.id).one()

    previous_bill = (  # new bill isn't generated yet so get latest.
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
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

    bill = bill_generate(session=session, user_card=uc)

    unpaid_bills = get_all_unpaid_bills(session, user.id)
    assert len(unpaid_bills) == 2

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(2000)

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("226.67")

    first_bill = unpaid_bills[0]
    first_bill_min_due = first_bill.get_minimum_amount_to_pay(session)
    assert first_bill_min_due == Decimal("113.33")


def _generate_bill_3(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = session.query(UserCard).filter_by(user_id=user.id).one()

    previous_bill = (  # new bill isn't generated yet so get latest.
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
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

    unpaid_bills = get_all_unpaid_bills(session, user.id)
    assert len(unpaid_bills) == 2

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(1000)

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("113.33")


def _run_anomaly_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
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
    _accrue_interest_bill_1(session)
    _generate_bill_2(session)


def test_generate_bill_3(session: Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.flush()
    session.add(uc)

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


def _pay_minimum_amount_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )

    # Pay 10 more. and 100 for late fee.
    bill = payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal(110),
        payment_date=parse_date("2020-06-20"),
    )
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
    user = session.query(User).filter(User.id == 99).one()
    value = 1.1
    print(value)

    user_bill = user_view(session, user.id)
    assert user_bill["current_bill_balance"] == Decimal(1490)  # 1490 to pay since 110 payment was made
    assert user_bill["current_bill_interest"] == Decimal(0)  # 100 interest
    assert user_bill["min_to_pay"] == Decimal(0)  # sum of all interest and fines
    bill_details = bill_view(session, user.id)
    assert bill_details[0]["transactions"][0]["amount"] == Decimal(1500)
    bill = session.query(LoanData).filter(LoanData.user_id == user.id).first()
    transactions = transaction_view(session, bill_id=bill.id)
    assert transactions[0]["amount"] == Decimal(1500)


def test_refund_1(session: Session) -> None:
    test_generate_bill_1(session)
    _generate_bill_3(session)
    user = session.query(User).filter(User.id == 99).one()
    unpaid_bills = get_all_unpaid_bills(session, user.id)

    status = refund_payment(session, 99, unpaid_bills[0].id)
    assert status == True
    _, amount = get_account_balance_from_str(session, book_string=f"62311/lender/merchant_refund/a")
    assert amount == Decimal("1000")  # 1000 refunded


def test_refund_2(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _generate_bill_3(session)
    user = session.query(User).filter(User.id == 99).one()
    unpaid_bills = get_all_unpaid_bills(session, user.id)

    status = refund_payment(session, 99, unpaid_bills[0].id)
    assert status == True
    _, amount = get_account_balance_from_str(session, book_string=f"62311/lender/merchant_refund/a")
    assert amount == Decimal("1100")  # 1100 refunded


def test_lender_incur(session: Session) -> None:
    test_refund_1(session)
    status = lender_interest_incur(session)
    uc = session.query(UserCard).filter(UserCard.user_id == 99).one()
    _, amount = get_account_balance_from_str(session, book_string=f"{uc.id}/card/lender_payable/l")
    assert amount == Decimal("2054.74")  # on date 2020-06-28


def test_prepayment(session: Session) -> None:
    test_generate_bill_1(session)
    uc = session.query(UserCard).filter(UserCard.user_id == 99).one()
    user_card_id = uc.id

    # prepayment of rs 2000 done
    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    payment_received(
        session=session, user_card=uc, payment_amount=amount, payment_date=payment_date,
    )

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )
    bill_id = swipe.loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(session=session, user_card=uc)

    assert bill.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 0  # since there is prepayment of 2000rs


def test_writeoff_recovery(session: Session) -> None:
    status = writeoff_payment(session, 99)
    assert status == False

    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-03-02"))
    session.add(uc)
    session.flush()

    user_card_id = uc.id

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-03-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )
    bill_id = swipe.loan_id
    bill = bill_generate(session=session, user_card=uc)
    assert bill.is_generated is True

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1500),
        description="BigBasket.com",
    )
    bill_id = swipe.loan_id
    bill = bill_generate(session=session, user_card=uc)
    assert bill.is_generated is True

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(1200),
        description="BigBasket.com",
    )
    bill_id = swipe.loan_id
    bill = bill_generate(session=session, user_card=uc)
    assert bill.is_generated is True
    unpaid_bills = get_all_unpaid_bills(session, 99)
    bill = unpaid_bills[0]

    accrue_interest_on_all_bills(session, bill.agreement_date, uc)
    accrue_late_charges(session, 99)
    status = writeoff_payment(session, 99)
    assert status == True

    _, writeoff_amount = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/lender_expenses/e"
    )
    assert writeoff_amount == Decimal("3911")

    payment_date = parse_date("2020-05-03")
    payment_received(session, uc, Decimal("3911"), payment_date)

    _, writeoff_amount = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/lender_expenses/e"
    )
    assert writeoff_amount == Decimal("0")
    user_info = user_view(session, 99)
    assert user_info["max_to_pay"] == 0  # 0 since all bills are paid.
