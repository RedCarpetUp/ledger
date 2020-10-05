import contextlib
from decimal import Decimal
from io import StringIO

import alembic
from _pytest.monkeypatch import MonkeyPatch
from alembic.command import current as alembic_current
from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    accrue_interest_on_all_bills,
    accrue_late_charges,
)
from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.create_bill import (
    bill_generate,
    extend_tenure,
)
from rush.create_card_swipe import create_card_swipe
from rush.create_emi import (
    check_moratorium_eligibility,
    update_event_with_dpd,
)
from rush.ledger_utils import (
    get_account_balance_from_str,
    is_bill_closed,
)
from rush.lender_funds import (
    lender_disbursal,
    lender_interest_incur,
    m2p_transfer,
)
from rush.loan_schedule.extension import extend_schedule
from rush.loan_schedule.moratorium import provide_moratorium
from rush.models import (
    BillFee,
    CardEmis,
    CardKitNumbers,
    CardNames,
    CardTransaction,
    EmiPaymentMapping,
    EventDpd,
    Fee,
    LedgerTriggerEvent,
    LenderPy,
    Lenders,
    LoanData,
    LoanMoratorium,
    PaymentMapping,
    PaymentSplit,
    Product,
    User,
    UserPy,
)
from rush.payments import (
    payment_received,
    refund_payment,
)
from rush.recon.revenue_earned import get_revenue_earned_in_a_period


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


def create_products(session: Session) -> None:
    ruby_product = Product(product_name="ruby")
    session.add(ruby_product)
    session.flush()


def card_db_updates(session: Session) -> None:
    create_products(session=session)

    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()
    ckn = CardKitNumbers(kit_number="00000", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()

    ckn = CardKitNumbers(kit_number="11111", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()


def test_user2(session: Session) -> None:
    # u = User(performed_by=123, id=1, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    u = User(
        id=1,
        performed_by=123,
    )
    session.add(u)
    session.commit()
    a = session.query(User).first()
    u = UserPy(
        id=a.id,
        performed_by=123,
        email="sss",
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
    )


def test_user(session: Session) -> None:
    # u = User(id=2, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    u = User(
        id=2,
        performed_by=123,
    )
    session.add(u)
    session.commit()
    a = session.query(User).first()
    u = UserPy(
        id=a.id,
        performed_by=123,
        email="sss",
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
    )


def test_lenders(session: Session) -> None:
    l1 = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(l1)
    l2 = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(l2)
    session.flush()
    a = session.query(Lenders).first()
    u = LenderPy(id=a.id, performed_by=123, lender_name="DMI", row_status="active")


def test_lender_disbursal(session: Session) -> None:
    test_lenders(session)
    resp = lender_disbursal(session, 100000, 62311)
    assert resp["result"] == "success"
    # _, lender_capital_balance = get_account_balance_from_str(session, "62311/lender/lender_capital/l")
    # assert lender_capital_balance == Decimal(100000)


def test_m2p_transfer(session: Session) -> None:
    test_lenders(session)
    resp = m2p_transfer(session, 50000, 62311)
    assert resp["result"] == "success"

    # _, lender_pool_balance = get_account_balance_from_str(session, "62311/lender/pool_balance/a")
    # assert lender_pool_balance == Decimal(50000)


def test_card_swipe(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    uc = create_user_product(
        session=session,
        user_id=2,
        card_activation_date=parse_date("2020-05-01").date(),
        card_type="ruby",
        lender_id=62311,
    )

    swipe1 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    swipe1 = swipe1["data"]

    swipe2 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )
    swipe2 = swipe2["data"]

    assert swipe1.loan_id == swipe2.loan_id
    bill_id = swipe1.loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{bill_id}/bill/unbilled/a")
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've not loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/available_limit/l")
    assert card_balance == -900

    _, lender_payable = get_account_balance_from_str(session, f"{uc.loan_id}/loan/lender_payable/l")
    assert lender_payable == 900


def test_closing_bill(session: Session) -> None:
    # Replicating nishant's case upto June
    test_lenders(session)
    card_db_updates(session)

    a = User(
        id=230,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2019-01-02").date(),
        card_type="ruby",
        lender_id=62311,
    )

    bill_date = parse_date("2019-02-01 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2019-02-02 19:23:11"),
        amount=Decimal(3000),
        description="BigB.com",
    )

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    bill_date = parse_date("2019-03-01 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    event_date = parse_date("2019-03-15 12:00:00")
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(100))

    payment_date = parse_date("2019-03-27")

    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal(463),
        payment_date=payment_date,
        payment_request_id="a1234",
    )

    bill_date = parse_date("2019-04-01 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    payment_date = parse_date("2019-04-15")

    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal(363),
        payment_date=payment_date,
        payment_request_id="a1235",
    )

    bill_date = parse_date("2019-05-01 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    payment_date = parse_date("2019-05-16")

    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal(2545),
        payment_date=payment_date,
        payment_request_id="a1236",
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2019-05-20 19:23:11"),
        amount=Decimal(3000),
        description="BigB.com",
    )

    bill_date = parse_date("2019-06-01 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    event_date = parse_date("2019-06-15 12:00:00")
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(120))


def test_generate_bill_1(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=99,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
        lender_id=62311,
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    user_loan = get_user_product(session, a.id)
    bill = bill_generate(user_loan=user_loan)
    # Interest event to be fired separately now

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

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

    update_event_with_dpd(user_loan=user_loan, post_date=parse_date("2020-05-21 00:05:00"))

    dpd_events = session.query(EventDpd).filter_by(loan_id=uc.loan_id).all()
    assert dpd_events[0].balance == Decimal(1000)

    emis = uc.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal(114)
    assert emis[0].principal_due == Decimal("83.33")
    assert emis[0].interest_due == Decimal("30.67")
    assert emis[0].due_date == parse_date("2020-05-15").date()
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(1000)
    assert emis[1].total_closing_balance == Decimal("916.67")
    assert emis[11].total_closing_balance == Decimal("83.33")


def _accrue_interest_on_bill_1(session: Session) -> None:
    user_loan = get_user_product(session, 99)
    bill = user_loan.get_all_bills()[0]
    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("30.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("30.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(loan_id=user_loan.loan_id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event is not None


def test_min_multiplier(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=99,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
        lender_id=62311,
        min_multiplier=Decimal(2),
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(12000),
        description="BigB.com",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 12000

    bill = bill_generate(user_loan=user_loan)
    # Interest event to be fired separately now

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    assert bill.bill_start_date == parse_date("2020-04-02").date()
    assert bill.table.is_generated is True

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 2720

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 12000


def test_min_tenure(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=99,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
        lender_id=62311,
        min_tenure=24,
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(12000),
        description="BigB.com",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 12000

    bill = bill_generate(user_loan=user_loan)
    # Interest event to be fired separately now

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    assert bill.bill_start_date == parse_date("2020-04-02").date()
    assert bill.table.is_generated is True

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == 860

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 12000


def _partial_payment_bill_1(session: Session) -> None:
    user_loan = get_user_product(session, 99)
    payment_date = parse_date("2020-05-03")
    amount = Decimal(100)
    unpaid_bills = user_loan.get_unpaid_bills()
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1000")
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a1237",
    )

    bill = unpaid_bills[0]
    _, gateway_expenses = get_account_balance_from_str(
        session, book_string="12345/redcarpet/gateway_expenses/e"
    )
    assert gateway_expenses == 0.5

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal("900")

    min_due = bill.get_remaining_min()
    assert min_due == 14

    min_due = user_loan.get_remaining_min()
    assert min_due == 14

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_received == Decimal("100")
    assert emis[0].payment_status == "UnPaid"
    assert emis[0].emi_number == 1

    # Check the entry in payment schedule mapping.
    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "a1237", PaymentMapping.row_status == "active")
        .all()
    )
    assert len(pm) == 1
    assert pm[0].emi_id == emis[0].id
    assert pm[0].amount_settled == Decimal("100")

    # Check the entries for payment split
    payment_splits = session.query(PaymentSplit).filter(PaymentSplit.payment_request_id == "a1237").all()
    assert len(payment_splits) == 1
    split = {ps.component: ps.amount_settled for ps in payment_splits}
    assert split["principal"] == Decimal("100")


def test_partial_payment_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)


def _accrue_late_fine_bill_1(session: Session) -> None:
    user_loan = get_user_product(session, 99)
    event_date = parse_date("2020-05-16 00:00:00")
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(118))

    fee_due = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "late_fee")
        .one_or_none()
    )
    assert fee_due.net_amount == Decimal(100)
    assert fee_due.gross_amount == Decimal(118)

    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]
    first_emi = emis_dict[0]

    assert first_emi["late_fee"] == Decimal(118)

    min_due = bill.get_remaining_min()
    assert min_due == 132


def _accrue_late_fine_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    event_date = parse_date("2020-05-16 00:00:00")
    user_loan = get_user_product(session, 99)
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(118))

    fee_due = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "late_fee")
        .order_by(BillFee.id.desc())
        .one_or_none()
    )
    assert fee_due.net_amount == Decimal(100)
    assert fee_due.gross_amount == Decimal(118)

    min_due = bill.get_remaining_min()
    assert min_due == Decimal("288")


def test_accrue_late_fine_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    # did only partial payment so accrue late fee.
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)


def _pay_minimum_amount_bill_1(session: Session) -> None:
    user_loan = get_user_product(session, 99)

    unpaid_bills = user_loan.get_unpaid_bills()

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    bill = unpaid_bills[0]

    fee_id = (
        session.query(BillFee.id)
        .filter(
            BillFee.identifier_id == bill.id, BillFee.name == "late_fee", BillFee.fee_status == "UNPAID"
        )
        .scalar()
    )
    # Pay 13.33 more. and 118 for late fee.
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal("132"),
        payment_date=parse_date("2020-05-20"),
        payment_request_id="a1238",
    )
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_remaining_min()
    assert min_due == Decimal(0)

    bill_fee = session.query(Fee).filter_by(id=fee_id).one_or_none()
    assert bill_fee.fee_status == "PAID"
    assert bill_fee.net_amount_paid == Decimal(100)
    assert bill_fee.igst_paid == Decimal(18)
    assert bill_fee.gross_amount_paid == Decimal(118)

    _, late_fine_earned = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_earned == Decimal(100)

    _, igst_balance = get_account_balance_from_str(session, "12345/redcarpet/igst_payable/l")
    assert igst_balance == Decimal(18)

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("16.67")

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 118 rupees got settled in late fine.
    assert principal_due == Decimal("900")

    _, pg_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert pg_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("769.0")


def test_accrue_interest_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)


def test_late_fee_reversal_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)

    user_loan = get_user_product(session, 99)

    unpaid_bills = user_loan.get_unpaid_bills()

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    # Pay 13.33 more. and 100 for late fee.
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal("132"),
        # Payment came before the due date.
        payment_date=parse_date("2020-06-14"),
        payment_request_id="a1239",
    )
    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_remaining_min()
    assert min_due == Decimal(0)

    fee_due = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.name == "late_fee")
        .one_or_none()
    )
    assert fee_due.fee_status == "PAID"

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_due == Decimal("100")

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    # changed from 916 to 816, the late did not get settled.
    assert principal_due == Decimal("900")

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("769.0")

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_received == Decimal("114")
    assert emis[0].payment_status == "Paid"
    assert emis[0].emi_number == 1
    assert emis[1].emi_number == 2
    assert emis[1].payment_status == "UnPaid"
    assert emis[1].payment_received == Decimal("0")

    # Check the entry in payment schedule mapping.
    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "a1239", PaymentMapping.row_status == "active")
        .order_by(PaymentMapping.id)
        .all()
    )
    assert len(pm) == 1
    assert pm[0].emi_id == emis[0].id
    assert pm[0].amount_settled == Decimal("14")

    payment_splits = session.query(PaymentSplit).filter(PaymentSplit.payment_request_id == "a1239").all()
    assert len(payment_splits) == 3
    split = {ps.component: ps.amount_settled for ps in payment_splits}
    assert split["late_fine"] == Decimal("100")
    assert split["igst"] == Decimal("18")
    assert split["interest"] == Decimal("14")


def test_is_bill_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    user_loan = get_user_product(session, 99)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_loan.user_id)
        .order_by(LoanData.bill_start_date.desc())
        .first()
    )
    # Should be false because min is 130 and payment made is 120
    is_it_paid = is_bill_closed(session, bill)
    assert is_it_paid is False
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("769")

    # Need to pay 916.67 more to close the bill.
    remaining_principal = Decimal("916.67")
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=remaining_principal,
        payment_date=parse_date("2020-05-25"),
        payment_request_id="a12310",
    )
    is_it_paid_now = is_bill_closed(session, bill)
    assert is_it_paid_now is True

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-147.17")  # negative that implies prepaid

    emis = user_loan.get_loan_schedule()
    assert emis[1].payment_received == remaining_principal
    assert emis[1].payment_status == "Paid"
    assert emis[1].principal_due == remaining_principal
    assert emis[2].principal_due == Decimal(0)


def _generate_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = get_user_product(session, 99)

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
        user_loan=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(2000),
        description="BigBasket.com",
    )

    _, user_loan_balance = get_account_balance_from_str(
        session=session, book_string=f"{uc.loan_id}/card/available_limit/a"
    )
    assert user_loan_balance == Decimal(-3000)

    bill_2 = bill_generate(user_loan=uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_2.table.bill_due_date + relativedelta(days=1), uc)
    assert bill_2.bill_start_date == parse_date("2020-05-01").date()

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
    assert interest_due == Decimal("47.34")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_accrued/r"
    )
    assert interest_due == Decimal("61.34")

    total_revenue_earned = get_revenue_earned_in_a_period(
        session, parse_date("2020-05-01").date(), parse_date("2020-05-31").date()
    )
    assert total_revenue_earned == Decimal("114.00")
    total_revenue_earned = get_revenue_earned_in_a_period(
        session, parse_date("2020-06-01").date(), parse_date("2020-06-30").date()
    )
    assert total_revenue_earned == Decimal("0")

    emis = uc.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal(114)
    assert emis[0].principal_due == Decimal("83.33")
    assert emis[0].interest_due == Decimal("30.67")
    assert emis[0].due_date == parse_date("2020-05-15").date()
    assert emis[0].emi_number == 1

    assert emis[1].total_due_amount == Decimal(341)
    assert emis[12].total_due_amount == Decimal(227)
    assert emis[1].principal_due == Decimal("250")
    assert emis[1].interest_due == Decimal("91")
    assert emis[1].due_date == parse_date("2020-06-15").date()
    assert emis[1].emi_number == 2
    assert len(emis) == 13


def test_generate_bill_2(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)


def test_generate_bill_3(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=99,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 20:23:11"),
        amount=Decimal(1500),
        description="Flipkart.com",
    )

    generate_date = parse_date("2020-06-01").date()
    user_loan = get_user_product(session, a.id)
    bill = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), user_loan)

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
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=108, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=108,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    user_loan = get_user_product(session, a.id)
    # Generate bill
    bill_april = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    all_emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )  # Get the latest emi of that user.

    last_emi = all_emis[11]
    assert last_emi.emi_number == 12


def test_subsequent_emi_creation(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=160, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=160,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    user_loan = get_user_product(session, a.id)
    bill_april = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_may.table.bill_due_date + relativedelta(days=1), user_loan
    )

    all_emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
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
    assert last_emi.due_date.strftime("%Y-%m-%d") == "2021-05-15"


def test_schedule_for_interest_and_payment(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=1991, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=1991,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-05-01").date(),
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    user_loan = get_user_product(session, a.id)
    bill_may = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_may.table.bill_due_date + relativedelta(days=1), user_loan
    )

    # Check calculated interest
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_may.id}/bill/interest_receivable/a"
    )
    assert interest_due == 180

    # Check if emi is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]
    first_emi = emis_dict[0]
    assert first_emi["interest_current_month"] == 90
    assert first_emi["interest_next_month"] == 90

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("6000")

    # Do Full Payment
    payment_date = parse_date("2020-07-30")
    amount = Decimal(6360)
    bill = payment_received(
        session=session,
        user_loan=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12311",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-359.5")

    # Refresh Schedule
    # slide_payments(session, a.id)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    assert emis_dict[0]["due_date"] == parse_date("2020-06-15").date()
    assert emis_dict[0]["total_due_amount"] == 680
    assert emis_dict[0]["due_amount"] == 500
    assert emis_dict[0]["total_closing_balance"] == 6000
    assert emis_dict[0]["total_closing_balance_post_due_date"] == 6180
    assert emis_dict[0]["interest_received"] == 180
    assert emis_dict[0]["payment_received"] == 500
    assert emis_dict[0]["interest"] == 180
    assert emis_dict[0]["interest_current_month"] == 90
    assert emis_dict[0]["interest_next_month"] == 90
    assert emis_dict[1]["due_date"] == parse_date("2020-07-15").date()
    assert emis_dict[1]["total_due_amount"] == 680
    assert emis_dict[1]["due_amount"] == 500
    assert emis_dict[1]["total_closing_balance"] == 5500
    assert emis_dict[1]["total_closing_balance_post_due_date"] == 5680
    assert emis_dict[1]["interest_received"] == 180
    assert emis_dict[1]["payment_received"] == 500
    assert emis_dict[1]["interest"] == 180
    assert emis_dict[1]["interest_current_month"] == 90
    assert emis_dict[1]["interest_next_month"] == 90
    assert emis_dict[2]["due_date"] == parse_date("2020-08-15").date()
    assert emis_dict[2]["total_due_amount"] == 5000
    assert emis_dict[2]["due_amount"] == 5000
    assert emis_dict[2]["total_closing_balance"] == 0
    assert emis_dict[2]["total_closing_balance_post_due_date"] == 0
    assert emis_dict[2]["interest_received"] == 0
    assert emis_dict[2]["payment_received"] == 5000
    assert emis_dict[2]["interest"] == 0
    assert emis_dict[2]["interest_current_month"] == 0
    assert emis_dict[2]["interest_next_month"] == 0
    assert emis_dict[3]["due_date"] == parse_date("2020-09-15").date()
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
    test_lenders(session)
    card_db_updates(session)
    # a = User(
    #     id=1764433,
    #     performed_by=123,
    #     name="UPENDRA",
    #     fullname="UPENDRA SINGH",
    #     nickname="UPENDRA",
    #     email="upsigh921067@gmail.com",
    # )
    a = User(
        id=1764433,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-05-04").date(),
        lender_id=62311,
    )

    # Card transactions
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-20 17:23:01"),
        amount=Decimal(129),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-22 09:33:18"),
        amount=Decimal(115),
        description="TPL*UDIO               MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-22 09:50:46"),
        amount=Decimal(500),
        description="AIRTELMONEY            MUMBAI        IND",
    )
    refunded_swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-22 12:50:05"),
        amount=Decimal(2),
        description="PHONEPE RECHARGE.      GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 01:18:54"),
        amount=Decimal(100),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 01:42:51"),
        amount=Decimal(54),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 01:49:44"),
        amount=Decimal(1100),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 13:12:33"),
        amount=Decimal(99),
        description="ULLU DIGITAL PRIVATE L MUMBAI        IND",
    )

    # Merchant Refund
    refund_date = parse_date("2020-05-23 21:20:07")
    amount = Decimal(2)
    refund_payment(session, uc, amount, refund_date, "A3d223g2")

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-24 22:09:42"),
        amount=Decimal(99),
        description="PayTM*KookuDigitalPriP Mumbai        IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 08:33:40"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 10:26:12"),
        amount=Decimal(380),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 11:40:05"),
        amount=Decimal(199),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 11:57:15"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 12:25:57"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 08:04:47"),
        amount=Decimal(1450),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 14:47:41"),
        amount=Decimal(110),
        description="TPL*UDIO               MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 16:37:27"),
        amount=Decimal(700),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    one_sixty_rupee = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 22:10:58"),
        amount=Decimal(160),
        description="Linkyun Technology Pri Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-27 12:25:25"),
        amount=Decimal(299),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-28 20:38:02"),
        amount=Decimal(199),
        description="Linkyun Technology Pri Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-28 21:45:55"),
        amount=Decimal(800),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-29 10:05:58"),
        amount=Decimal(525),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-30 16:04:21"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    # Generate bill
    bill_may = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Check for atm fee.
    atm_fee_due = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill_may.id, BillFee.name == "atm_fee")
        .one_or_none()
    )
    assert atm_fee_due.gross_amount == 50

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-03 13:20:40"),
        amount=Decimal("150"),
        description="JUNE",
    )
    one_rupee_1 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 17:09:57"),
        amount=Decimal("1"),
        description="JUNE",
    )
    one_rupee_2 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 17:12:01"),
        amount=Decimal("1"),
        description="JUNE",
    )
    one_rupee_3 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 17:26:54"),
        amount=Decimal("1"),
        description="JUNE",
    )
    one_rupee_4 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 18:02:08"),
        amount=Decimal("1"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-08 20:03:37"),
        amount=Decimal("281.52"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 14:58:57"),
        amount=Decimal("810"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 15:02:50"),
        amount=Decimal("939.96"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 15:43:12"),
        amount=Decimal("240.54"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 15:51:18"),
        amount=Decimal("240.08"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 09:37:59"),
        amount=Decimal("10"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 15:21:01"),
        amount=Decimal("1700.84"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 23:27:06"),
        amount=Decimal("273.39"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 23:31:55"),
        amount=Decimal("273.39"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-12 17:11:11"),
        amount=Decimal("1254.63"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-13 11:59:50"),
        amount=Decimal("281.52"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-13 12:06:56"),
        amount=Decimal("281.52"),
        description="JUNE",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-13 12:17:49"),
        amount=Decimal("1340.64"),
        description="JUNE",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_may.table.bill_due_date + relativedelta(days=1), uc)

    # Merchant Refund
    refund_date = parse_date("2020-06-16 01:48:05")
    amount = Decimal(160)
    refund_payment(session, uc, amount, refund_date, "A3d223g3")
    # Merchant Refund
    refund_date = parse_date("2020-06-17 00:21:23")
    amount = Decimal(160)
    refund_payment(session, uc, amount, refund_date, "A3d223g4")
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:54:58")
    amount = Decimal(1)
    refund_payment(session, uc, amount, refund_date, "A3d223g5")
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:54:59")
    amount = Decimal(1)
    refund_payment(session, uc, amount, refund_date, "A3d223g6")
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:54:59")
    amount = Decimal(1)
    refund_payment(session, uc, amount, refund_date, "A3d223g7")
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:55:00")
    amount = Decimal(1)
    refund_payment(session, uc, amount, refund_date, "A3d223g8")

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("20672.03")

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")

    assert uc.get_remaining_max() == Decimal("13027.83")
    assert uc.get_total_outstanding() == Decimal("21109.86")

    bill_june = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_june.table.bill_due_date + relativedelta(days=1), uc)

    bill_july = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Do Partial Payment
    payment_date = parse_date("2020-08-02 14:25:52")
    amount = Decimal(1)
    payment_received(
        session=session,
        user_loan=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12312",
    )
    # Do Partial Payment
    payment_date = parse_date("2020-08-02 14:11:06")
    amount = Decimal(1139)
    payment_received(
        session=session,
        user_loan=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12313",
    )

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]
    first_emi = emis_dict[0]

    assert first_emi["interest"] == Decimal("387.83")
    assert first_emi["atm_fee"] == Decimal(50)
    assert first_emi["interest_received"] == Decimal("387.83")

    event_date = parse_date("2020-08-21 00:05:00")
    update_event_with_dpd(uc, event_date)

    dpd_events = session.query(EventDpd).filter_by(loan_id=uc.loan_id).all()

    last_entry_first_bill = dpd_events[-1]
    last_entry_second_bill = dpd_events[-2]

    assert last_entry_first_bill.balance == Decimal("12708.86")
    assert last_entry_second_bill.balance == Decimal("7891.33")

    _, bill_may_principal_due = get_account_balance_from_str(
        session, book_string=f"{bill_may.id}/bill/principal_receivable/a"
    )
    _, bill_june_principal_due = get_account_balance_from_str(
        session, book_string=f"{bill_june.id}/bill/principal_receivable/a"
    )
    assert bill_may_principal_due == Decimal("12708.86")
    assert bill_june_principal_due == Decimal("7891.33")


def test_interest_reversal_interest_already_settled(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    user_loan = get_user_product(session, 99)

    # Pay min amount before interest is accrued.
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal("132"),
        payment_date=parse_date("2020-05-05 19:23:11"),
        payment_request_id="aasdf123",
    )

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_received == Decimal("114")
    assert emis[0].payment_status == "Paid"
    assert emis[0].emi_number == 1
    assert emis[1].emi_number == 2
    assert emis[1].payment_status == "Paid"
    assert emis[1].payment_received == Decimal("114")
    assert emis[2].emi_number == 3
    assert emis[2].payment_status == "UnPaid"
    assert emis[2].payment_received == Decimal("4")

    # Check the entry in payment mapping.
    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "aasdf123", PaymentMapping.row_status == "active")
        .order_by(PaymentMapping.id)
        .all()
    )
    assert len(pm) == 3
    assert pm[0].emi_id == emis[0].id
    assert pm[0].amount_settled == Decimal("14")
    assert pm[1].emi_id == emis[1].id
    assert pm[1].amount_settled == Decimal("114")
    assert pm[2].emi_id == emis[2].id
    assert pm[2].amount_settled == Decimal("4")

    # Accrue interest.
    _accrue_interest_on_bill_1(session)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("769")

    payment_date = parse_date("2020-05-14 19:23:11")
    amount = Decimal("786")
    unpaid_bills = user_loan.get_unpaid_bills()
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12314",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-16.5")

    bill = unpaid_bills[0]

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 0

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_accrued/r"
    )
    assert interest_earned == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(0)


def test_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    #  Pay 500 rupees
    user_loan = get_user_product(session, 99)
    payment_date = parse_date("2020-06-14 19:23:11")
    amount = Decimal("3008.34")
    unpaid_bills = user_loan.get_unpaid_bills()
    first_bill = unpaid_bills[0]
    second_bill = unpaid_bills[1]

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_accrued/r"
    )
    assert interest_earned == Decimal("61.34")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_accrued/r"
    )
    assert interest_earned == Decimal("60.33")

    # Get emi list post few bill creations
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )

    second_emi = all_emis_query[1]
    assert second_emi.interest == 91

    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12315",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-238.84")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_accrued/r"
    )
    # 30.67 Interest got removed from first bill.
    assert interest_earned == Decimal("30.67")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_accrued/r"
    )
    assert interest_earned == Decimal(0)

    # Get emi list post few bill creations
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )

    second_emi = all_emis_query[1]
    assert second_emi.interest == 0

    assert is_bill_closed(session, first_bill) is True
    # 90 got settled in new bill.
    assert is_bill_closed(session, second_bill) is True


def test_failed_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    user_loan = get_user_product(session, 99)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("2769")

    payment_date = parse_date(
        "2020-06-18 19:23:11"
    )  # Payment came after due date. Interest won't get reversed.
    amount = Decimal("2916.67")
    unpaid_bills = user_loan.get_unpaid_bills()
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12316",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-147.17")

    first_bill = unpaid_bills[0]
    second_bill = unpaid_bills[1]

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_accrued/r"
    )
    # 30 Interest did not get removed.
    assert interest_earned == Decimal("61.34")

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_accrued/r"
    )
    assert interest_earned == Decimal("60.33")
    assert is_bill_closed(session, first_bill) is True
    assert is_bill_closed(session, second_bill) is False


def _pay_minimum_amount_bill_2(session: Session) -> None:
    user_loan = get_user_product(session, 99)

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1500")

    # Pay 10 more. and 100 for late fee.
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal(110),
        payment_date=parse_date("2020-06-20"),
        payment_request_id="a12317",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1390.5")

    balance_paid = (
        session.query(LedgerTriggerEvent)
        .order_by(LedgerTriggerEvent.post_date.desc())
        .filter(LedgerTriggerEvent.name == "payment_received")
        .first()
    )
    assert balance_paid.amount == Decimal(110)


def test_refund_1(session: Session) -> None:
    test_generate_bill_1(session)
    _accrue_interest_on_bill_1(session)
    user_loan = get_user_product(session, 99)

    refund_payment(session, user_loan, 100, parse_date("2020-05-05 15:24:34"), "asd23g2")

    _, merchant_refund_off_balance = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/refund_off_balance/l"
    )
    assert merchant_refund_off_balance == Decimal("100")  # 1000 refunded with interest 60

    # Test same month refund.
    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-10 19:23:11"),
        amount=Decimal(1500),
        description="BigBB.com",
    )
    refund_payment(session, user_loan, 1500, parse_date("2020-05-15 15:24:34"), "af423g2")

    _, merchant_refund_off_balance = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/refund_off_balance/l"
    )
    assert merchant_refund_off_balance == Decimal("1600")  # 1000 refunded with interest 60


def test_lender_incur(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas")
    a = User(
        id=99,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
        lender_id=62311,
    )
    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )
    bill_id = swipe["data"].loan_id
    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000
    user_loan = get_user_product(session, a.id)
    bill = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), user_loan)
    assert bill.table.is_generated is True

    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-29 19:23:11"),
        amount=Decimal(1500),
        description="BigBasket.com",
    )
    bill = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), user_loan)
    assert bill.table.is_generated is True

    lender_interest_incur(
        session, from_date=parse_date("2020-06-01").date(), to_date=parse_date("2020-06-30").date()
    )
    _, amount = get_account_balance_from_str(session, book_string=f"{uc.loan_id}/loan/lender_payable/l")
    assert amount == Decimal("2511.65")

    _, amount = get_account_balance_from_str(session, book_string=f"{uc.loan_id}/loan/lender_interest/e")
    assert amount == Decimal("11.65")

    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-07-29 19:23:11"),
        amount=Decimal(1500),
        description="Flipkart.com",
    )
    lender_interest_incur(
        session, from_date=parse_date("2020-07-01").date(), to_date=parse_date("2020-07-31").date()
    )
    _, amount = get_account_balance_from_str(session, book_string=f"{uc.loan_id}/loan/lender_interest/e")
    assert amount == Decimal("51.81")
    _, amount = get_account_balance_from_str(session, book_string=f"{uc.loan_id}/loan/lender_payable/l")
    assert amount == Decimal("4051.81")


def test_lender_incur_two(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas")
    a = User(
        id=99,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        card_type="ruby",
        lender_id=62311,
    )
    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-07-29 19:23:11"),
        amount=Decimal(500),
        description="BigBasket.com",
    )
    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-07-29 10:23:11"),
        amount=Decimal(500),
        description="BigBasket.com",
    )
    user_loan = get_user_product(session, a.id)
    bill = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), user_loan)
    assert bill.table.is_generated is True

    lender_interest_incur(
        session, from_date=parse_date("2020-07-01").date(), to_date=parse_date("2020-07-31").date()
    )
    _, amount = get_account_balance_from_str(session, book_string=f"{uc.loan_id}/loan/lender_payable/l")
    assert amount == Decimal("1000.99")

    _, amount = get_account_balance_from_str(session, book_string=f"{uc.loan_id}/loan/lender_interest/e")
    assert amount == Decimal("0.99")


def test_prepayment(session: Session) -> None:
    test_generate_bill_1(session)
    uc = get_user_product(session, 99)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )

    _ = [u.as_dict() for u in all_emis_query.all()]

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1000")

    # prepayment of rs 2000 done
    payment_received(
        session=session,
        user_loan=uc,
        payment_amount=Decimal(2000),
        payment_date=parse_date("2020-05-03"),
        payment_request_id="a12318",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-999.5")

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    _ = [u.as_dict() for u in all_emis_query.all()]

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/pre_payment/l"
    )
    # since payment is made earlier than due_date, that is, 2020-05-15,
    # run_anomaly is reversing interest charged entry and adding it into prepayment amount.
    # assert prepayment_amount == Decimal("969.33")
    assert prepayment_amount == Decimal("1000")

    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )
    bill_id = swipe["data"].loan_id

    emi_payment_mapping = (
        session.query(EmiPaymentMapping).filter(EmiPaymentMapping.loan_id == uc.loan_id).all()
    )
    first_payment_mapping = emi_payment_mapping[0]
    assert first_payment_mapping.emi_number == 1
    assert first_payment_mapping.interest_received == Decimal(0)
    assert first_payment_mapping.principal_received == Decimal(1000)

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000
    bill = bill_generate(user_loan=uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), uc)
    assert bill.table.is_generated is True

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/pre_payment/l"
    )
    assert prepayment_amount == Decimal("0")

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    # since payment is made earlier than due_date, that is 2020-05-15,
    # run_anomaly is reversing interest charged entry and adding it into prepayment amount.
    # assert billed_amount == Decimal("30.67")
    assert billed_amount == Decimal("0")


#
# def test_writeoff(session: Session) -> None:
#     a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
#     a = User(id=99,performed_by=123,)
#     session.add(a)
#     session.flush()
#
#     # assign card
#     uc = create_user_product(
#         session=session, user_id=a.id, card_activation_date=parse_date("2020-03-02"), card_type="ruby", lender_id = 62311,
#     )
#
#
#     swipe = create_card_swipe(
#         session=session,
#         user_loan=uc,
#         txn_time=parse_date("2020-03-08 19:23:11"),
#         amount=Decimal(1000),
#         description="BigBasket.com",
#     )
#     bill = bill_generate(user_loan=uc)
#     assert bill.table.is_generated is True
#
#     swipe = create_card_swipe(
#         session=session,
#         user_loan=uc,
#         txn_time=parse_date("2020-04-08 19:23:11"),
#         amount=Decimal(1500),
#         description="BigBasket.com",
#     )
#
#     _, prepayment_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/pre_payment/l"
#     )
#     bill = bill_generate(user_loan=uc)
#     assert bill.table.is_generated is True
#
#     swipe = create_card_swipe(
#         session=session,
#         user_loan=uc,
#         txn_time=parse_date("2020-05-08 19:23:11"),
#         amount=Decimal(1200),
#         description="BigBasket.com",
#     )
#     bill = bill_generate(user_loan=uc)
#     assert bill.table.is_generated is True
#     unpaid_bills = uc.get_unpaid_bills()
#     bill = unpaid_bills[0]
#
#     lender_interest_incur(session, parse_date("2020-06-01 19:00:00"))
#     write_off_payment(session, uc)
#
#     _, lender_payable_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
#     )
#     assert lender_payable_amount == Decimal("0")
#     _, redcarpet_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/redcarpet/redcarpet_account/a"
#     )
#     assert redcarpet_amount == Decimal("-3748.68")
#     _, writeoff_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/writeoff_expenses/e"
#     )
#     assert writeoff_amount == Decimal("3748.68")
#     _, bad_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/bad_debt_allowance/ca"
#     )
#     assert bad_amount == Decimal("3748.68")
#
#
# def test_writeoff_recovery_one(session: Session) -> None:
#     test_writeoff(session)
#     uc = get_user_product(session, 99)
#     payment_received(
#         session,
#         uc,
#         Decimal("3748.68"),
#         payment_date=parse_date("2020-07-01"),
#         payment_request_id="abcde",
#     )
#     _, writeoff_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/writeoff_expenses/e"
#     )
#     assert writeoff_amount == Decimal("0")
#     _, bad_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/bad_debt_allowance/ca"
#     )
#     assert bad_amount == Decimal("0")
#     _, pg_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
#     assert pg_amount == Decimal("3748.18")
#
#
# def test_writeoff_recovery_two(session: Session) -> None:
#     test_writeoff(session)
#     uc = get_user_product(session, 99)
#
#     payment_received(
#         session, uc, Decimal("3000"), payment_date=parse_date("2020-07-01"), payment_request_id="abcdef",
#     )
#     _, writeoff_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/writeoff_expenses/e"
#     )
#     assert writeoff_amount == Decimal("748.68")
#     _, bad_amount = get_account_balance_from_str(
#         session, book_string=f"{uc.loan_id}/loan/bad_debt_allowance/ca"
#     )
#     assert bad_amount == Decimal("748.68")
#     _, pg_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
#     assert pg_amount == Decimal("2999.50")
#


def test_moratorium(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(
    #     id=38612,
    #     performed_by=123,
    #     name="Ananth",
    #     fullname="Ananth Venkatesh",
    #     nickname="Ananth",
    #     email="ananth@redcarpetup.com",
    # )
    a = User(
        id=38612,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    # 25 days to enforce 15th june as first due date
    uc = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-01-20").date(),
        interest_free_period_in_days=25,
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-01-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    # Generate bill
    generate_date = parse_date("2020-02-01").date()
    user_loan = get_user_product(session, a.id)
    bill_may = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_may.table.bill_due_date + relativedelta(days=1), user_loan
    )

    # Give moratorium
    m = LoanMoratorium.new(
        session,
        loan_id=user_loan.loan_id,
        start_date=parse_date("2020-03-01"),
        end_date=parse_date("2020-06-01"),
    )

    # Apply moratorium
    check_moratorium_eligibility(user_loan)
    provide_moratorium(user_loan, m.start_date.date(), m.end_date.date())

    # Check if scehdule has been updated according to moratorium
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    last_emi = emis_dict[-1]
    assert last_emi["emi_number"] == 15

    emis = user_loan.get_loan_schedule()

    assert len(emis) == 15  # 3 new emis got added for moratorium
    assert emis[1].emi_number == 2
    assert emis[1].total_due_amount == 0
    assert emis[1].due_date == parse_date("2020-03-15").date()
    assert emis[1].total_closing_balance == Decimal("2291.67")
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-04-15").date()
    assert emis[2].total_closing_balance == Decimal("2291.67")
    assert emis[4].emi_number == 5  # emi after moratorium
    assert emis[4].principal_due == Decimal("208.33")
    assert emis[4].interest_due == Decimal("302.68")  # Interest of 3 emis + this month's interest.
    assert emis[4].due_date == parse_date("2020-06-15").date()
    assert emis[4].total_closing_balance == Decimal("2291.67")


def test_moratorium_schedule(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(id=160, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    a = User(
        id=160,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    user_loan = get_user_product(session, a.id)
    bill_april = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("6000")

    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    payment_received(
        session=session,
        user_loan=uc,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="a12319",
    )

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("4000.5")

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    generate_date = parse_date("2020-06-01").date()
    user_loan = get_user_product(session, a.id)
    bill_may = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_may.table.bill_due_date + relativedelta(days=1), user_loan
    )

    # Give moratorium to user
    m = LoanMoratorium.new(
        session,
        loan_id=uc.loan_id,
        start_date=parse_date("2020-09-01"),
        end_date=parse_date("2020-12-01"),
    )

    # Apply moratorium
    check_moratorium_eligibility(user_loan)
    provide_moratorium(user_loan, m.start_date.date(), m.end_date.date())

    # Get list post refresh
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == uc.loan_id, CardEmis.row_status == "active", CardEmis.bill_id == None
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    second_emi = emis_dict[1]
    assert second_emi["interest_received"] == Decimal(360)


def test_is_in_moratorium(session: Session, monkeypatch: MonkeyPatch) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(
    #     id=38613,
    #     performed_by=123,
    #     name="Ananth",
    #     fullname="Ananth Venkatesh",
    #     nickname="Ananth",
    #     email="ananth@redcarpetup.com",
    # )
    a = User(
        id=38613,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    user_loan = create_user_product(
        session,
        user_id=a.id,
        card_type="ruby",
        card_activation_date=parse_date("2020-01-20").date(),
        interest_free_period_in_days=25,
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-01-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    uc = get_user_product(session, a.id)
    # Generate bill
    bill = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), uc)

    assert (
        LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=parse_date("2020-02-21")
        )
        is False
    )

    assert user_loan.get_remaining_min(parse_date("2020-02-01").date()) == 284

    # Give moratorium
    m = LoanMoratorium.new(
        session,
        loan_id=user_loan.loan_id,
        start_date=parse_date("2020-01-20"),
        end_date=parse_date("2020-03-20"),
    )

    assert (
        LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=parse_date("2020-02-21")
        )
        is True
    )

    # Date is outside the moratorium period
    assert (
        LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=parse_date("2020-03-21")
        )
        is False
    )
    assert user_loan.get_remaining_min(parse_date("2020-02-01").date()) == 0  # 0 after moratorium


def test_moratorium_live_user_1836540(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(
    #     id=1836540,
    #     performed_by=123,
    #     name="Mohammad Shahbaz Mohammad Shafi Qureshi",
    #     fullname="Mohammad Shahbaz Mohammad Shafi Qureshi",
    #     nickname="Mohammad Shahbaz Mohammad Shafi Qureshi",
    #     email="shahbazq797@gmail.com",
    # )
    a = User(
        id=1836540,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        # 16th March actual
        card_activation_date=parse_date("2020-03-01").date(),
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-03-19 21:33:53"),
        amount=Decimal(10),
        description="TRUEBALANCE IO         GURGAON       IND",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-03-24 14:01:35"),
        amount=Decimal(100),
        description="PAY*TRUEBALANCE IO     GURGAON       IND",
    )

    uc = get_user_product(session, a.id)
    bill_march = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-03 17:41:43"),
        amount=Decimal(4),
        description="TRUEBALANCE IO         GURGAON       IND",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-12 22:02:47"),
        amount=Decimal(52),
        description="PAYU PAYMENTS PVT LTD  0001243054000 IND",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_march.table.bill_due_date + relativedelta(days=1), uc)

    bill_april = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_april.table.bill_due_date + relativedelta(days=1), uc)

    # Give moratorium
    m = LoanMoratorium.new(
        session,
        loan_id=user_loan.loan_id,
        start_date=parse_date("2020-04-01"),
        end_date=parse_date("2020-06-01"),
    )

    # Apply moratorium
    check_moratorium_eligibility(user_loan)
    provide_moratorium(user_loan, m.start_date.date(), m.end_date.date())

    # Get emi list post few bill creations
    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )

    last_emi = all_emis_query[-1]
    out_of_moratorium_emi = all_emis_query[2]
    assert last_emi.emi_number == 15
    assert out_of_moratorium_emi.total_due_amount == Decimal("20.66")


def test_moratorium_live_user_1836540_with_extension(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)

    a = User(
        id=1836540,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        # 16th March actual
        card_activation_date=parse_date("2020-03-01").date(),
        lender_id=62311,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-03-19 21:33:53"),
        amount=Decimal(10),
        description="TRUEBALANCE IO         GURGAON       IND",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-03-24 14:01:35"),
        amount=Decimal(100),
        description="PAY*TRUEBALANCE IO     GURGAON       IND",
    )

    uc = get_user_product(session, a.id)
    bill_march = bill_generate(uc)

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-03 17:41:43"),
        amount=Decimal(4),
        description="TRUEBALANCE IO         GURGAON       IND",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-12 22:02:47"),
        amount=Decimal(52),
        description="PAYU PAYMENTS PVT LTD  0001243054000 IND",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_march.table.bill_due_date + relativedelta(days=1), uc)

    bill_april = bill_generate(uc)
    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_april.table.bill_due_date + relativedelta(days=1), uc)

    # Extend tenure to 18 months
    extend_tenure(session, uc, 18, parse_date("2020-05-22 22:02:47"))
    extend_schedule(uc, 18, parse_date("2020-05-22"))

    # Get emi list post tenure extension
    all_emis = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
        .all()
    )

    last_emi = all_emis[-1]
    second_last_emi = all_emis[-2]
    # 110-18.34 = 91.66, 91.66/16 = 5.73
    # 56-4.67 = 51.33, 51.33/17 = 3.02
    # 5.73+3.02 = 8.75
    assert second_last_emi.due_amount == Decimal("8.75")
    # 56-4.67 = 51.33, 51.33/16 = 3.21
    assert last_emi.due_amount == Decimal("3.02")
    # First cycle 18 emis, next bill 19 emis
    assert last_emi.emi_number == 19

    emis = uc.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal(13)
    assert emis[0].principal_due == Decimal("9.17")
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(110)
    assert emis[1].total_due_amount == Decimal(20)
    assert emis[1].principal_due == Decimal("13.84")
    assert emis[1].emi_number == 2
    assert emis[1].total_closing_balance == Decimal("156.83")
    assert emis[2].total_due_amount == Decimal("14.53")
    assert emis[2].principal_due == Decimal("8.75")
    assert emis[2].emi_number == 3
    assert emis[2].total_closing_balance == Decimal(143)
    assert emis[18].total_due_amount == Decimal("4.91")
    assert emis[18].principal_due == Decimal("3.02")
    assert emis[18].emi_number == 19
    assert emis[18].total_closing_balance == Decimal("3.02")


def test_intermediate_bill_generation(session: Session) -> None:
    test_card_swipe(session)
    user_loan = get_user_product(session, 2)
    bill_1 = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_1.table.bill_due_date + relativedelta(days=1), user_loan)

    # We now create a swipe after 5 months
    swipe3 = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )

    bill_2 = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_2.table.bill_due_date + relativedelta(days=1), user_loan)

    assert (
        session.query(LoanData)
        .filter(LoanData.loan_id == user_loan.loan_id, LoanData.is_generated.is_(True))
        .count()
    ) == 6


def test_transaction_before_activation(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    # a = User(
    #     id=1836540,
    #     performed_by=123,
    #     name="Mohammad Shahbaz Mohammad Shafi Qureshi",
    #     fullname="Mohammad Shahbaz Mohammad Shafi Qureshi",
    #     nickname="Mohammad Shahbaz Mohammad Shafi Qureshi",
    #     email="shahbazq797@gmail.com",
    # )
    a = User(
        id=1836540,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        card_type="ruby",
        user_id=a.id,
        lender_id=62311,
    )

    # Swipe before activation
    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )

    assert swipe["result"] == "error"


def test_excess_payment_in_future_emis(session: Session) -> None:
    test_generate_bill_1(session)

    user_loan = get_user_product(session, 99)
    payment_date = parse_date("2020-05-03")
    amount = Decimal(450)  # min is 114. Paying for 3 emis. Touching 4th.

    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="s3234",
    )
    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[1].payment_status == "Paid"
    assert emis[2].payment_status == "Paid"
    assert emis[3].payment_status == "UnPaid"
    assert emis[3].payment_received == Decimal("108")

    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "s3234", PaymentMapping.row_status == "active")
        .order_by(PaymentMapping.emi_id)
        .all()
    )
    assert len(pm) == 4
    assert pm[0].emi_id == emis[0].id
    assert pm[0].amount_settled == Decimal("114")
    assert pm[3].amount_settled == Decimal("108")

    # accrue interest of first bill
    _accrue_interest_on_bill_1(session)

    # Generate 2nd bill. 2nd emi is now 341.
    # Do transaction to create new bill.
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(2000),
        description="BigBasket.com",
    )

    bill_2 = bill_generate(user_loan=user_loan)

    emis = user_loan.get_loan_schedule()

    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "s3234", PaymentMapping.row_status == "active")
        .order_by(PaymentMapping.emi_id)
        .all()
    )
    assert len(pm) == 2
    assert pm[1].amount_settled == Decimal("336")


def test_one_rupee_leniency(session: Session) -> None:
    test_generate_bill_1(session)

    user_loan = get_user_product(session, 99)
    payment_date = parse_date("2020-05-03")
    amount = Decimal("113.50")  # min is 114. Paying half paisa less.

    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id="s32224",
    )
    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[0].remaining_amount == Decimal("0.5")

    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "s32224", PaymentMapping.row_status == "active")
        .order_by(PaymentMapping.emi_id)
        .all()
    )
    assert len(pm) == 1
    assert pm[0].emi_id == emis[0].id
    assert pm[0].amount_settled == Decimal("113.50")

    # Make another payment of 10 rupees.
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_amount=Decimal(10),
        payment_date=payment_date,
        payment_request_id="f1234",
    )

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[0].remaining_amount == Decimal(0)
    assert emis[1].remaining_amount == Decimal("104.50")
    assert emis[1].payment_status == "UnPaid"

    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "f1234", PaymentMapping.row_status == "active")
        .order_by(PaymentMapping.emi_id)
        .all()
    )
    assert len(pm) == 2
    assert pm[0].emi_id == emis[0].id
    assert pm[0].amount_settled == Decimal("0.5")
    assert pm[1].emi_id == emis[1].id
    assert pm[1].amount_settled == Decimal("9.5")
