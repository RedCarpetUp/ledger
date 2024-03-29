import contextlib
from decimal import Decimal
from io import StringIO
from test.utils import (
    pay_payment_request,
    payment_request_data,
)

import alembic
from _pytest.monkeypatch import MonkeyPatch
from alembic.command import current as alembic_current
from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from rush.accrue_financial_charges import (
    accrue_interest_on_all_bills,
    accrue_late_charges,
    create_loan_fee_entry,
    get_interest_left_to_accrue,
)
from rush.card import (
    create_user_product,
    get_product_class,
    get_user_product,
)
from rush.card.base_card import BaseBill
from rush.card.health_card import HealthCard
from rush.card.rebel_card import RebelCard
from rush.card.reset_card import ResetCard
from rush.card.reset_card_v2 import ResetCardV2
from rush.card.ruby_card import RubyCard
from rush.card.term_loan import TermLoan
from rush.card.term_loan2 import TermLoan2
from rush.card.term_loan_pro import TermLoanPro
from rush.card.term_loan_pro2 import TermLoanPro2
from rush.card.utils import (
    create_loan,
    create_loan_fee,
    create_user_product_mapping,
    get_daily_spend,
    get_daily_total_transactions,
    get_weekly_spend,
)
from rush.card.zeta_card import ZetaCard
from rush.create_bill import bill_generate
from rush.create_card_swipe import (
    create_card_swipe,
    reverse_card_swipe,
)
from rush.create_emi import (
    daily_dpd_update,
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
    EventDpd,
    Fee,
    JournalEntry,
    LedgerLoanData,
    LedgerTriggerEvent,
    Lenders,
    LoanMoratorium,
    LoanSchedule,
    MoratoriumInterest,
    PaymentMapping,
    PaymentSplit,
    Product,
    User,
)
from rush.payments import (
    customer_prepayment_refund,
    find_split_to_slide_in_loan,
    payment_received,
    refund_payment,
    remove_fee,
    settle_payment_in_bank,
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


def test_product_classes() -> None:
    assert get_product_class("ruby") == RubyCard
    assert get_product_class("term_loan_reset") == ResetCard
    assert get_product_class("term_loan_reset_v2") == ResetCardV2
    assert get_product_class("rebel") == RebelCard
    assert get_product_class("term_loan") == TermLoan
    assert get_product_class("term_loan_2") == TermLoan2
    assert get_product_class("term_loan_pro") == TermLoanPro
    assert get_product_class("term_loan_pro_2") == TermLoanPro2
    assert get_product_class("health_card") == HealthCard


def create_products(session: Session) -> None:
    ruby_product = Product(product_name="ruby")
    session.add(ruby_product)
    reset_product = Product(product_name="term_loan_reset")
    session.add(reset_product)
    session.flush()


def card_db_updates(session: Session) -> None:
    create_products(session=session)
    pass


def test_user2(session: Session) -> None:
    # u = User(performed_by=123, id=1, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    u = User(
        id=1,
        performed_by=123,
    )
    session.add(u)
    session.commit()
    a = session.query(User).first()


def test_user(session: Session) -> None:
    # u = User(id=2, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    u = User(
        id=2,
        performed_by=123,
    )
    session.add(u)
    session.commit()
    a = session.query(User).first()


def test_lenders(session: Session) -> None:
    l1 = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(l1)
    l2 = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(l2)
    session.flush()
    lender = session.query(Lenders).first()
    assert isinstance(lender, Lenders) == True


def test_lender_disbursal(session: Session) -> None:
    test_lenders(session)
    resp = lender_disbursal(session, Decimal(100000), 62311)
    assert resp["result"] == "success"
    # _, lender_capital_balance = get_account_balance_from_str(session, "62311/lender/lender_capital/l")
    # assert lender_capital_balance == Decimal(100000)


def test_m2p_transfer(session: Session) -> None:
    test_lenders(session)
    resp = m2p_transfer(session, Decimal(50000), 62311)
    assert resp["result"] == "success"

    # _, lender_pool_balance = get_account_balance_from_str(session, "62311/lender/pool_balance/a")
    # assert lender_pool_balance == Decimal(50000)


def test_card_swipe_and_reversal(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    uc = create_user_product(
        session=session,
        user_id=2,
        card_activation_date=parse_date("2020-05-01").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    swipe1 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="123456",
    )

    swipe2 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123456",
    )

    assert swipe1["data"].loan_id == swipe2["data"].loan_id
    bill_id = swipe1["data"].loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{bill_id}/bill/unbilled/a")
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've not loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/available_limit/l")
    assert card_balance == -900

    _, lender_payable = get_account_balance_from_str(session, f"{uc.loan_id}/loan/lender_payable/l")
    assert lender_payable == 900

    resp = reverse_card_swipe(session, uc, swipe2["data"], parse_date("2020-05-02 13:22:11"))
    assert resp["result"] == "success"

    _, unbilled_balance = get_account_balance_from_str(session, f"{bill_id}/bill/unbilled/a")
    assert unbilled_balance == 700
    # remaining card balance should be -700(-900+200) because we've not loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{uc.loan_id}/card/available_limit/l")
    assert card_balance == -700

    _, lender_payable = get_account_balance_from_str(session, f"{uc.loan_id}/loan/lender_payable/l")
    assert lender_payable == 700

    swipe3 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-02 17:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no_3",
        trace_no="123452",
    )


def test_closing_bill(session: Session) -> None:
    # Replicating nishant's case upto June
    test_lenders(session)
    card_db_updates(session)

    user = User(
        id=230,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    # assign card
    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2019-02-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    swipe_date = parse_date("2019-02-03 19:23:11")
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=swipe_date,
        amount=Decimal(3000),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_4",
        trace_no="123456",
    )

    daily_txn_1 = get_daily_total_transactions(
        session=session, loan=user_loan, date_to_check_against=swipe_date.date()
    )
    assert daily_txn_1 == 1

    daily_spent_1 = get_daily_spend(
        session=session, loan=user_loan, date_to_check_against=swipe_date.date()
    )
    assert daily_spent_1 == Decimal("3000")

    weekly_spent_1 = get_weekly_spend(
        session=session, loan=user_loan, date_to_check_against=swipe_date.date()
    )
    assert weekly_spent_1 == Decimal("3000")

    bill_date = parse_date("2019-02-28 23:59:59")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    event_date = parse_date("2019-03-16 00:00:00")
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(100))

    payment_date = parse_date("2019-03-27")
    payment_request_id = "a1234"
    amount = Decimal(463)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )

    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    bill_date = parse_date("2019-03-31 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    payment_date = parse_date("2019-04-15")
    payment_request_id = "a1235"
    amount = Decimal(363)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )

    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    bill_date = parse_date("2019-04-30 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )

    payment_date = parse_date("2019-05-16")
    payment_request_id = "a1236"
    amount = Decimal(2545)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2019-05-20 19:23:11"),
        amount=Decimal(3000),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_5",
        trace_no="123456",
    )

    daily_txn_2 = get_daily_total_transactions(
        session=session, loan=user_loan, date_to_check_against=parse_date("2019-05-20").date()
    )
    assert daily_txn_2 == 1

    bill_date = parse_date("2019-05-31 00:00:00")
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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_6",
        trace_no="123456",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill = bill_generate(user_loan=user_loan)
    # Interest event to be fired separately now

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-04-02").date()
    assert bill.table.bill_close_date == parse_date("2020-04-30").date()
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

    dpd_events = session.query(EventDpd).filter_by(loan_id=uc.loan_id).all()
    assert dpd_events[0].balance == Decimal(1000)

    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    assert interest_left_to_accrue == Decimal("368.04")

    emis = uc.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal(114)
    assert emis[0].principal_due == Decimal("83.33")
    assert emis[0].interest_due == Decimal("30.67")
    assert emis[0].due_date == parse_date("2020-05-15").date()
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(1000)
    assert emis[1].total_closing_balance == Decimal("916.67")
    assert emis[11].total_closing_balance == Decimal("83.33")


def test_generate_bill_reducing_interest_1(session: Session) -> None:
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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        interest_type="reducing",
        tenure=12,
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1200),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_7",
        trace_no="123456",
    )
    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1200

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill = bill_generate(user_loan=user_loan)
    # Interest event to be fired separately now

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-04-02").date()
    assert bill.table.bill_close_date == parse_date("2020-04-30").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    # Should be 0 because it has moved to billed account.
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 1200

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == Decimal("121")

    post_date = parse_date("2020-05-21")
    event = LedgerTriggerEvent(name="daily_dpd_update", loan_id=user_loan.loan_id, post_date=post_date)
    session.add(event)

    update_event_with_dpd(user_loan=user_loan, event=event)

    dpd_events = session.query(EventDpd).filter_by(loan_id=uc.loan_id).all()
    assert dpd_events[0].balance == Decimal(1200)

    emis = uc.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal("121")
    assert emis[0].principal_due == Decimal("84.55")
    assert emis[0].interest_due == Decimal("36.45")
    assert emis[0].due_date == parse_date("2020-05-15").date()
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(1200)
    assert emis[1].total_closing_balance == Decimal("1115.45")
    assert emis[11].principal_due == Decimal("117.04")
    assert emis[11].interest_due == Decimal("3.96")
    assert emis[11].total_closing_balance == Decimal("117.04")


def _accrue_interest_on_bill_1(session: Session) -> None:
    user_loan = get_user_product(session, 99)
    assert user_loan is not None
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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        min_multiplier=Decimal(2),
        tenure=12,
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(12000),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_8",
        trace_no="123456",
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
    assert bill.table.bill_close_date == parse_date("2020-04-30").date()
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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        min_tenure=24,
        tenure=12,
    )
    assert user_loan is not None
    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(12000),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_9",
        trace_no="123456",
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
    assert bill.table.bill_close_date == parse_date("2020-04-30").date()
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
    assert user_loan is not None
    unpaid_bills = user_loan.get_unpaid_bills()
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1000")

    payment_date = parse_date("2020-05-03")
    amount = Decimal(100)
    payment_request_id = "a1237"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    bill = unpaid_bills[0]
    _, gateway_expenses = get_account_balance_from_str(
        session,
        book_string=f"{user_loan.lender_id}/lender/gateway_expenses/e",
        to_date=payment_requests_data.payment_received_in_bank_date,
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

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
    assert user_loan is not None
    event_date = parse_date("2020-05-16 00:00:00")
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(118))

    fee_due = (
        session.query(Fee)
        .filter(Fee.identifier_id == bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )
    assert fee_due is not None
    assert fee_due.net_amount == Decimal(100)
    assert fee_due.gross_amount == Decimal(118)

    min_due = bill.get_remaining_min()
    assert min_due == 132


def _accrue_late_fine_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    event_date = parse_date("2020-05-16 00:00:00")
    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    bill = accrue_late_charges(session, user_loan, event_date, Decimal(118))

    fee_due = (
        session.query(Fee)
        .filter(Fee.identifier_id == bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .order_by(Fee.id.desc())
        .one_or_none()
    )
    assert fee_due is not None
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
    assert user_loan is not None
    unpaid_bills = user_loan.get_unpaid_bills()

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    bill = unpaid_bills[0]

    fee_id = (
        session.query(Fee.id)
        .filter(
            Fee.identifier_id == bill.id,
            Fee.identifier == "bill",
            Fee.name == "late_fee",
            Fee.fee_status == "UNPAID",
        )
        .scalar()
    )

    payment_date = parse_date("2020-05-20")
    payment_request_id = "a1238"
    amount = Decimal(132)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    # assert is_min_paid(session, bill) is True
    min_due = bill.get_remaining_min()
    assert min_due == Decimal(0)

    bill_fee = session.query(Fee).filter_by(id=fee_id).one_or_none()
    assert bill_fee is not None
    assert bill_fee.fee_status == "PAID"
    assert bill_fee.net_amount_paid == Decimal(100)
    assert bill_fee.sgst_paid == Decimal(9)
    assert bill_fee.cgst_paid == Decimal(9)
    assert bill_fee.gross_amount_paid == Decimal(118)

    _, late_fine_earned = get_account_balance_from_str(session, f"{bill.id}/bill/late_fee/r")
    assert late_fine_earned == Decimal(100)

    _, sgst_balance = get_account_balance_from_str(session, f"{user_loan.user_id}/user/sgst_payable/l")
    assert sgst_balance == Decimal(9)

    _, cgst_balance = get_account_balance_from_str(session, f"{user_loan.user_id}/user/cgst_payable/l")
    assert cgst_balance == Decimal(9)

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("16.67")

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 118 rupees got settled in late fine.
    assert principal_due == Decimal("900")

    _, pg_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
    assert user_loan is not None
    unpaid_bills = user_loan.get_unpaid_bills()

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("900.5")

    payment_date = parse_date("2020-06-14")
    payment_request_id = "a1239"
    amount = Decimal(132)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_remaining_min()
    assert min_due == Decimal(0)

    fee_due = (
        session.query(Fee)
        .filter(Fee.identifier_id == bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )
    assert fee_due is not None
    assert fee_due.fee_status == "PAID"

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fee/r")
    assert late_fine_due == Decimal("100")

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    # changed from 916 to 816, the late did not get settled.
    assert principal_due == Decimal("900")

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
    assert len(payment_splits) == 4
    split = {ps.component: ps.amount_settled for ps in payment_splits}
    assert split["late_fee"] == Decimal("100")
    assert split["sgst"] == Decimal("9")
    assert split["cgst"] == Decimal("9")
    assert split["interest"] == Decimal("14")


def test_is_bill_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)

    bill = (
        session.query(LedgerLoanData)
        .filter(LedgerLoanData.user_id == user_loan.user_id)
        .order_by(LedgerLoanData.bill_start_date.desc())
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

    amount = Decimal("916.67")
    payment_date = parse_date("2020-05-25")
    payment_request_id = "a12310"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    is_it_paid_now = is_bill_closed(session, bill)
    assert is_it_paid_now is True

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-147.17")  # negative that implies prepaid

    emis = user_loan.get_loan_schedule()
    assert emis[1].payment_received == amount
    assert emis[1].payment_status == "Paid"
    assert emis[1].principal_due == amount
    assert emis[2].principal_due == Decimal(0)


def _generate_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    previous_bill = (  # get last generated bill.
        session.query(LedgerLoanData)
        .filter(LedgerLoanData.user_id == user.id, LedgerLoanData.is_generated.is_(True))
        .order_by(LedgerLoanData.bill_start_date.desc())
        .first()
    )
    # Bill shouldn't be closed.
    assert is_bill_closed(session, previous_bill) is False

    # Do transaction to create new bill.
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(2000),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_10",
        trace_no="123456",
    )
    assert user_loan.sub_product_type == "card"
    _, user_loan_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/card/available_limit/a"
    )
    assert user_loan_balance == Decimal(-3000)

    bill_2 = bill_generate(user_loan=user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_2.table.bill_due_date + relativedelta(days=1), user_loan)
    assert bill_2.bill_start_date == parse_date("2020-05-01").date()

    unpaid_bills = user_loan.get_unpaid_bills()
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

    emis = user_loan.get_loan_schedule()
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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 20:23:11"),
        amount=Decimal(1500),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no_11",
        trace_no="123456",
    )

    generate_date = parse_date("2020-06-01").date()
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_12",
        trace_no="123456",
    )

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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

    all_emis = user_loan.get_loan_schedule()  # Get the latest emi of that user.

    last_emi = all_emis[-1]
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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_13",
        trace_no="123456",
    )

    generate_date = parse_date("2020-05-01").date()
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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
        txn_ref_no="dummy_txn_ref_no_14",
        trace_no="123456",
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

    all_emis = user_loan.get_loan_schedule()  # Get the latest emi of that user.

    last_emi = all_emis[12]
    first_emi = all_emis[0]
    second_emi = all_emis[1]
    assert first_emi.principal_due == 500
    assert last_emi.principal_due == 500
    assert second_emi.principal_due == 1000
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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        card_activation_date=parse_date("2020-05-01").date(),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_17",
        trace_no="123456",
    )

    generate_date = parse_date("2020-06-01").date()
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("6000")

    # Do Full Payment
    payment_date = parse_date("2020-07-30")
    amount = Decimal(6360)
    payment_request_id = "a12311"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-359.5")

    # Check if amount is adjusted correctly in schedule
    all_emis = user_loan.get_loan_schedule()
    emis_dict = [u.as_dict() for u in all_emis]

    # TODO Get interest from table

    # assert emis_dict[0]["due_date"] == parse_date("2020-06-15").date()
    # assert emis_dict[0]["total_due_amount"] == 680
    # assert emis_dict[0]["due_amount"] == 500
    # assert emis_dict[0]["total_closing_balance"] == 6000
    # assert emis_dict[0]["total_closing_balance_post_due_date"] == 6180
    # assert emis_dict[0]["interest_received"] == 180
    # assert emis_dict[0]["payment_received"] == 500
    # assert emis_dict[0]["interest"] == 180
    # assert emis_dict[0]["interest_current_month"] == 90
    # assert emis_dict[0]["interest_next_month"] == 90
    # assert emis_dict[1]["due_date"] == parse_date("2020-07-15").date()
    # assert emis_dict[1]["total_due_amount"] == 680
    # assert emis_dict[1]["due_amount"] == 500
    # assert emis_dict[1]["total_closing_balance"] == 5500
    # assert emis_dict[1]["total_closing_balance_post_due_date"] == 5680
    # assert emis_dict[1]["interest_received"] == 180
    # assert emis_dict[1]["payment_received"] == 500
    # assert emis_dict[1]["interest"] == 180
    # assert emis_dict[1]["interest_current_month"] == 90
    # assert emis_dict[1]["interest_next_month"] == 90
    # assert emis_dict[2]["due_date"] == parse_date("2020-08-15").date()
    # assert emis_dict[2]["total_due_amount"] == 5000
    # assert emis_dict[2]["due_amount"] == 5000
    # assert emis_dict[2]["total_closing_balance"] == 0
    # assert emis_dict[2]["total_closing_balance_post_due_date"] == 0
    # assert emis_dict[2]["interest_received"] == 0
    # assert emis_dict[2]["payment_received"] == 5000
    # assert emis_dict[2]["interest"] == 0
    # assert emis_dict[2]["interest_current_month"] == 0
    # assert emis_dict[2]["interest_next_month"] == 0
    # assert emis_dict[3]["due_date"] == parse_date("2020-09-15").date()
    # assert emis_dict[3]["total_due_amount"] == 0
    # assert emis_dict[3]["due_amount"] == 0
    # assert emis_dict[3]["total_closing_balance"] == 0
    # assert emis_dict[3]["total_closing_balance_post_due_date"] == 0
    # assert emis_dict[3]["interest_received"] == 0
    # assert emis_dict[3]["payment_received"] == 0
    # assert emis_dict[3]["interest"] == 0
    # assert emis_dict[3]["interest_current_month"] == 0
    # assert emis_dict[3]["interest_next_month"] == 0


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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        card_activation_date=parse_date("2020-05-04").date(),
        lender_id=62311,
        tenure=12,
    )

    # Card transactions
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-20 17:23:01"),
        amount=Decimal(129),
        description="PAYTM                  Noida         IND",
        txn_ref_no="dummy_txn_ref_no_18",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-22 09:33:18"),
        amount=Decimal(115),
        description="TPL*UDIO               MUMBAI        IND",
        txn_ref_no="dummy_txn_ref_no_19",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-22 09:50:46"),
        amount=Decimal(500),
        description="AIRTELMONEY            MUMBAI        IND",
        txn_ref_no="dummy_txn_ref_no_20",
        trace_no="123456",
    )
    refunded_swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-22 12:50:05"),
        amount=Decimal(2),
        description="PHONEPE RECHARGE.      GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_21",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 01:18:54"),
        amount=Decimal(100),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_22",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 01:42:51"),
        amount=Decimal(54),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_23",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 01:49:44"),
        amount=Decimal(1100),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
        txn_ref_no="dummy_txn_ref_no_24",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-23 13:12:33"),
        amount=Decimal(99),
        description="ULLU DIGITAL PRIVATE L MUMBAI        IND",
        txn_ref_no="dummy_txn_ref_no_25",
        trace_no="123456",
    )

    # Merchant Refund
    refund_date = parse_date("2020-05-23 21:20:07")
    amount = Decimal(2)
    payment_request_id = "A3d223g2"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no_26",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-24 22:09:42"),
        amount=Decimal(99),
        description="PayTM*KookuDigitalPriP Mumbai        IND",
        txn_ref_no="dummy_txn_ref_no_27",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 08:33:40"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_28",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 10:26:12"),
        amount=Decimal(380),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_29",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 11:40:05"),
        amount=Decimal(199),
        description="PAYTM                  Noida         IND",
        txn_ref_no="dummy_txn_ref_no_30",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 11:57:15"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
        txn_ref_no="dummy_txn_ref_no_31",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-25 12:25:57"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
        txn_ref_no="dummy_txn_ref_no_32",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 08:04:47"),
        amount=Decimal(1450),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_33",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 14:47:41"),
        amount=Decimal(110),
        description="TPL*UDIO               MUMBAI        IND",
        txn_ref_no="dummy_txn_ref_no_34",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 16:37:27"),
        amount=Decimal(700),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_35",
        trace_no="123456",
    )
    one_sixty_rupee = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-26 22:10:58"),
        amount=Decimal(160),
        description="Linkyun Technology Pri Gurgaon       IND",
        txn_ref_no="dummy_txn_ref_no_36",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-27 12:25:25"),
        amount=Decimal(299),
        description="PAYTM                  Noida         IND",
        txn_ref_no="dummy_txn_ref_no_37",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-28 20:38:02"),
        amount=Decimal(199),
        description="Linkyun Technology Pri Gurgaon       IND",
        txn_ref_no="dummy_txn_ref_no_38",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-28 21:45:55"),
        amount=Decimal(800),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_39",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-29 10:05:58"),
        amount=Decimal(525),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
        txn_ref_no="dummy_txn_ref_no_40",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-05-30 16:04:21"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_41",
        trace_no="123456",
    )

    # Generate bill
    bill_may = bill_generate(uc)

    # check latest bill method
    latest_bill = uc.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Check for atm fee.
    atm_fee_due = (
        session.query(Fee)
        .filter(Fee.identifier_id == bill_may.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )
    assert atm_fee_due is not None
    assert atm_fee_due.gross_amount == 59

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-03 13:20:40"),
        amount=Decimal("150"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_42",
        trace_no="123456",
    )
    one_rupee_1 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 17:09:57"),
        amount=Decimal("1"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_43",
        trace_no="123456",
    )
    one_rupee_2 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 17:12:01"),
        amount=Decimal("1"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_44",
        trace_no="123456",
    )
    one_rupee_3 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 17:26:54"),
        amount=Decimal("1"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_45",
        trace_no="123456",
    )
    one_rupee_4 = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-07 18:02:08"),
        amount=Decimal("1"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_46",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-08 20:03:37"),
        amount=Decimal("281.52"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_47",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 14:58:57"),
        amount=Decimal("810"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_48",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 15:02:50"),
        amount=Decimal("939.96"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_49",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 15:43:12"),
        amount=Decimal("240.54"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_50",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-09 15:51:18"),
        amount=Decimal("240.08"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_51",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 09:37:59"),
        amount=Decimal("10"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_52",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 15:21:01"),
        amount=Decimal("1700.84"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_53",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 23:27:06"),
        amount=Decimal("273.39"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_54",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-10 23:31:55"),
        amount=Decimal("273.39"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_55",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-12 17:11:11"),
        amount=Decimal("1254.63"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_56",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-13 11:59:50"),
        amount=Decimal("281.52"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_57",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-13 12:06:56"),
        amount=Decimal("281.52"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_58",
        trace_no="123456",
    )
    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-13 12:17:49"),
        amount=Decimal("1340.64"),
        description="JUNE",
        txn_ref_no="dummy_txn_ref_no_59",
        trace_no="123456",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_may.table.bill_due_date + relativedelta(days=1), uc)

    # Merchant Refund
    refund_date = parse_date("2020-06-16 01:48:05")
    amount = Decimal(160)
    payment_request_id = "A3d223g3"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    # Merchant Refund
    refund_date = parse_date("2020-06-17 00:21:23")
    amount = Decimal(160)
    payment_request_id = "A3d223g4"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:54:58")
    amount = Decimal(1)
    payment_request_id = "A3d223g5"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:54:59")
    amount = Decimal(1)
    payment_request_id = "A3d223g6"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:54:59")
    amount = Decimal(1)
    payment_request_id = "A3d223g7"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    # Merchant Refund
    refund_date = parse_date("2020-06-18 06:55:00")
    amount = Decimal(1)
    payment_request_id = "A3d223g8"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, uc, payment_requests_data)
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{uc.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("20672.03")

    _, lender_amount = get_account_balance_from_str(session, book_string=f"62311/lender/pg_account/a")
    assert lender_amount == Decimal("0")

    assert uc.get_remaining_max() == Decimal("13036.83")
    assert uc.get_total_outstanding() == Decimal("21118.86")

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
    payment_request_id = "a12312"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=uc,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=uc,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    # Do Partial Payment
    payment_date = parse_date("2020-08-02 14:11:06")
    amount = Decimal(1139)
    payment_request_id = "a12313"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=uc.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=uc,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=uc,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    # Check if amount is adjusted correctly in schedule
    # Get from table entries TODO

    # assert first_emi["interest"] == Decimal("387.83")
    # assert first_emi["atm_fee"] == Decimal(50)
    # assert first_emi["interest_received"] == Decimal("387.83")

    event_date = parse_date("2020-08-21 00:05:00")

    dpd_events = session.query(EventDpd).filter_by(loan_id=uc.loan_id).all()

    last_entry_first_bill = dpd_events[-2]
    last_entry_second_bill = dpd_events[-1]

    assert last_entry_first_bill.balance == Decimal("8082.03")
    assert last_entry_second_bill.balance == Decimal("7933.12")

    _, bill_may_principal_due = get_account_balance_from_str(
        session, book_string=f"{bill_may.id}/bill/principal_receivable/a"
    )
    _, bill_june_principal_due = get_account_balance_from_str(
        session, book_string=f"{bill_june.id}/bill/principal_receivable/a"
    )
    assert bill_may_principal_due == Decimal("12676.07")
    assert bill_june_principal_due == Decimal("7933.12")

    daily_date = parse_date("2020-08-28 00:05:00")
    daily_dpd_update(session, uc, daily_date)

    dc_sum = (
        session.query(func.sum(JournalEntry.debit), func.sum(JournalEntry.credit))
        .filter(JournalEntry.loan_id == uc.id)
        .all()
    )

    debit_total = dc_sum[0][0]
    credit_total = dc_sum[0][1]
    assert debit_total == credit_total


def test_interest_reversal_interest_already_settled(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    # Pay min amount before interest is accrued.
    payment_date = parse_date("2020-05-05 19:23:11")
    amount = Decimal(132)
    payment_request_id = "aasdf123"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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
    payment_request_id = "a12314"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
    assert user_loan is not None
    payment_date = parse_date("2020-06-14 19:23:11")
    amount = Decimal("2916.67")
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
    # TODO get interest from table
    # second_emi = all_emis_query[1]
    # assert second_emi.interest == 91
    payment_request_id = "a12315"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-147.17")

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
    # TODO get interest from table
    # second_emi = all_emis_query[1]
    # assert second_emi.interest == 0

    assert is_bill_closed(session, first_bill.table) is True
    # 90 got settled in new bill.
    assert is_bill_closed(session, second_bill.table) is True


def test_failed_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_interest_on_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("2769")

    payment_date = parse_date(
        "2020-06-18 19:23:11"
    )  # Payment came after due date. Interest won't get reversed.
    amount = Decimal("2916.67")
    unpaid_bills = user_loan.get_unpaid_bills()
    payment_request_id = "a12316"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
    assert is_bill_closed(session, first_bill.table) is False
    assert is_bill_closed(session, second_bill.table) is False


def _pay_minimum_amount_bill_2(session: Session) -> None:
    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1500")

    # Pay 10 more. and 100 for late fee.
    payment_date = parse_date("2020-06-20")
    amount = Decimal(110)
    payment_request_id = "a12317"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
    assert balance_paid is not None
    assert balance_paid.amount == Decimal(110)


def test_refund_1(session: Session) -> None:
    test_generate_bill_1(session)
    _accrue_interest_on_bill_1(session)
    user_loan = get_user_product(session, 99)
    assert user_loan is not None

    refund_date = parse_date("2020-05-05 15:24:34")
    payment_request_id = "asd23g2"
    amount = Decimal(100)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, user_loan, payment_requests_data)

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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
        txn_ref_no="dummy_txn_ref_no_61",
        trace_no="123456",
    )
    refund_date = parse_date("2020-05-15 15:24:34")
    payment_request_id = "af423g2"
    amount = Decimal(1500)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, user_loan, payment_requests_data)

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )
    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-06-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_63",
        trace_no="123456",
    )
    bill_id = swipe["data"].loan_id
    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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
        txn_ref_no="dummy_txn_ref_no_62",
        trace_no="123456",
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
        txn_ref_no="dummy_txn_ref_no_65",
        trace_no="123456",
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
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )
    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-07-29 19:23:11"),
        amount=Decimal(500),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_66",
        trace_no="123456",
    )
    swipe = create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-07-29 10:23:11"),
        amount=Decimal(500),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_67",
        trace_no="123456",
    )
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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
    user_loan = get_user_product(session, 99)
    assert user_loan is not None

    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("1000")

    # prepayment of rs 2000 done
    payment_date = parse_date("2020-05-03")
    amount = Decimal(2000)
    payment_request_id = "a12318"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
    assert lender_amount == Decimal("0")
    _, lender_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable == Decimal("-999.5")

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )
    # since payment is made earlier than due_date, that is, 2020-05-15,
    # run_anomaly is reversing interest charged entry and adding it into prepayment amount.
    # assert prepayment_amount == Decimal("969.33")
    assert prepayment_amount == Decimal("1000")

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
        txn_ref_no="dummy_txn_ref_no_68",
        trace_no="123456",
    )
    bill_id = swipe["data"].loan_id

    emi_payment_mapping = session.query(PaymentMapping).all()
    first_payment_mapping = emi_payment_mapping[0]
    assert first_payment_mapping.amount_settled == Decimal(114)

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000
    bill = bill_generate(user_loan=user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), user_loan)
    assert bill.table.is_generated is True

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
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
    assert latest_bill.gross_principal == Decimal(1000)
    assert latest_bill.principal == Decimal(0)


#
# def test_writeoff(session: Session) -> None:
#     a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
#     a = User(id=99,performed_by=123,)
#     session.add(a)
#     session.flush()
#
#     # assign card
#     uc = create_user_product(
#         session=session, user_id=a.id, card_activation_date=parse_date("2020-03-02"), card_type="ruby",
#         rc_rate_of_interest_monthly=Decimal(3), lender_id = 62311,
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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        card_activation_date=parse_date("2020-01-20").date(),
        interest_free_period_in_days=25,
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-01-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="dummy_txn_ref_no_69",
        trace_no="123456",
    )

    # Generate bill
    generate_date = parse_date("2020-02-01").date()
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill_may = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_may.table.bill_due_date + relativedelta(days=1), user_loan
    )

    start_date = parse_date("2020-03-15").date()
    end_date = parse_date("2020-05-15").date()
    # Apply moratorium
    provide_moratorium(user_loan, start_date, end_date)

    # Check if scehdule has been updated according to moratorium
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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        card_activation_date=parse_date("2020-04-02").date(),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
        txn_ref_no="a",
        trace_no="123456",
    )

    generate_date = parse_date("2020-05-01").date()
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
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
    payment_request_id = "a12319"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, lender_amount = get_account_balance_from_str(
        session,
        book_string=f"62311/lender/pg_account/a",
        to_date=payment_requests_data.payment_received_in_bank_date,
    )
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
        txn_ref_no="b",
        trace_no="123456",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    generate_date = parse_date("2020-06-01").date()
    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill_may = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_may.table.bill_due_date + relativedelta(days=1), user_loan
    )

    start_date = parse_date("2020-09-15").date()
    end_date = parse_date("2020-11-15").date()
    # Apply moratorium
    provide_moratorium(user_loan, start_date, end_date)

    # Get list post refresh
    emis = uc.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal(680)
    assert emis[0].principal_due == Decimal(500)
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(6000)
    assert emis[1].total_due_amount == Decimal(1360)
    assert emis[1].principal_due == Decimal(1000)
    assert emis[1].emi_number == 2
    assert emis[1].total_closing_balance == Decimal(11500)
    assert emis[2].total_due_amount == Decimal(1360)
    assert emis[2].principal_due == Decimal(1000)
    assert emis[2].emi_number == 3
    assert emis[2].total_closing_balance == Decimal(10500)
    assert emis[15].total_due_amount == Decimal(680)
    assert emis[15].principal_due == Decimal(500)
    assert emis[15].emi_number == 16
    assert emis[15].total_closing_balance == Decimal(500)


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

    uc = create_user_product(
        session,
        user_id=a.id,
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        card_activation_date=parse_date("2020-01-20").date(),
        interest_free_period_in_days=25,
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-01-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        txn_ref_no="e",
        trace_no="123456",
    )

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    # Generate bill
    bill = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill.table.bill_due_date + relativedelta(days=1), user_loan)

    assert (
        LoanMoratorium.is_in_moratorium(
            session, loan_id=user_loan.loan_id, date_to_check_against=parse_date("2020-02-21")
        )
        is False
    )

    assert user_loan.get_remaining_min(parse_date("2020-02-01").date()) == 284

    start_date = parse_date("2020-01-15").date()
    end_date = parse_date("2020-03-15").date()
    # Apply moratorium
    provide_moratorium(user_loan, start_date, end_date)

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
    uc = create_user_product(
        session=session,
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        # 16th March actual
        card_activation_date=parse_date("2020-03-01").date(),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-03-19 21:33:53"),
        amount=Decimal(10),
        description="TRUEBALANCE IO         GURGAON       IND",
        txn_ref_no="c",
        trace_no="123456",
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-03-24 14:01:35"),
        amount=Decimal(100),
        description="PAY*TRUEBALANCE IO     GURGAON       IND",
        txn_ref_no="d",
        trace_no="123456",
    )

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill_march = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-03 17:41:43"),
        amount=Decimal(4),
        description="TRUEBALANCE IO         GURGAON       IND",
        txn_ref_no="g",
        trace_no="123456",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-12 22:02:47"),
        amount=Decimal(52),
        description="PAYU PAYMENTS PVT LTD  0001243054000 IND",
        txn_ref_no="f",
        trace_no="123456",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_march.table.bill_due_date + relativedelta(days=1), user_loan
    )

    bill_april = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    start_date = parse_date("2020-04-15").date()
    end_date = parse_date("2020-05-15").date()
    # Apply moratorium
    provide_moratorium(user_loan, start_date, end_date)

    # Get emi list post few bill creations
    emis = user_loan.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal(0)
    assert emis[0].principal_due == Decimal(0)
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(110)
    assert emis[1].total_due_amount == Decimal(0)
    assert emis[1].principal_due == Decimal(0)
    assert emis[1].emi_number == 2
    assert emis[1].total_closing_balance == Decimal(166)
    assert emis[2].total_due_amount == Decimal("29.99")
    assert emis[2].principal_due == Decimal("13.84")
    assert emis[2].emi_number == 3
    assert emis[2].total_closing_balance == Decimal(166)
    assert emis[13].total_due_amount == Decimal(20)
    assert emis[13].principal_due == Decimal("13.84")
    assert emis[13].emi_number == 14
    assert emis[13].total_closing_balance == Decimal("13.84")


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
    uc = create_user_product(
        session=session,
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        # 16th March actual
        card_activation_date=parse_date("2020-03-01").date(),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-03-19 21:33:53"),
        amount=Decimal(10),
        description="TRUEBALANCE IO         GURGAON       IND",
        txn_ref_no="l",
        trace_no="123456",
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-03-24 14:01:35"),
        amount=Decimal(100),
        description="PAY*TRUEBALANCE IO     GURGAON       IND",
        txn_ref_no="i",
        trace_no="123456",
    )

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill_march = bill_generate(user_loan)

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-03 17:41:43"),
        amount=Decimal(4),
        description="TRUEBALANCE IO         GURGAON       IND",
        txn_ref_no="h",
        trace_no="123456",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-12 22:02:47"),
        amount=Decimal(52),
        description="PAYU PAYMENTS PVT LTD  0001243054000 IND",
        txn_ref_no="m",
        trace_no="123456",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_march.table.bill_due_date + relativedelta(days=1), user_loan
    )

    bill_april = bill_generate(user_loan)
    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    # Extend tenure to 18 months
    extend_schedule(user_loan, 18, parse_date("2020-05-22"))

    emis = user_loan.get_loan_schedule()
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
    # First cycle 18 emis, next bill 19 emis
    assert emis[18].emi_number == 19
    assert emis[18].total_closing_balance == Decimal("3.02")


def test_reducing_interest_with_extension(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)

    a = User(
        id=1836540,
        performed_by=123,
    )
    session.add(a)
    session.flush()

    # assign card
    uc = create_user_product(
        session=session,
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        # 16th March actual
        card_activation_date=parse_date("2020-03-01").date(),
        lender_id=62311,
        interest_type="reducing",
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-03-19 21:33:53"),
        amount=Decimal(10),
        description="TRUEBALANCE IO         GURGAON       IND",
        txn_ref_no="n",
        trace_no="123456",
    )

    create_card_swipe(
        session=session,
        user_loan=uc,
        txn_time=parse_date("2020-03-24 14:01:35"),
        amount=Decimal(100),
        description="PAY*TRUEBALANCE IO     GURGAON       IND",
        txn_ref_no="k",
        trace_no="123456",
    )

    user_loan = get_user_product(session, a.id)
    assert user_loan is not None
    bill_march = bill_generate(user_loan)

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-03 17:41:43"),
        amount=Decimal(4),
        description="TRUEBALANCE IO         GURGAON       IND",
        txn_ref_no="o",
        trace_no="123456",
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-04-12 22:02:47"),
        amount=Decimal(52),
        description="PAYU PAYMENTS PVT LTD  0001243054000 IND",
        txn_ref_no="j",
        trace_no="123456",
    )

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_march.table.bill_due_date + relativedelta(days=1), user_loan
    )

    bill_april = bill_generate(user_loan)
    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_april.table.bill_due_date + relativedelta(days=1), user_loan
    )

    # Extend tenure to 18 months
    extend_schedule(user_loan, 18, parse_date("2020-05-22"))

    emis = user_loan.get_loan_schedule()
    assert emis[0].total_due_amount == Decimal("12")
    assert emis[0].principal_due == Decimal("7.75")
    assert emis[0].interest_due == Decimal("4.25")
    assert emis[0].emi_number == 1
    assert emis[0].total_closing_balance == Decimal(110)
    assert emis[1].total_due_amount == Decimal("18")  # 12 from first bill. 6 from 2nd.
    assert emis[1].principal_due == Decimal("11.93")
    assert emis[1].interest_due == Decimal("6.07")
    assert emis[1].emi_number == 2
    assert emis[1].total_closing_balance == Decimal("158.25")
    assert emis[2].total_due_amount == Decimal("12")  # post extension. 8 from first bill. 4 from 2nd.
    assert emis[2].principal_due == Decimal("7.07")
    assert emis[2].interest_due == Decimal("4.93")
    assert emis[2].emi_number == 3
    assert emis[2].total_closing_balance == Decimal("146.32")
    assert emis[18].total_due_amount == Decimal("4")
    assert emis[18].principal_due == Decimal("3.84")
    assert emis[18].interest_due == Decimal("0.16")
    # First cycle 18 emis, next bill 19 emis
    assert emis[18].emi_number == 19
    assert emis[18].total_closing_balance == Decimal("3.84")


def test_intermediate_bill_generation(session: Session) -> None:
    test_card_swipe_and_reversal(session)
    user_loan = get_user_product(session, 2)
    assert user_loan is not None
    bill_1 = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert latest_bill.bill_start_date == parse_date("2020-05-01").date()
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
        txn_ref_no="p",
        trace_no="123456",
    )

    bill_2 = bill_generate(user_loan)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(session, bill_2.table.bill_due_date + relativedelta(days=1), user_loan)

    assert (
        session.query(LedgerLoanData)
        .filter(LedgerLoanData.loan_id == user_loan.loan_id, LedgerLoanData.is_generated.is_(True))
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
        rc_rate_of_interest_monthly=Decimal(3),
        user_id=a.id,
        lender_id=62311,
        tenure=12,
    )

    # Swipe before activation
    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
        txn_ref_no="q",
        trace_no="123456",
    )

    assert swipe["result"] == "error"


def test_excess_payment_in_future_emis(session: Session) -> None:
    test_generate_bill_1(session)

    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    payment_date = parse_date("2020-05-03")
    amount = Decimal(450)  # min is 114. Paying for 3 emis. Touching 4th.
    payment_request_id = "s3234"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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
        txn_ref_no="r",
        trace_no="123456",
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
    assert user_loan is not None
    payment_date = parse_date("2020-05-03")
    amount = Decimal("113.50")  # min is 114. Paying half paisa less.
    payment_request_id = "s32224"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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
    amount = Decimal(10)
    payment_request_id = "f1234"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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


def test_get_product_class() -> None:
    ruby_klass = get_product_class(card_type="ruby")
    assert ruby_klass is RubyCard

    zeta_klass = get_product_class(card_type="zeta_card")
    assert zeta_klass is ZetaCard


def test_readjust_future_payment_with_new_swipe(session: Session) -> None:
    test_generate_bill_1(session)

    user_loan = get_user_product(session, 99)
    assert user_loan is not None
    payment_date = parse_date("2020-05-03")
    amount = Decimal(228)
    payment_request_id = "s3234"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[1].payment_status == "Paid"
    assert emis[2].payment_status == "UnPaid"

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(2000),
        description="BigBasket.com",
        txn_ref_no="s",
        trace_no="123456",
    )

    bill_generate(user_loan=user_loan)

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[1].payment_status == "UnPaid"
    assert emis[1].payment_received == Decimal("114.00")
    assert emis[1].total_due_amount == Decimal("341.00")


def test_readjust_future_payment_with_extension(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
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
        card_activation_date=parse_date("2020-10-01").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-02 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
        txn_ref_no="t",
        trace_no="123456",
    )
    assert swipe["result"] == "success"

    bill_generate(user_loan=user_loan)

    payment_date = parse_date("2020-10-03")
    amount = Decimal(228)
    payment_request_id = "s3234"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[1].payment_status == "Paid"
    assert emis[2].payment_status == "UnPaid"

    extend_schedule(user_loan=user_loan, new_tenure=15, from_date=parse_date("2020-10-04"))

    emis = user_loan.get_loan_schedule()
    assert emis[0].payment_status == "Paid"
    assert emis[1].payment_status == "Paid"
    assert emis[2].payment_status == "UnPaid"
    assert emis[2].payment_received == Decimal("34.00")


def test_customer_fee_refund(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-11-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
        txn_ref_no="u",
        trace_no="123456",
    )

    bill = bill_generate(user_loan=user_loan)

    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None

    bill = accrue_late_charges(session, user_loan, parse_date("2020-11-15"), Decimal(118))

    fee_due = (
        session.query(Fee)
        .filter(Fee.identifier_id == bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )
    assert fee_due is not None
    assert fee_due.net_amount == Decimal(100)
    assert fee_due.gross_amount == Decimal(118)

    payment_date = parse_date("2020-11-15")
    amount = Decimal(1118)
    payment_request_id = "s33234"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    bill_fee = session.query(Fee).filter_by(id=fee_due.id).one_or_none()
    assert bill_fee is not None
    assert bill_fee.fee_status == "PAID"
    assert bill_fee.net_amount_paid == Decimal(100)
    assert bill_fee.gross_amount_paid == Decimal(118)

    status = remove_fee(
        session=session,
        user_loan=user_loan,
        fee=bill_fee,
    )
    assert status["result"] == "success"

    fee = session.query(Fee).filter_by(id=fee_due.id).one_or_none()
    assert fee is not None
    assert fee.fee_status == "REMOVED"


def test_customer_prepayment_refund(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-11-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
        txn_ref_no="v",
        trace_no="123456",
    )

    bill_generate(user_loan=user_loan)

    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None

    payment_date = parse_date("2020-11-13")
    amount = Decimal(5000)
    payment_request_id = "a12318"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )

    assert prepayment_amount == Decimal(4000)

    refund_payment_request_id = "a12319"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(3000),
        user_id=user_loan.user_id,
        payment_request_id=refund_payment_request_id,
        collection_by="customer_refund",
        payment_received_in_bank_date=payment_date,
    )

    customer_prepayment_refund(
        session=session,
        user_loan=user_loan,
        payment_request_id=refund_payment_request_id,
        refund_source="payment_gateway",
    )

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )
    assert prepayment_amount == Decimal(1000)

    refund_payment_request_id_2 = "a12320"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(1000),
        user_id=user_loan.user_id,
        payment_request_id=refund_payment_request_id_2,
        collection_by="customer_refund",
        payment_received_in_bank_date=payment_date,
    )

    customer_prepayment_refund(
        session=session,
        user_loan=user_loan,
        payment_request_id=refund_payment_request_id_2,
        refund_source="NEFT",
    )

    _, prepayment_amount = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )
    assert prepayment_amount == Decimal(0)


def test_find_split_to_slide_in_loan(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-11-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(1000),
        description="BigB.com",
        txn_ref_no="w",
        trace_no="123456",
    )
    bill = bill_generate(user_loan=user_loan)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )
    accrue_late_charges(session, user_loan, parse_date("2020-11-16"), Decimal(118))

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-12-04 19:23:11"),
        amount=Decimal(1200),
        description="BigB.com",
        txn_ref_no="x",
        trace_no="1234567",
    )
    bill = bill_generate(user_loan=user_loan)

    accrue_interest_on_all_bills(
        session=session, post_date=bill.table.bill_due_date + relativedelta(days=1), user_loan=user_loan
    )
    accrue_late_charges(session, user_loan, parse_date("2020-12-16"), Decimal(118))

    payment_split_info = find_split_to_slide_in_loan(session, user_loan, Decimal(150))
    assert len(payment_split_info) == 2
    assert payment_split_info[0]["type"] == "fee"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal(75)

    payment_split_info = find_split_to_slide_in_loan(session, user_loan, Decimal(336))
    assert len(payment_split_info) == 6
    assert payment_split_info[0]["type"] == "fee"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal(118)
    assert payment_split_info[2]["type"] == "interest"
    assert payment_split_info[2]["amount_to_adjust"] == Decimal("61.34")
    assert payment_split_info[3]["amount_to_adjust"] == Decimal("36")
    assert payment_split_info[4]["type"] == "principal"
    assert payment_split_info[4]["amount_to_adjust"] == Decimal("1.21")
    assert payment_split_info[5]["type"] == "principal"
    assert payment_split_info[5]["amount_to_adjust"] == Decimal("1.45")


def test_payment_split_for_multiple_fees_of_multiple_types(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-10-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-04 19:23:11"),
        amount=Decimal(1200),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="y",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-22 20:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="z",
        trace_no="1234567",
    )
    oct_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-10-31"))

    accrue_interest_on_all_bills(
        session=session,
        post_date=oct_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )
    accrue_late_charges(session, user_loan, parse_date("2020-11-16"), Decimal(118))

    oct_late_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == oct_bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )

    assert oct_late_fee.remaining_fee_amount == Decimal(118)

    oct_atm_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == oct_bill.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )

    assert oct_atm_fee.remaining_fee_amount == Decimal("87.32")

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(500),
        description="Flipkart.com",
        txn_ref_no="dsa",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-15 19:23:11"),
        amount=Decimal(800),
        description="Amazon.com",
        txn_ref_no="dsad",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-24 16:29:25"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="fsfsaf",
        trace_no="1234567",
    )
    nov_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-11-30"))

    accrue_interest_on_all_bills(
        session=session,
        post_date=nov_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )
    accrue_late_charges(session, user_loan, parse_date("2020-12-16"), Decimal(118))

    nov_late_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == nov_bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )

    assert nov_late_fee.remaining_fee_amount == Decimal(118)

    nov_atm_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == nov_bill.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )

    assert nov_atm_fee.remaining_fee_amount == Decimal("33.04")

    payment_split_info = find_split_to_slide_in_loan(session, user_loan, Decimal(150))
    assert len(payment_split_info) == 4
    assert payment_split_info[0]["type"] == "fee"
    assert payment_split_info[0]["fee"].name == "atm_fee"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal("87.32")

    assert payment_split_info[1]["type"] == "fee"
    assert payment_split_info[1]["fee"].name == "atm_fee"
    assert payment_split_info[1]["amount_to_adjust"] == Decimal("33.04")

    assert payment_split_info[2]["type"] == "fee"
    assert payment_split_info[2]["fee"].name == "late_fee"
    assert payment_split_info[2]["amount_to_adjust"] == Decimal("14.82")

    assert payment_split_info[3]["type"] == "fee"
    assert payment_split_info[3]["fee"].name == "late_fee"
    assert payment_split_info[3]["amount_to_adjust"] == Decimal("14.82")


def test_payment_split_for_multiple_loan_and_bill_fees(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-10-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    activation_fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2020-08-01 00:00:00"),
        gross_amount=Decimal("100"),
        include_gst_from_gross_amount=False,
        fee_name="card_activation_fees",
    )
    session.flush()

    assert activation_fee.identifier_id == user_loan.loan_id
    assert activation_fee.fee_status == "UNPAID"
    assert activation_fee.gross_amount == Decimal(118)

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-04 19:23:11"),
        amount=Decimal(1200),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-22 20:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="1234567",
    )
    oct_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-11-01"))

    accrue_interest_on_all_bills(
        session=session,
        post_date=oct_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )
    accrue_late_charges(session, user_loan, parse_date("2020-11-16"), Decimal(118))

    oct_late_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == oct_bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )

    assert oct_late_fee.remaining_fee_amount == Decimal(118)

    oct_atm_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == oct_bill.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )

    assert oct_atm_fee.remaining_fee_amount == Decimal("87.32")

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(500),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no2",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-15 19:23:11"),
        amount=Decimal(800),
        description="Amazon.com",
        txn_ref_no="dummy_txn_ref_no3",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-24 16:29:25"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no4",
        trace_no="1234567",
    )
    nov_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-12-01"))

    accrue_interest_on_all_bills(
        session=session,
        post_date=nov_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )
    accrue_late_charges(session, user_loan, parse_date("2020-12-16"), Decimal(118))

    nov_late_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == nov_bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )

    assert nov_late_fee.remaining_fee_amount == Decimal(118)

    nov_atm_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == nov_bill.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )

    assert nov_atm_fee.remaining_fee_amount == Decimal("33.04")

    reload_fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        gross_amount=Decimal("100"),
        post_date=parse_date("2020-08-01 00:00:00"),
        fee_name="card_reload_fees",
        include_gst_from_gross_amount=True,
    )

    assert reload_fee.identifier_id == user_loan.loan_id
    assert reload_fee.fee_status == "UNPAID"
    assert reload_fee.gross_amount == Decimal(100)

    payment_split_info = find_split_to_slide_in_loan(session, user_loan, Decimal(500))
    assert len(payment_split_info) == 6
    assert "bill" not in payment_split_info[0]
    assert payment_split_info[0]["type"] == "fee"
    assert payment_split_info[0]["fee"].name == "card_activation_fees"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal("118")

    assert "bill" not in payment_split_info[1]
    assert payment_split_info[1]["type"] == "fee"
    assert payment_split_info[1]["fee"].name == "card_reload_fees"
    assert payment_split_info[1]["amount_to_adjust"] == Decimal("100")

    assert "bill" in payment_split_info[2]
    assert payment_split_info[2]["type"] == "fee"
    assert payment_split_info[2]["fee"].name == "atm_fee"
    assert payment_split_info[2]["amount_to_adjust"] == Decimal("87.32")

    assert "bill" in payment_split_info[3]
    assert payment_split_info[3]["type"] == "fee"
    assert payment_split_info[3]["fee"].name == "atm_fee"
    assert payment_split_info[3]["amount_to_adjust"] == Decimal("33.04")

    assert "bill" in payment_split_info[4]
    assert payment_split_info[4]["type"] == "fee"
    assert payment_split_info[4]["fee"].name == "late_fee"
    assert payment_split_info[4]["amount_to_adjust"] == Decimal("80.82")

    assert "bill" in payment_split_info[5]
    assert payment_split_info[5]["type"] == "fee"
    assert payment_split_info[5]["fee"].name == "late_fee"
    assert payment_split_info[5]["amount_to_adjust"] == Decimal("80.82")


def test_payment_split_for_unknown_fee(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-10-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    activation_fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2020-08-01 00:00:00"),
        gross_amount=Decimal("100"),
        include_gst_from_gross_amount=False,
        fee_name="card_activation_fees",
    )
    session.flush()

    assert activation_fee.identifier_id == user_loan.loan_id
    assert activation_fee.fee_status == "UNPAID"
    assert activation_fee.gross_amount == Decimal(118)

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-04 19:23:11"),
        amount=Decimal(1200),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-22 20:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="1234567",
    )
    oct_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-11-01"))

    accrue_interest_on_all_bills(
        session=session,
        post_date=oct_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )
    accrue_late_charges(session, user_loan, parse_date("2020-11-16"), Decimal(118))

    oct_late_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == oct_bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )

    assert oct_late_fee.remaining_fee_amount == Decimal(118)

    oct_atm_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == oct_bill.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )

    assert oct_atm_fee.remaining_fee_amount == Decimal("87.32")

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(500),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no2",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-15 19:23:11"),
        amount=Decimal(800),
        description="Amazon.com",
        txn_ref_no="dummy_txn_ref_no3",
        trace_no="1234567",
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-24 16:29:25"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
        source="ATM",
        txn_ref_no="dummy_txn_ref_no4",
        trace_no="1234567",
    )
    nov_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-12-01"))

    accrue_interest_on_all_bills(
        session=session,
        post_date=nov_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )
    accrue_late_charges(session, user_loan, parse_date("2020-12-16"), Decimal(118))

    nov_late_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == nov_bill.id, Fee.identifier == "bill", Fee.name == "late_fee")
        .one_or_none()
    )

    assert nov_late_fee.remaining_fee_amount == Decimal(118)

    nov_atm_fee = (
        session.query(Fee)
        .filter(Fee.identifier_id == nov_bill.id, Fee.identifier == "bill", Fee.name == "atm_fee")
        .one_or_none()
    )

    assert nov_atm_fee.remaining_fee_amount == Decimal("33.04")

    reload_fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        gross_amount=Decimal("100"),
        post_date=parse_date("2020-08-01 00:00:00"),
        fee_name="card_reload_fees",
        include_gst_from_gross_amount=True,
    )

    # Creating a new, unknown fee
    dummy_fee_event = LedgerTriggerEvent.ledger_new(
        session=session,
        name="unknown_fee_payment",
        loan_id=user_loan.id,
        amount=100,
        post_date=parse_date("2020-08-02 00:00:00"),
    )
    session.flush()

    create_loan_fee_entry(
        session=session,
        fee_name="unknown",
        user_loan=user_loan,
        gross_fee_amount=Decimal(100),
        event=dummy_fee_event,
    )

    assert reload_fee.identifier_id == user_loan.loan_id
    assert reload_fee.fee_status == "UNPAID"
    assert reload_fee.gross_amount == Decimal(100)

    payment_split_info = find_split_to_slide_in_loan(session, user_loan, Decimal(600))
    assert len(payment_split_info) == 7
    assert "bill" not in payment_split_info[0]
    assert payment_split_info[0]["type"] == "fee"
    assert payment_split_info[0]["fee"].name == "card_activation_fees"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal("118")

    assert "bill" not in payment_split_info[1]
    assert payment_split_info[1]["type"] == "fee"
    assert payment_split_info[1]["fee"].name == "card_reload_fees"
    assert payment_split_info[1]["amount_to_adjust"] == Decimal("100")

    assert "bill" in payment_split_info[2]
    assert payment_split_info[2]["type"] == "fee"
    assert payment_split_info[2]["fee"].name == "atm_fee"
    assert payment_split_info[2]["amount_to_adjust"] == Decimal("87.32")

    assert "bill" in payment_split_info[3]
    assert payment_split_info[3]["type"] == "fee"
    assert payment_split_info[3]["fee"].name == "atm_fee"
    assert payment_split_info[3]["amount_to_adjust"] == Decimal("33.04")

    assert "bill" in payment_split_info[4]
    assert payment_split_info[4]["type"] == "fee"
    assert payment_split_info[4]["fee"].name == "late_fee"
    assert payment_split_info[4]["amount_to_adjust"] == Decimal("118")

    assert "bill" in payment_split_info[5]
    assert payment_split_info[5]["type"] == "fee"
    assert payment_split_info[5]["fee"].name == "late_fee"
    assert payment_split_info[5]["amount_to_adjust"] == Decimal("118")

    # Unknown fee is slid last
    assert payment_split_info[6]["type"] == "fee"
    assert payment_split_info[6]["fee"].name == "unknown"
    assert payment_split_info[6]["amount_to_adjust"] == Decimal("25.64")


def test_refund_fee_payment_to_customer(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_product = create_user_product_mapping(
        session=session, user_id=user.id, product_type="term_loan_reset"
    )

    create_loan(session=session, user_product=user_product, lender_id=62311)
    user_loan = get_user_product(
        session=session,
        user_id=user_product.user_id,
        card_type="term_loan_reset",
        user_product_id=user_product.id,
    )

    reset_joining_fees = create_loan_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2019-02-01 00:00:00"),
        gross_amount=Decimal(100),
        include_gst_from_gross_amount=False,
        fee_name="reset_joining_fees",
    )

    assert reset_joining_fees.fee_status == "UNPAID"
    assert reset_joining_fees.gross_amount == Decimal(118)

    payment_date = parse_date("2020-10-03")
    amount = Decimal(118)
    payment_request_id = "refund_fee_payment_request_id"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )

    assert reset_joining_fees.fee_status == "PAID"

    _, cgst_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.user_id}/user/cgst_payable/l"
    )
    assert cgst_payable == Decimal(9)

    _, sgst_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.user_id}/user/sgst_payable/l"
    )
    assert sgst_payable == Decimal(9)

    _, igst_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.user_id}/user/igst_payable/l"
    )
    assert igst_payable == Decimal(0)

    _, fees = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/reset_joining_fees/r"
    )
    assert fees == Decimal(100)

    from rush.payments import refund_payment_to_customer

    resp = refund_payment_to_customer(session=session, payment_request_id=payment_request_id)
    assert resp["result"] == "success"

    _, cgst_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.user_id}/user/cgst_payable/l"
    )
    assert cgst_payable == Decimal(0)

    _, sgst_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.user_id}/user/sgst_payable/l"
    )
    assert sgst_payable == Decimal(0)

    _, igst_payable = get_account_balance_from_str(
        session, book_string=f"{user_loan.user_id}/user/igst_payable/l"
    )
    assert igst_payable == Decimal(0)

    _, fees = get_account_balance_from_str(
        session, book_string=f"{user_loan.loan_id}/loan/reset_joining_fees/r"
    )
    assert fees == Decimal(0)


def test_updated_emi_payment_mapping_after_early_loan_close(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-11-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(2000),
        description="BigB.com",
        txn_ref_no="dsads",
        trace_no="123456",
    )

    bill_generate(user_loan=user_loan)

    dec_bill = user_loan.get_latest_bill()
    assert dec_bill.bill_due_date == parse_date("2020-12-15").date()

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{dec_bill.id}/bill/principal_receivable/a"
    )
    assert billed_amount == 2000

    _, min_amount = get_account_balance_from_str(session, book_string=f"{dec_bill.id}/bill/min/a")
    assert min_amount == 227

    payment_request_id = "a1231909"
    amount = Decimal(227)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=parse_date("2020-12-13")
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    accrue_interest_on_all_bills(
        session=session,
        post_date=dec_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{dec_bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("60.33")

    pm = (
        session.query(PaymentMapping)
        .filter(
            PaymentMapping.payment_request_id == payment_request_id,
            PaymentMapping.row_status == "active",
        )
        .one_or_none()
    )

    assert pm.amount_settled == Decimal("227.00")

    jan_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2020-12-31"))
    assert jan_bill.table.bill_due_date == parse_date("2021-01-15").date()

    payment_request_id = "a12319"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(500),
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=parse_date("2021-01-13")
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == Decimal(500)

    accrue_interest_on_all_bills(
        session=session,
        post_date=jan_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{dec_bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("60.33")

    pm = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.payment_request_id == "a12319", PaymentMapping.row_status == "active")
        .all()
    )

    assert pm[0].amount_settled == Decimal("227.00")
    assert pm[1].amount_settled == Decimal("227.00")
    assert pm[2].amount_settled == Decimal("46.00")

    feb_bill = bill_generate(user_loan=user_loan, creation_time=parse_date("2021-01-31"))
    assert feb_bill.table.bill_due_date == parse_date("2021-02-15").date()

    payment_request_id = "a12320"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(1500),
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=parse_date("2021-02-13")
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == Decimal(1500)

    accrue_interest_on_all_bills(
        session=session,
        post_date=feb_bill.table.bill_due_date + relativedelta(days=1),
        user_loan=user_loan,
    )

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{dec_bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal(0)

    emis = user_loan.get_loan_schedule()
    closing_bill = emis[2]  # loan  should close in third EMI

    amount_settled = (
        session.query(func.sum(PaymentMapping.amount_settled))
        .filter(
            PaymentMapping.payment_request_id.in_(("a12319", "a12320")),
            PaymentMapping.emi_id == closing_bill.id,
            PaymentMapping.row_status == "active",
        )
        .all()
    )

    # This should be 1666.67 but we have an error of 0.01
    assert amount_settled[0][0] == Decimal("1666.66")


def test_moratorium_emi_schedule(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)

    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-08-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-08-04 19:23:11"),
        amount=Decimal(2500),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="123456",
    )
    bill_date = parse_date("2019-08-31").date()
    bill_sep = bill_generate(user_loan=user_loan, creation_time=bill_date)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_sep.table.bill_due_date + relativedelta(days=1), user_loan
    )

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_sep.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("75.67")

    _, interest_accrued = get_account_balance_from_str(
        session, book_string=f"{bill_sep.id}/bill/interest_accrued/r"
    )
    assert interest_accrued == Decimal("75.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(loan_id=user_loan.loan_id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event is not None
    assert interest_event.amount == Decimal("75.67")

    start_date = parse_date("2020-09-15").date()
    end_date = parse_date("2020-11-15").date()
    # Apply moratorium
    provide_moratorium(user_loan, start_date, end_date)

    loan_moratorium = (
        session.query(LoanMoratorium).filter(LoanMoratorium.loan_id == user_loan.loan_id).first()
    )
    assert loan_moratorium is not None
    assert loan_moratorium.due_date_after_moratorium == parse_date("2020-12-15").date()

    moratorium_interest_for_sep = (
        session.query(MoratoriumInterest.interest)
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_sep.table.id,
            LoanSchedule.due_date == bill_sep.table.bill_due_date,
        )
        .scalar()
    )
    assert moratorium_interest_for_sep is not None
    assert moratorium_interest_for_sep == Decimal("75.67")

    total_moratorium_interest_sep_bill = (
        session.query(func.sum(MoratoriumInterest.interest))
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_sep.id,
        )
        .scalar()
    )
    assert total_moratorium_interest_sep_bill is not None
    assert total_moratorium_interest_sep_bill == Decimal("227.01")

    emis = user_loan.get_loan_schedule()

    assert len(emis) == 15
    assert emis[0].emi_number == 1
    assert emis[0].total_due_amount == 0
    assert emis[0].due_date == parse_date("2020-09-15").date()
    assert emis[0].total_closing_balance == Decimal("2500.00")
    assert emis[1].emi_number == 2
    assert emis[1].total_due_amount == 0
    assert emis[1].due_date == parse_date("2020-10-15").date()
    assert emis[1].total_closing_balance == Decimal("2500.00")
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-11-15").date()
    assert emis[2].total_closing_balance == Decimal("2500.00")
    assert emis[3].emi_number == 4
    assert emis[3].principal_due == Decimal("208.33")
    assert emis[3].interest_due == Decimal("302.68")  # Interest of 3 emis + this month's interest.
    assert emis[3].total_due_amount == Decimal("511.01")
    assert emis[3].due_date == parse_date("2020-12-15").date()
    assert emis[3].total_closing_balance == Decimal("2500.00")
    assert emis[4].emi_number == 5
    assert emis[4].principal_due == Decimal("208.33")
    assert emis[4].interest_due == Decimal("75.67")
    assert emis[4].due_date == parse_date("2021-01-15").date()
    assert emis[4].total_closing_balance == Decimal("2291.67")

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-09-04 19:23:11"),
        amount=Decimal(2500),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123456",
    )

    bill_date = parse_date("2019-09-30").date()
    bill_oct = bill_generate(user_loan=user_loan, creation_time=bill_date)
    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_oct.table.bill_due_date + relativedelta(days=1), user_loan
    )

    _, sep_interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_sep.id}/bill/interest_receivable/a"
    )
    assert sep_interest_due == Decimal("151.34")

    _, sep_interest_accrued = get_account_balance_from_str(
        session, book_string=f"{bill_sep.id}/bill/interest_accrued/r"
    )
    assert sep_interest_accrued == Decimal("151.34")

    _, oct_interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_oct.id}/bill/interest_receivable/a"
    )
    assert oct_interest_due == Decimal("75.67")

    _, oct_interest_accrued = get_account_balance_from_str(
        session, book_string=f"{bill_oct.id}/bill/interest_accrued/r"
    )
    assert oct_interest_accrued == Decimal("75.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(loan_id=user_loan.loan_id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event is not None
    assert interest_event.amount == Decimal("151.34")

    moratorium_interest_for_oct = (
        session.query(MoratoriumInterest.interest)
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_oct.table.id,
            LoanSchedule.due_date == bill_oct.table.bill_due_date,
        )
        .scalar()
    )
    assert moratorium_interest_for_oct is not None
    assert moratorium_interest_for_oct == Decimal("75.67")

    total_moratorium_interest_oct_bill = (
        session.query(func.sum(MoratoriumInterest.interest))
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_oct.id,
        )
        .scalar()
    )
    assert total_moratorium_interest_oct_bill is not None
    assert total_moratorium_interest_oct_bill == Decimal("151.34")

    total_moratorium_interest_accrued_till_oct = (
        session.query(func.sum(MoratoriumInterest.interest))
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            MoratoriumInterest.moratorium_id == loan_moratorium.id,
            LoanSchedule.due_date <= bill_oct.table.bill_due_date,
        )
        .scalar()
    )
    assert total_moratorium_interest_accrued_till_oct is not None
    assert total_moratorium_interest_accrued_till_oct == Decimal("227.01")

    emis = user_loan.get_loan_schedule()

    assert len(emis) == 15
    assert emis[0].emi_number == 1
    assert emis[0].total_due_amount == 0
    assert emis[0].due_date == parse_date("2020-09-15").date()
    assert emis[0].total_closing_balance == Decimal("2500.00")
    assert emis[1].emi_number == 2
    assert emis[1].total_due_amount == 0
    assert emis[1].due_date == parse_date("2020-10-15").date()
    assert emis[1].total_closing_balance == Decimal("5000.00")
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-11-15").date()
    assert emis[2].total_closing_balance == Decimal("5000.00")
    assert emis[3].emi_number == 4
    assert emis[3].principal_due == Decimal("416.90")
    assert emis[3].interest_due == Decimal("529.44")
    assert emis[3].total_due_amount == Decimal("946.34")
    assert emis[3].due_date == parse_date("2020-12-15").date()
    assert emis[3].total_closing_balance == Decimal("5000.00")
    assert emis[4].emi_number == 5
    assert emis[4].principal_due == Decimal("416.90")
    assert emis[4].interest_due == Decimal("151.10")
    assert emis[4].due_date == parse_date("2021-01-15").date()
    assert emis[4].total_closing_balance == Decimal("4583.10")

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-10-04 19:23:11"),
        amount=Decimal(2500),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_3",
        trace_no="123456",
    )

    bill_date = parse_date("2020-10-31").date()
    bill_nov = bill_generate(user_loan=user_loan, creation_time=bill_date)
    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_nov.table.bill_due_date + relativedelta(days=1), user_loan
    )

    _, nov_interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_nov.id}/bill/interest_receivable/a"
    )
    assert nov_interest_due == Decimal("75.67")

    _, nov_interest_accrued = get_account_balance_from_str(
        session, book_string=f"{bill_nov.id}/bill/interest_accrued/r"
    )
    assert nov_interest_accrued == Decimal("75.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(loan_id=user_loan.loan_id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event is not None
    assert interest_event.amount == Decimal("227.01")

    moratorium_interest_for_nov = (
        session.query(MoratoriumInterest.interest)
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_nov.table.id,
            LoanSchedule.due_date == bill_nov.table.bill_due_date,
        )
        .scalar()
    )
    assert moratorium_interest_for_nov is not None
    assert moratorium_interest_for_nov == Decimal("75.67")

    total_moratorium_interest_nov_bill = (
        session.query(func.sum(MoratoriumInterest.interest))
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_nov.id,
        )
        .scalar()
    )
    assert total_moratorium_interest_nov_bill is not None
    assert total_moratorium_interest_nov_bill == Decimal("75.67")

    total_moratorium_interest_accrued_till_nov = (
        session.query(func.sum(MoratoriumInterest.interest))
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            MoratoriumInterest.moratorium_id == loan_moratorium.id,
            LoanSchedule.due_date <= bill_nov.table.bill_due_date,
        )
        .scalar()
    )
    assert total_moratorium_interest_accrued_till_nov is not None
    assert total_moratorium_interest_accrued_till_nov == Decimal("454.02")

    assert len(emis) == 15
    assert emis[0].emi_number == 1
    assert emis[0].total_due_amount == 0
    assert emis[0].due_date == parse_date("2020-09-15").date()
    assert emis[0].total_closing_balance == Decimal("2500.00")
    assert emis[1].emi_number == 2
    assert emis[1].total_due_amount == 0
    assert emis[1].due_date == parse_date("2020-10-15").date()
    assert emis[1].total_closing_balance == Decimal("5000.00")
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-11-15").date()
    assert emis[2].total_closing_balance == Decimal("7500.00")
    assert emis[3].emi_number == 4
    assert emis[3].principal_due == Decimal("625.21")
    assert emis[3].interest_due == Decimal("680.80")
    assert emis[3].total_due_amount == Decimal("1306.01")
    assert emis[3].due_date == parse_date("2020-12-15").date()
    assert emis[3].total_closing_balance == Decimal("7500.00")
    assert emis[4].emi_number == 5
    assert emis[4].principal_due == Decimal("625.21")
    assert emis[4].interest_due == Decimal("226.79")
    assert emis[4].due_date == parse_date("2021-01-15").date()
    assert emis[4].total_closing_balance == Decimal("6874.79")

    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(2500),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no_4",
        trace_no="123456",
    )

    bill_date = parse_date("2020-11-30").date()
    bill_dec = bill_generate(user_loan=user_loan, creation_time=bill_date)
    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_dec.table.bill_due_date + relativedelta(days=1), user_loan
    )

    _, dec_interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_dec.id}/bill/interest_receivable/a"
    )
    assert dec_interest_due == Decimal("75.67")

    _, dec_interest_accrued = get_account_balance_from_str(
        session, book_string=f"{bill_dec.id}/bill/interest_accrued/r"
    )
    assert dec_interest_accrued == Decimal("75.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(loan_id=user_loan.loan_id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event is not None
    assert interest_event.amount == Decimal("302.45")

    assert len(emis) == 15
    assert emis[0].emi_number == 1
    assert emis[0].total_due_amount == 0
    assert emis[0].due_date == parse_date("2020-09-15").date()
    assert emis[0].total_closing_balance == Decimal("2500.00")
    assert emis[1].emi_number == 2
    assert emis[1].total_due_amount == 0
    assert emis[1].due_date == parse_date("2020-10-15").date()
    assert emis[1].total_closing_balance == Decimal("5000.00")
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-11-15").date()
    assert emis[2].total_closing_balance == Decimal("7500.00")
    assert emis[3].emi_number == 4
    assert emis[3].principal_due == Decimal("833.54")
    assert emis[3].interest_due == Decimal("756.47")
    assert emis[3].total_due_amount == Decimal("1590.01")
    assert emis[3].due_date == parse_date("2020-12-15").date()
    assert emis[3].total_closing_balance == Decimal("10000.00")
    assert emis[4].emi_number == 5
    assert emis[4].principal_due == Decimal("833.54")
    assert emis[4].interest_due == Decimal("302.46")
    assert emis[4].due_date == parse_date("2021-01-15").date()
    assert emis[4].total_closing_balance == Decimal("9166.46")


def test_close_loan_in_moratorium(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)

    user = User(
        id=99,
        performed_by=123,
    )
    session.add(user)
    session.flush()

    user_loan = create_user_product(
        session=session,
        user_id=user.id,
        card_activation_date=parse_date("2020-08-02").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        tenure=12,
    )
    create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-08-04 19:23:11"),
        amount=Decimal(2500),
        description="BigB.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    bill_date = parse_date("2019-08-31").date()
    bill_sep = bill_generate(user_loan=user_loan, creation_time=bill_date)

    # check latest bill method
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    # Interest event to be fired separately now
    accrue_interest_on_all_bills(
        session, bill_sep.table.bill_due_date + relativedelta(days=1), user_loan
    )

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_sep.id}/bill/interest_receivable/a"
    )
    assert interest_due == Decimal("75.67")

    _, interest_accrued = get_account_balance_from_str(
        session, book_string=f"{bill_sep.id}/bill/interest_accrued/r"
    )
    assert interest_accrued == Decimal("75.67")

    interest_event = (
        session.query(LedgerTriggerEvent)
        .filter_by(loan_id=user_loan.loan_id, name="accrue_interest")
        .order_by(LedgerTriggerEvent.post_date.desc())
        .first()
    )
    assert interest_event is not None
    assert interest_event.amount == Decimal("75.67")

    start_date = parse_date("2020-09-15").date()
    end_date = parse_date("2020-11-15").date()
    # Apply moratorium
    provide_moratorium(user_loan, start_date, end_date)

    loan_moratorium = (
        session.query(LoanMoratorium).filter(LoanMoratorium.loan_id == user_loan.loan_id).first()
    )
    assert loan_moratorium is not None

    moratorium_interest_for_sep = (
        session.query(MoratoriumInterest.interest)
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_sep.table.id,
            LoanSchedule.due_date == bill_sep.table.bill_due_date,
        )
        .scalar()
    )
    assert moratorium_interest_for_sep is not None
    assert moratorium_interest_for_sep == Decimal("75.67")

    total_moratorium_interest_sep_bill = (
        session.query(func.sum(MoratoriumInterest.interest))
        .join(LoanSchedule, LoanSchedule.id == MoratoriumInterest.loan_schedule_id)
        .filter(
            LoanSchedule.bill_id == bill_sep.id,
        )
        .scalar()
    )
    assert total_moratorium_interest_sep_bill is not None
    assert total_moratorium_interest_sep_bill == Decimal("227.01")

    emis = user_loan.get_loan_schedule()

    assert len(emis) == 15
    assert emis[0].emi_number == 1
    assert emis[0].total_due_amount == 0
    assert emis[0].due_date == parse_date("2020-09-15").date()
    assert emis[0].total_closing_balance == Decimal("2500.00")
    assert emis[1].emi_number == 2
    assert emis[1].total_due_amount == 0
    assert emis[1].due_date == parse_date("2020-10-15").date()
    assert emis[1].total_closing_balance == Decimal("2500.00")
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-11-15").date()
    assert emis[2].total_closing_balance == Decimal("2500.00")
    assert emis[3].emi_number == 4
    assert emis[3].principal_due == Decimal("208.33")
    assert emis[3].interest_due == Decimal("302.68")  # Interest of 3 emis + this month's interest.
    assert emis[3].total_due_amount == Decimal("511.01")
    assert emis[3].due_date == parse_date("2020-12-15").date()
    assert emis[3].total_closing_balance == Decimal("2500.00")
    assert emis[4].emi_number == 5
    assert emis[4].principal_due == Decimal("208.33")
    assert emis[4].interest_due == Decimal("75.67")
    assert emis[4].due_date == parse_date("2021-01-15").date()
    assert emis[4].total_closing_balance == Decimal("2291.67")

    payment_date = parse_date("2020-09-20")
    payment_request_id = "a12319"
    amount = Decimal("2575.67")
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user.id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(session=session, user_loan=user_loan, payment_request_data=payment_requests_data)

    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    bill = (
        session.query(LedgerLoanData)
        .filter(LedgerLoanData.user_id == user_loan.user_id)
        .order_by(LedgerLoanData.bill_start_date.desc())
        .first()
    )

    is_sep_bill_closed = is_bill_closed(session, bill)
    assert is_sep_bill_closed is True

    future_moratorium_interest_emis = (
        session.query(MoratoriumInterest)
        .filter(
            MoratoriumInterest.moratorium_id == loan_moratorium.id,
            LoanSchedule.id == MoratoriumInterest.loan_schedule_id,
            LoanSchedule.due_date > payment_date,
        )
        .all()
    )

    for moratorium_interest_emi in future_moratorium_interest_emis:
        assert moratorium_interest_emi.interest == Decimal("0")

    emis = user_loan.get_loan_schedule()

    assert len(emis) == 15
    assert emis[0].emi_number == 1
    assert emis[0].total_due_amount == 0
    assert emis[0].due_date == parse_date("2020-09-15").date()
    assert emis[0].total_closing_balance == Decimal("2500.00")
    assert emis[0].payment_status == "UnPaid"
    assert emis[1].emi_number == 2
    assert emis[1].principal_due == Decimal("2500")
    assert emis[1].interest_due == Decimal("75.67")
    assert emis[1].total_due_amount == Decimal("2575.67")
    assert emis[1].due_date == parse_date("2020-10-15").date()
    assert emis[1].total_closing_balance == Decimal("2500.00")
    assert emis[1].payment_status == "Paid"
    assert emis[1].payment_received == Decimal("2500.00")
    assert emis[1].last_payment_date == payment_date
    assert emis[2].emi_number == 3
    assert emis[2].total_due_amount == 0
    assert emis[2].due_date == parse_date("2020-11-15").date()
    assert emis[2].total_closing_balance == Decimal("0")
    assert emis[2].payment_status == "UnPaid"
    assert emis[3].emi_number == 4
    assert emis[3].principal_due == Decimal("0")
    assert emis[3].interest_due == Decimal("0")
    assert emis[3].total_due_amount == Decimal("0")
    assert emis[3].due_date == parse_date("2020-12-15").date()
    assert emis[3].total_closing_balance == Decimal("0")
    assert emis[3].payment_status == "UnPaid"
    assert emis[4].emi_number == 5
    assert emis[4].principal_due == Decimal("0")
    assert emis[4].interest_due == Decimal("0")
    assert emis[4].due_date == parse_date("2021-01-15").date()
    assert emis[4].total_closing_balance == Decimal("0")
    assert emis[4].payment_status == "UnPaid"
