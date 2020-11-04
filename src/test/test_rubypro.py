from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    RubyProCard,
    create_user_product,
    get_product_class,
    get_user_product,
)
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.card.utils import (
    add_pre_product_fee,
    create_user_product_mapping,
)
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardKitNumbers,
    CardNames,
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
)


def create_lenders(session: Session) -> None:
    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def create_products(session: Session) -> None:
    hc_product = Product(product_name="rubypro")
    session.add(hc_product)
    session.flush()


def create_user(session: Session) -> None:
    u = User(
        id=4369,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def test_rubpro_user(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(session=session, user_id=4369, product_type="rubypro")
    user_card = create_user_product(
        session=session,
        card_type="rubypro",
        lender_id=1756833,
        interest_free_period_in_days=45,
        user_id=4369,
        user_product_id=user_product.id,
        card_activation_date=parse_date("2020-11-01").date(),
        interest_type="reducing",
    )

    assert user_card.product_type == "rubypro"
    assert user_card.amortization_date == parse_date("2020-11-01").date()

    swipe = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-11-03 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    session.flush()
    bill_id = swipe["data"].loan_id
    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1200
    user_loan = get_user_product(session, 4369, card_type="rubypro")
    bill_date = parse_date("2020-12-01 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-11-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a", to_date=bill_date
    )
    assert billed_amount == 1200
