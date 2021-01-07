from decimal import Decimal

from sqlalchemy.orm.session import Session
from sqlalchemy.sql.sqltypes import DateTime

from rush.card import create_user_product
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.card.transaction_loan import (
    TransactionLoan,
    TransactionLoanBill,
)
from rush.card.utils import create_user_product_mapping
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)
from rush.payments import payment_received
from rush.utils import get_current_ist_time


def transaction_to_loan(
    session: Session, txn_id: int, user_id: int, post_date: DateTime
) -> TransactionLoan:
    txn: CardTransaction = session.query(CardTransaction).filter(CardTransaction.id == txn_id).scalar()

    if not txn:
        return {"result": "error", "message": "Invalid Transaction ID"}

    # checking if bill is already generated for this txn
    bill: LoanData = session.query(LoanData).filter(LoanData.id == txn.loan_id).scalar()

    if bill.is_generated:
        return {"result": "error", "message": "Bill for this transaction has already been generated."}

    user_loan: BaseLoan = (
        session.query(BaseLoan)
        .join(LoanData, LoanData.loan_id == BaseLoan.id)
        .join(CardTransaction, CardTransaction.loan_id == LoanData.id)
        .filter(CardTransaction.id == txn_id)
        .scalar()
    )

    user_product = create_user_product_mapping(
        session=session, user_id=user_id, product_type="transaction_loan"
    )

    # loan for txn amount
    txn_loan = create_user_product(
        session=session,
        user_id=user_id,
        card_type="transaction_loan",
        lender_id=user_loan.lender_id,
        interest_free_period_in_days=15,
        tenure=12,
        amount=txn.amount,
        product_order_date=get_current_ist_time().date(),
        user_product_id=user_product.id,
        downpayment_percent=Decimal("0"),
        credit_book=f"{txn.loan_id}/bill/unbilled/a",
        parent_loan_id=user_loan.id,
    )

    txn_loan_bill: LoanData = session.query(LoanData).filter(LoanData.loan_id == txn_loan.id).scalar()
    txn.loan_id = txn_loan_bill.id

    session.flush()

    return {"result": "success", "data": txn_loan}
