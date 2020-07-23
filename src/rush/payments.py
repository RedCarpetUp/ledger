from decimal import Decimal

from dateutil import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import (
    BaseCard,
    get_user_card,
)
from rush.ledger_events import (
    payment_received_event,
    refund_event,
    writeoff_event,
)
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import get_current_ist_time


def payment_received(
    session: Session,
    user_card: BaseCard,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
) -> None:
    lt = LedgerTriggerEvent(
        name="payment_received",
        card_id=user_card.id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={"payment_request_id": payment_request_id, "gateway_charges": 0.5},
    )
    session.add(lt)
    session.flush()
    lender_id = (
        session.query(LoanData.lender_id).filter(LoanData.card_id == user_card.id).limit(1).scalar() or 0
    )
    payment_received_event(session, user_card, f"{lender_id}/lender/pg_account/a", lt)
    run_anomaly(session, user_card, payment_date)


def refund_payment(session, user_id: int, bill_id: int, refund_request_id: str) -> bool:

    bill = (
        session.query(CardTransaction.amount)
        .join(LoanData, CardTransaction.loan_id == LoanData.id)
        .filter(LoanData.id == bill_id)
        .first()
    )
    user_card = get_user_card(session, user_id)
    amount = Decimal(bill.amount)
    lt = LedgerTriggerEvent(
        name="refund_bill",
        amount=amount,
        post_date=get_current_ist_time(),
        extra_details={"payment_request_id": refund_request_id},
    )
    session.add(lt)
    session.flush()
    current_bill = session.query(LoanData).filter(LoanData.id == bill_id).one()
    refund_event(session, current_bill, user_card, lt)
    return True


def writeoff_payment(session: Session, user_id: int) -> bool:

    usercard = get_user_card(session, 99)
    if _check_writeoff(session, user_id, usercard):
        _, balance = get_account_balance_from_str(
            session, book_string=f"{usercard.id}/card/lender_payable/l"
        )
        lt = LedgerTriggerEvent(
            name="writeoff_payment", amount=balance, post_date=get_current_ist_time()
        )
        session.add(lt)
        session.flush()
        writeoff_event(session, usercard, lt)
        return True
    else:
        return False


def _check_writeoff(session, user_id: int, user_card: BaseCard) -> bool:

    unpaid_bills = user_card.get_unpaid_bills()
    if len(unpaid_bills) >= 1:
        relative = relativedelta.relativedelta(get_current_ist_time(), unpaid_bills[0].bill_start_date)
        months = relative.months + (12 * relative.years)
        if months >= 3:
            return True
        else:
            return False
