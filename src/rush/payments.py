from decimal import Decimal

from pendulum import DateTime
from dateutil import relativedelta
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.ledger_events import (
    payment_received_event,
    refund_event,
    writeoff_event,
)
from rush.ledger_utils import (
    get_account_balance_from_str,
    get_all_unpaid_bills,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import get_current_ist_time
from rush.views import user_view


def payment_received(
    session: Session, user_card: UserCard, payment_amount: Decimal, payment_date: DateTime
) -> None:

    _, writeoff_amount = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/writeoff_expenses/e"
    )
    lt = LedgerTriggerEvent(
        name="payment_received", card_id=user_card.id, amount=payment_amount, post_date=payment_date
    )
    session.add(lt)
    session.flush()
    if writeoff_amount > 0:
        payment_received_event(session, user_card, "recovery", lt)
    else:
        payment_received_event(session, user_card, "payment", lt)
    run_anomaly(session, user_card, payment_date)


def refund_payment(session, user_id: int, bill_id: int) -> bool:

    bill = (
        session.query(CardTransaction.amount)
        .join(LoanData, CardTransaction.loan_id == LoanData.id)
        .filter(LoanData.id == bill_id)
        .first()
    )
    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).one()
    amount = Decimal(bill.amount)
    lt = LedgerTriggerEvent(name="refund_bill", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    current_bill = session.query(LoanData).filter(LoanData.id == bill_id).one()
    refund_event(session, current_bill, user_card, lt)
    return True


def writeoff_payment(session: Session, user_id: int) -> bool:

    unpaid_bills = get_all_unpaid_bills(session, user_id)
    if len(unpaid_bills) >= 1:
        r = relativedelta.relativedelta(get_current_ist_time(), unpaid_bills[0].agreement_date)
        months = r.months + (12 * r.years)
        if months >= 3:
            usercard = session.query(UserCard).filter(UserCard.user_id == user_id).one()
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
    else:
        return False
