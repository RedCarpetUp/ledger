from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import BaseCard
from rush.ledger_events import generate_invoice_event
from rush.ledger_utils import get_account_balance_from_str
from rush.models import LedgerTriggerEvent
from rush.utils import get_current_ist_time


def invoice_generation(session: Session, user_card: BaseCard) -> None:
    all_bills = user_card.get_unpaid_bills()
    amount = Decimal("0")
    for bill in all_bills:
        _, interest_revenue = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/interest_earned/r"
        )
        _, late_fine_revenue = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/late_fine/r"
        )
        amount = amount + interest_revenue + late_fine_revenue
    lt = LedgerTriggerEvent(name="invoice_generation", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    generate_invoice_event(session, 12345, all_bills[0].lender_id, lt)
