from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from models import LedgerTriggerEvent
from rush.card import get_user_card
from rush.ledger_events import limit_assignment_event
from rush.utils import get_current_ist_time


def card_assignment(session: Session, user_id: int, amount: Decimal) -> bool:
    # amount = Decimal("10000")
    user_card = get_user_card(session, user_id)
    card_id = user_card.id
    lt = LedgerTriggerEvent(name="limit_assignment", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    limit_assignment_event(session, card_id, lt)
    return True


def card_reload(session: Session, user_id: int) -> bool:
    amount = Decimal("10000")
    card_id = None  # Down for now
    lt = LedgerTriggerEvent(name="limit_reload", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    limit_assignment_event(session, card_id, lt)
    return True
