from decimal import Decimal

from pendulum import DateTime
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_card,
    get_user_card,
)
from rush.ledger_events import (
    charge_fee_event,
    limit_assignment_event,
)
from rush.models import LedgerTriggerEvent
from rush.utils import get_current_ist_time


def card_assignment(
    session: Session, user_id: int, lender_id: int, amount: Decimal, fee_amount: Decimal, card_type: str
) -> None:
    # assign card
    user_card = create_user_card(
        session=session,
        user_id=user_id,
        card_activation_date=get_current_ist_time(),
        card_type=card_type,
        lender_id=lender_id,
    )
    card_id = user_card.id

    lt1 = LedgerTriggerEvent(
        name="card_processing_fee", amount=fee_amount, post_date=get_current_ist_time()
    )
    session.add(lt1)
    session.flush()
    charge_fee_event(session, card_id, lender_id, lt1)

    lt = LedgerTriggerEvent(name="limit_assignment", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    limit_assignment_event(session, card_id, lt)


def card_reload(session: Session, user_id: int, amount: Decimal, fee_amount: Decimal) -> None:
    user_card = get_user_card(session, user_id)
    card_id = user_card.id
    lender_id = user_card.lender_id
    lt = LedgerTriggerEvent(
        name="reload_processing_fee", amount=fee_amount, post_date=get_current_ist_time()
    )
    session.add(lt)
    session.flush()
    charge_fee_event(session, card_id, lender_id, lt)

    lt = LedgerTriggerEvent(name="limit_assignment", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    limit_assignment_event(session, card_id, lt)
