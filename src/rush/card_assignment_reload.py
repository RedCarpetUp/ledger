from decimal import Decimal

from pendulum import DateTime
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_card,
    get_user_card,
)
from rush.ledger_events import limit_assignment_event
from rush.ledger_utils import get_account_balance_from_str
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
    )
    card_id = user_card.id

    lt1 = LedgerTriggerEvent(
        name="card_processing_fee", amount=fee_amount, post_date=get_current_ist_time()
    )
    session.add(lt1)
    session.flush()

    amount_rcv = get_account_balance_from_str(session, book_string=f"{card_id}/card/revenue_fee/r")
    if amount_rcv == 0:
        card_limit_assign(amount=amount)


def card_limit_reload(session: Session, user_id: int, amount: Decimal, fee_amount: Decimal) -> None:
    user_card = get_user_card(session, user_id)
    card_id = user_card.id
    lt = LedgerTriggerEvent(
        name="reload_processing_fee", amount=fee_amount, post_date=get_current_ist_time()
    )
    session.add(lt)
    session.flush()

    amount_rcv = get_account_balance_from_str(session, book_string=f"{card_id}/card/revenue_fee/r")
    if amount_rcv == 0:
        card_limit_assign(amount=amount)


def card_limit_assign(amount: Decimal, name="limit_assignment", post_date=get_current_ist_time()):
    lt = LedgerTriggerEvent(name, amount, post_date)
    session.add(lt)
    session.flush()
    limit_assignment_event(session, card_id, lt)
