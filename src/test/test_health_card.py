from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import create_user_card
from rush.models import (
    CardKitNumbers,
    CardNames,
    Lenders,
    User,
)


def create_lenders(session: Session) -> None:
    dmi = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(dmi)

    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def card_db_updates(session: Session) -> None:
    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()

    ckn = CardKitNumbers(kit_number="10000", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()


def create_user(session: Session) -> None:
    u = User(id=3, performed_by=123,)
    session.add(u)
    session.flush()


def test_create_health_card(session: Session) -> None:
    create_lenders(session)
    card_db_updates(session)
    create_user(session)
    uc = create_user_card(
        session=session,
        user_id=3,
        card_activation_date=parse_date("2020-08-11").date(),
        card_type="health_card",
        lender_id=62311,
        kit_number="10000",
    )

    assert uc.card_type == "health_card"
