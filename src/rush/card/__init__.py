from sqlalchemy.orm import Session

from rush.models import UserCard


def create_user_card(session: Session, **kwargs) -> UserCard:
    uc = UserCard(**kwargs)
    session.add(uc)
    session.flush()
    return uc
