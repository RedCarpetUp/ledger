from sqlalchemy.orm import Session

from rush.models import Product


def get_product_id_from_card_type(session: Session, card_type: str) -> int:
    return session.query(Product.id).filter(Product.product_name == card_type,).scalar()
