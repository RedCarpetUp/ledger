from rush.card import get_product_class
from rush.card.health_card import HealthCard
from rush.card.ruby_card import RubyCard
from rush.card.term_loan import TermLoan


def test_get_product_class_ruby_card() -> None:
    klass = get_product_class(card_type="ruby")
    assert klass.__mro__[0].__module__ == RubyCard.__module__


def test_get_product_class_health_card() -> None:
    klass = get_product_class(card_type="health_card")
    assert klass.__mro__[0].__module__ == HealthCard.__module__


def test_get_product_class_term_loan() -> None:
    klass = get_product_class(card_type="term_loan")
    assert klass.__mro__[0].__module__ == TermLoan.__module__
