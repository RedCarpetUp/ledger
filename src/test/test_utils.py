from rush.card import get_product_class
from rush.card.health_card import HealthCard
from rush.card.ruby_card import RubyCard
from rush.card.term_loan import TermLoan
from rush.card.term_loan2 import TermLoan2
from rush.card.term_loan_pro import TermLoanPro
from rush.card.term_loan_pro2 import TermLoanPro2


def test_get_product_class_ruby_card() -> None:
    klass = get_product_class(card_type="ruby")
    assert klass.__mro__[0].__module__ == RubyCard.__module__


def test_get_product_class_health_card() -> None:
    klass = get_product_class(card_type="health_card")
    assert klass.__mro__[0].__module__ == HealthCard.__module__


def test_get_product_class_term_loan() -> None:
    klass = get_product_class(card_type="term_loan")
    assert klass.__mro__[0].__module__ == TermLoan.__module__


def test_get_product_class_term_loan_pro() -> None:
    klass = get_product_class(card_type="term_loan_pro")
    assert klass.__mro__[0].__module__ == TermLoanPro.__module__


def test_get_product_class_term_loan2() -> None:
    klass = get_product_class(card_type="term_loan_2")
    assert klass.__mro__[0].__module__ == TermLoan2.__module__


def test_get_product_class_term_loan_pro2() -> None:
    klass = get_product_class(card_type="term_loan_pro_2")
    assert klass.__mro__[0].__module__ == TermLoanPro2.__module__
