from decimal import Decimal

from rush.loan_schedule.calculations import get_down_payment


def test_get_down_payment_1() -> None:
    downpayment_amount = get_down_payment(
        principal=Decimal("10000"), down_payment_percentage=Decimal("20"),
    )
    assert downpayment_amount == Decimal("2000")


def test_get_down_payment_amount_with_first_emi() -> None:
    downpayment_amount = get_down_payment(
        principal=Decimal("10000"),
        down_payment_percentage=Decimal("20"),
        interest_rate_monthly=Decimal(3),
        interest_type="flat",
        number_of_instalments=12,
        include_first_emi_amount=True,
    )
    assert downpayment_amount == Decimal("2910")
