from decimal import Decimal
from typing import Optional

from rush.utils import (
    round_up,
    round_up_to_ten,
)


def get_down_payment(
    principal: Decimal,
    down_payment_percentage: Decimal,
    interest_rate_monthly: Optional[Decimal] = None,
    interest_type: Optional[str] = None,
    number_of_instalments: Optional[int] = None,
    include_first_emi_amount: Optional[bool] = False,
):
    down_payment_by_percentage = principal * (down_payment_percentage / 100)

    if not include_first_emi_amount or down_payment_by_percentage == 0:
        return round_up_to_ten(down_payment_by_percentage)

    first_emi = get_monthly_instalment(
        principal=principal,
        down_payment_percentage=down_payment_percentage,
        interest_type=interest_type,
        interest_rate_monthly=interest_rate_monthly,
        number_of_instalments=number_of_instalments,
        to_round=False,
    )
    down_payment = round_up_to_ten(down_payment_by_percentage + first_emi)
    return down_payment


def get_monthly_instalment(
    principal: Decimal,
    down_payment_percentage: Decimal,
    interest_type: str,
    interest_rate_monthly: Decimal,
    number_of_instalments: int,
    to_round: bool,
    round_to: Optional[str] = "one",
):
    down_payment = get_down_payment(
        principal=principal,
        down_payment_percentage=down_payment_percentage,
    )
    principal_without_down_payment = principal - down_payment
    if interest_type == "reducing":
        instalment = get_reducing_emi(
            principal_without_down_payment,
            interest_rate_monthly,
            number_of_instalments,
            to_round=to_round,
            round_to=round_to,
        )
    else:
        principal_instalment = principal_without_down_payment / number_of_instalments
        interest = get_interest_to_charge(
            principal=principal_without_down_payment,
            interest_rate_monthly=interest_rate_monthly,
        )
        instalment = principal_instalment + interest
        if to_round:
            instalment = round_up(to=round_to, val=instalment)
    return instalment


def get_reducing_emi(
    principal: Decimal,
    interest_rate_monthly: Decimal,
    tenure: int,
    to_round: Optional[bool] = False,
    round_to: Optional[str] = "one",
) -> Decimal:
    emi = (
        principal
        * interest_rate_monthly
        / 100
        * pow((1 + (interest_rate_monthly / 100)), tenure)
        / (pow((1 + (interest_rate_monthly / 100)), tenure) - 1)
    )
    if to_round:
        emi = round_up(to=round_to, val=emi)
    return emi


def get_interest_to_charge(
    principal: Decimal,
    interest_rate_monthly: Decimal,
):
    interest = principal * Decimal(interest_rate_monthly / 100)
    return interest


def get_interest_for_integer_emi(
    principal: Decimal,
    interest_rate_monthly: Decimal,
    instalment: Decimal,
    round_to: Optional[str] = "one",
):
    """
    Returns the interest amount to make the emi a whole number with added rounding difference.
    """
    interest_on_principal = get_interest_to_charge(principal, interest_rate_monthly)
    rounding_difference = round_up(to=round_to, val=instalment) - instalment
    interest = interest_on_principal + rounding_difference
    return interest
