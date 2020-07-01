from decimal import Decimal, ROUND_UP

import pendulum
from pendulum import DateTime


def get_current_ist_time() -> DateTime:
    return pendulum.now("Asia/Kolkata").replace(tzinfo=None)


def mul(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x * y).quantize(fp)


def div(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x / y).quantize(fp)


def get_updated_fee_diff_amount_from_principal(principal: Decimal, fee: Decimal) -> Decimal:
    # Adjust for rounding because total due amount has to be rounded
    divided_principal = div(principal, 12)
    rounded_total_due = (divided_principal + fee).quantize(Decimal("1."), rounding=ROUND_UP)
    diff = rounded_total_due - (divided_principal + fee)
    return diff
