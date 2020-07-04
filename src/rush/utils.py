from decimal import (
    ROUND_UP,
    Decimal,
)

import pendulum
from pendulum import DateTime


def get_current_ist_time() -> DateTime:
    return pendulum.now("Asia/Kolkata").replace(tzinfo=None)


def mul(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x * y).quantize(fp)


def div(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x / y).quantize(fp)


def round_up_decimal(val: Decimal) -> Decimal:
    rounded_up = val.quantize(Decimal("1."), rounding=ROUND_UP)
    return rounded_up
