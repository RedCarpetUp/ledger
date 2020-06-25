from decimal import Decimal

import pendulum
from pendulum import DateTime


def get_current_ist_time() -> DateTime:
    return pendulum.now("Asia/Kolkata").replace(tzinfo=None)


def mul(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x * y).quantize(fp)


def div(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x / y).quantize(fp)
