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


EMI_FORMULA_DICT = {
    "card_id": None,
    "due_date": None,
    "due_amount": Decimal(0),
    "total_due_amount": Decimal(0),
    "interest_current_month": Decimal(0),
    "interest_next_month": Decimal(0),
    "interest": Decimal(0),
    "emi_number": Decimal(0),
    "late_fee": Decimal(0),
    "row_status": Decimal(0),
    "dpd": Decimal(0),
    "last_payment_date": Decimal(0),
    "total_closing_balance": Decimal(0),
    "total_closing_balance_post_due_date": Decimal(0),
    "late_fee_received": Decimal(0),
    "interest_received": Decimal(0),
    "payment_received": Decimal(0),
    "payment_status": "Paid",
    "extra_details": {},
}
