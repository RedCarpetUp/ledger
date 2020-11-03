import math
from decimal import (
    ROUND_UP,
    Decimal,
)
from typing import (
    Any,
    Dict,
    Union,
)

import pendulum
from pendulum import DateTime


def get_current_ist_time() -> DateTime:
    return pendulum.now("Asia/Kolkata").replace(tzinfo=None)


def mul(x: Decimal, y: Union[Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x * y).quantize(fp)


def div(x: Decimal, y: Union[Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x / y).quantize(fp)


def round_up(to: str, val: Decimal) -> Decimal:
    if to == "one":
        return round_up_to_one(val)
    elif to == "ten":
        return round_up_to_ten(val)


def round_up_to_one(val: Decimal) -> Decimal:
    rounded_up = val.quantize(Decimal("1."), rounding=ROUND_UP)
    return rounded_up


def round_up_decimal_to_nearest(val: Decimal, to_nearest: Decimal = Decimal("10")) -> Decimal:
    rounded_val = round_up_to_one(val)
    if to_nearest == Decimal("1"):
        return rounded_val

    remainder = rounded_val % to_nearest
    if remainder:
        rounded_val = rounded_val - remainder + to_nearest

    return rounded_val


def round_up_to_ten(val: Decimal) -> Decimal:
    return Decimal(math.ceil(val / 10) * 10)


def get_gst_split_from_amount(
    amount: Decimal, sgst_rate: Decimal, cgst_rate: Decimal, igst_rate: Decimal
) -> Dict[str, Any]:
    sgst_multiplier = sgst_rate / 100
    cgst_multiplier = cgst_rate / 100
    igst_multiplier = igst_rate / 100

    net_amount = amount / (sgst_multiplier + cgst_multiplier + igst_multiplier + Decimal(1))
    return add_gst_split_to_amount(net_amount, sgst_rate, cgst_rate, igst_rate)


def add_gst_split_to_amount(
    net_amount: Decimal, sgst_rate: Decimal, cgst_rate: Decimal, igst_rate: Decimal
) -> Dict[str, Any]:
    sgst_multiplier = sgst_rate / 100
    cgst_multiplier = cgst_rate / 100
    igst_multiplier = igst_rate / 100

    sgst = mul(net_amount, sgst_multiplier)
    cgst = mul(net_amount, cgst_multiplier)
    igst = mul(net_amount, igst_multiplier)
    d = {"net_amount": net_amount.quantize(Decimal(".01")), "sgst": sgst, "cgst": cgst, "igst": igst}
    d["gross_amount"] = d["net_amount"] + d["sgst"] + d["cgst"] + d["igst"]
    return d
