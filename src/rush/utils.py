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


def get_gst_split_from_amount(amount: Decimal, total_gst_rate: Decimal) -> Dict[str, Any]:
    gst_multiplier = total_gst_rate / 100

    total_gst = amount * gst_multiplier / (gst_multiplier + Decimal(1))
    cgst = round(total_gst / 2, 2)
    sgst = cgst

    net_amount = amount - cgst - sgst

    gst_split_data = {
        "gross_amount": amount,
        "net_amount": net_amount,
        "sgst": sgst,
        "cgst": cgst,
        "igst": 0,
    }
    return gst_split_data


def add_gst_split_to_amount(net_amount: Decimal, total_gst_rate: Decimal) -> Dict[str, Any]:

    gst_multiplier = total_gst_rate / 100

    total_gst = mul(net_amount, gst_multiplier)
    split_gst = div(total_gst, 2)

    d = {"net_amount": round(net_amount, 2), "sgst": split_gst, "cgst": split_gst, "igst": 0}
    d["gross_amount"] = d["net_amount"] + d["sgst"] + d["cgst"] + d["igst"]
    return d
