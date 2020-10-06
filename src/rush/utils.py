from decimal import (
    ROUND_UP,
    Decimal,
)
from typing import Optional

import pendulum
from pendulum import DateTime


def get_current_ist_time() -> DateTime:
    return pendulum.now("Asia/Kolkata").replace(tzinfo=None)


def mul(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x * y).quantize(fp)


def div(x: Decimal, y: [Decimal, int, str], fp: Decimal = Decimal(".01")) -> Decimal:
    return (x / y).quantize(fp)


def round_up_decimal(val: Decimal, fp: Decimal = Decimal("1.")) -> Decimal:
    rounded_up = val.quantize(fp, rounding=ROUND_UP)
    return rounded_up


def round_up_decimal_to_nearest(val: Decimal, to_nearest: Decimal = Decimal("10")) -> Decimal:
    rounded_val = round_up_decimal(val)
    if to_nearest == Decimal("1"):
        return rounded_val

    remainder = rounded_val % to_nearest
    if remainder:
        rounded_val = rounded_val - remainder + to_nearest

    return rounded_val


def get_gst_split_from_amount(
    amount: Decimal, sgst_rate: Decimal, cgst_rate: Decimal, igst_rate: Decimal
) -> dict:
    sgst_multiplier = sgst_rate / 100
    cgst_multiplier = cgst_rate / 100
    igst_multiplier = igst_rate / 100

    net_amount = amount / (sgst_multiplier + cgst_multiplier + igst_multiplier + Decimal(1))
    return add_gst_split_to_amount(net_amount, sgst_rate, cgst_rate, igst_rate)


def add_gst_split_to_amount(
    net_amount: Decimal, sgst_rate: Decimal, cgst_rate: Decimal, igst_rate: Decimal
) -> dict:
    sgst_multiplier = sgst_rate / 100
    cgst_multiplier = cgst_rate / 100
    igst_multiplier = igst_rate / 100

    sgst = mul(net_amount, sgst_multiplier)
    cgst = mul(net_amount, cgst_multiplier)
    igst = mul(net_amount, igst_multiplier)
    d = {"net_amount": net_amount.quantize(Decimal(".01")), "sgst": sgst, "cgst": cgst, "igst": igst}
    d["gross_amount"] = d["net_amount"] + d["sgst"] + d["cgst"] + d["igst"]
    return d


def get_reducing_emi(
    principal: Decimal, interest_rate_monthly: Decimal, tenure: Decimal, to_round: Optional[bool] = False
) -> Decimal:
    emi = (
        principal
        * interest_rate_monthly
        / 100
        * pow((1 + (interest_rate_monthly / 100)), tenure)
        / (pow((1 + (interest_rate_monthly / 100)), tenure) - 1)
    )
    if to_round:
        emi = round(emi, 2)
    return emi
