from decimal import (
    ROUND_UP,
    Decimal,
)
from typing import (
    Any,
    Dict,
)

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


EMI_FORMULA_DICT = {
    "loan_id": None,
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
