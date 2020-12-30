from decimal import Decimal
from typing import (
    Any,
    Dict,
)

from pendulum import now as current_time
from sqlalchemy.orm import Session

from rush.models import PaymentRequestsData


def payment_request_data(
    session: Session,
    type: str,
    payment_request_amount: Decimal,
    user_id: int,
    payment_request_id: str,
    **kwargs: str,
) -> PaymentRequestsData:
    """
    populate v3_payment_requests_data table
    """
    data = PaymentRequestsData.new(
        session=session,
        type=type,
        payment_request_amount=payment_request_amount,
        payment_request_status="UNPAID",
        source_account_id=0,
        destination_account_id=0,
        user_id=user_id,
        payment_request_id=payment_request_id,
        row_status="active",
        created_at=kwargs.get("created_at"),
        updated_at=kwargs.get("updated_at"),
        payment_reference_id=kwargs.get("payment_reference_id"),
        intermediary_payment_date=kwargs.get("intermediary_payment_date"),
        payment_received_in_bank_date=kwargs.get("payment_received_in_bank_date"),
        payment_request_mode=kwargs.get("payment_request_mode"),
        payment_execution_charges=kwargs.get("payment_execution_charges"),
        payment_gateway_id=kwargs.get("payment_gateway_id"),
        gateway_response=kwargs.get("gateway_response", {}),
        collection_by=kwargs.get("collection_by"),
        collection_request_id=kwargs.get("collection_request_id"),
        payment_request_comments=kwargs.get("payment_request_comments"),
        prepayment_amount=kwargs.get("prepayment_amount"),
        net_payment_amount=kwargs.get("net_payment_amount"),
        fee_amount=kwargs.get("fee_amount"),
        expire_date=kwargs.get("expire_date"),
        coupon_data=kwargs.get("coupon_data", {}),
        gross_request_amount=kwargs.get("gross_request_amount"),
        coupon_code=kwargs.get("coupon_code"),
        extra_details=kwargs.get("extra_details", {}),
    )

    return data


def pay_payment_request(session: Session, amount: Decimal, payment_request_id: str) -> Dict[str, Any]:
    gateway_charges = 0.5
    payment_gateway_id = 23

    payment_data = (
        session.query(PaymentRequestsData)
        .filter(PaymentRequestsData.payment_request_id == payment_request_id)
        .first()
    )
    payment_data.payment_execution_charges = gateway_charges
    payment_data.payment_gateway_id = payment_gateway_id
    payment_data.payment_request_status = "PAID"
    payment_data.payment_received_in_bank_date = current_time().replace(tzinfo=None)
    payment_data.extra_details.update(
        {
            "pay_id": "sdfsadf",
            "gateway_charges": gateway_charges,
            "amount": amount,
            "payment_gateway_id": payment_gateway_id,
            "payment_request_id": payment_request_id,
        }
    )

    return {
        "status": "success",
        "id": "sdfsadf",
        "gateway_charges": gateway_charges,
        "amount": amount,
        "payment_gateway_id": payment_gateway_id,
        "payment_request_id": payment_request_id,
    }
