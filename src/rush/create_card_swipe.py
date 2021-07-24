from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)

from pendulum import (
    DateTime,
    datetime,
)
from sqlalchemy.orm.session import Session

from rush.card.base_card import BaseLoan
from rush.card.reset_card_v2 import ResetCardV2
from rush.card.ruby_card import RubyCard
from rush.create_bill import get_or_create_bill_for_card_swipe
from rush.create_emi import (
    update_event_with_dpd,
    update_journal_entry,
)
from rush.ledger_events import (
    card_transaction_event,
    disburse_money_to_card,
)
from rush.ledger_utils import reverse_event
from rush.models import (
    CardTransaction,
    LedgerLoanData,
    LedgerTriggerEvent,
)


def create_card_swipe(
    session: Session,
    user_loan: BaseLoan,
    txn_time: DateTime,
    amount: Decimal,
    description: str,
    trace_no: str,
    txn_ref_no: str,
    source: Optional[str] = "ECOM",
    mcc: Optional[str] = None,
    skip_activation_check: bool = False,
) -> Dict[str, Any]:
    if not hasattr(user_loan, "amortization_date") or not user_loan.amortization_date:
        return {"result": "error", "message": "Card has not been activated"}

    if not skip_activation_check and txn_time.date() < user_loan.amortization_date:
        return {"result": "error", "message": "Transaction cannot happen before activation"}

    if user_loan.loan_status == "CANCELLED":
        return {"result": "error", "message": "Card has been cancelled."}

    card_bill = get_or_create_bill_for_card_swipe(user_loan=user_loan, txn_time=txn_time)
    if card_bill["result"] == "error":
        return card_bill
    card_bill = card_bill["bill"]
    swipe = CardTransaction(  # This can be moved to user card too.
        loan_id=card_bill.id,
        txn_time=txn_time,
        amount=amount,
        description=description,
        source=source,
        mcc=mcc,
        status="CONFIRMED",
        trace_no=trace_no,
        txn_ref_no=txn_ref_no,
    )
    session.add(swipe)
    session.flush()

    lt = LedgerTriggerEvent(
        performed_by=user_loan.user_id,
        name="card_transaction",
        loan_id=user_loan.loan_id,
        post_date=txn_time,
        amount=amount,
        extra_details={"swipe_id": swipe.id},
    )
    session.add(lt)
    session.flush()  # need id. TODO Gotta use table relationships

    if not isinstance(user_loan, ResetCardV2):
        disburse_money_to_card(session=session, user_loan=user_loan, event=lt)

    card_transaction_event(session=session, user_loan=user_loan, event=lt, mcc=mcc)

    # Dpd calculation
    update_event_with_dpd(user_loan=user_loan, event=lt)
    # Update Journal Entry
    update_journal_entry(user_loan=user_loan, event=lt)
    return {"result": "success", "data": swipe}


def reverse_card_swipe(
    session: Session, user_loan: BaseLoan, card_transaction: CardTransaction, post_date: datetime
) -> Dict:

    bill = session.query(LedgerLoanData).filter(LedgerLoanData.id == card_transaction.loan_id).one()
    if bill.is_generated:  # Can't reverse if already generated. # TODO what to do?
        return {"result": "error", "message": "Bill is already generated."}

    original_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.loan_id == user_loan.loan_id,
            LedgerTriggerEvent.extra_details["swipe_id"].astext == str(card_transaction.id),
        )
        .one()
    )

    reversal_event = LedgerTriggerEvent.ledger_new(
        session,
        performed_by=user_loan.user_id,
        name="transaction_reversal",
        loan_id=user_loan.loan_id,
        post_date=post_date,
        amount=card_transaction.amount,
        extra_details={"swipe_id": card_transaction.id},
    )
    reverse_event(session=session, event_to_reverse=original_event, event=reversal_event)
    card_transaction.status = "REVERSED"
    return {"result": "success"}
