from decimal import Decimal

from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import (
    and_,
    or_,
)

from rush.accrue_financial_charges import (
    add_early_close_charges,
    get_interest_left_to_accrue,
)
from rush.anomaly_detection import run_anomaly
from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.create_emi import (
    update_event_with_dpd,
    update_journal_entry,
)
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_prepayment,
    adjust_for_revenue,
    get_revenue_book_str_for_fee,
    limit_assignment_event,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    reverse_event,
)
from rush.loan_schedule.loan_schedule import (
    close_loan,
    slide_payment_to_emis,
)
from rush.models import (
    BookAccount,
    CollectionOrders,
    Fee,
    LedgerEntry,
    LedgerTriggerEvent,
    Loan,
    PaymentMapping,
    PaymentRequestsData,
    PaymentSplit,
)
from rush.utils import (
    get_current_ist_time,
    mul,
)
from rush.writeoff_and_recovery import (
    recovery_event,
    write_off_loan,
)


def payment_received(
    session: Session,
    user_loan: BaseLoan,
    payment_request_data: PaymentRequestsData,
    skip_closing: bool = False,
) -> None:
    payment_for_loan = get_payment_for_loan(
        session=session, payment_request_data=payment_request_data, user_loan=user_loan
    )

    if payment_request_data.collection_by == "rc_lender_payment":
        write_off_loan(user_loan=user_loan, payment_request_data=payment_request_data)
        return

    event = LedgerTriggerEvent.new(
        session,
        name="payment_received",
        loan_id=user_loan.loan_id,
        amount=payment_for_loan,
        post_date=payment_request_data.intermediary_payment_date,
        extra_details={
            "payment_request_id": payment_request_data.payment_request_id,
        },
    )
    session.flush()

    remaining_payment_amount = payment_for_loan

    def call_payment_received_event(amount_to_adjust: Decimal) -> Decimal:
        if amount_to_adjust <= 0:
            return amount_to_adjust
        remaining_amount = payment_received_event(
            session=session,
            user_loan=loan,
            amount_to_adjust=amount_to_adjust,
            debit_book_str=f"{loan.lender_id}/lender/pg_account/a",
            event=event,
            skip_closing=skip_closing,
        )
        return remaining_amount

    all_loans = [user_loan] + user_loan.get_child_loans()
    if len(all_loans) > 1:  # if more than 2 loans then pay minimum of all loans first.
        for loan in all_loans:
            min_to_pay = loan.get_remaining_min(include_child_loans=False)
            amount_to_actually_adjust = min(min_to_pay, remaining_payment_amount)
            call_payment_received_event(amount_to_actually_adjust)
            remaining_payment_amount -= amount_to_actually_adjust
    # Settle whatever is remaining after it.
    for loan in all_loans:
        remaining_payment_amount = call_payment_received_event(remaining_payment_amount)
    for loan in all_loans:
        run_anomaly(
            session=session,
            user_loan=loan,
            event_date=event.post_date,
        )
        update_event_with_dpd(user_loan=loan, event=event)
    if remaining_payment_amount > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_loan.loan_id,
            event_id=event.id,
            amount=remaining_payment_amount,
            debit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        )
    create_payment_split(session, event)


def refund_payment(
    session: Session, user_loan: BaseLoan, payment_request_data: PaymentRequestsData
) -> None:
    lt = LedgerTriggerEvent(
        name="transaction_refund",
        loan_id=user_loan.loan_id,
        amount=payment_request_data.payment_request_amount,
        post_date=payment_request_data.intermediary_payment_date,
        extra_details={
            "payment_request_id": payment_request_data.payment_request_id,
        },
    )
    skip_limit_assignment = False
    if payment_request_data.payment_reference_id:
        if payment_request_data.payment_reference_id[:2].lower() == "rc":
            skip_limit_assignment = True

    session.add(lt)
    session.flush()
    # Checking if bill is generated or not. if not then reduce from unbilled else treat as payment.
    transaction_refund_event(
        session=session,
        user_loan=user_loan,
        event=lt,
        skip_limit_assignment=skip_limit_assignment,
    )
    run_anomaly(
        session=session,
        user_loan=user_loan,
        event_date=payment_request_data.intermediary_payment_date,
    )

    # Update dpd
    update_event_with_dpd(user_loan=user_loan, event=lt)
    # Update Journal Entry
    update_journal_entry(user_loan=user_loan, event=lt)


def payment_received_event(
    session: Session,
    user_loan: BaseLoan,
    debit_book_str: str,
    event: LedgerTriggerEvent,
    amount_to_adjust: Decimal,
    skip_closing: bool = False,
) -> Decimal:
    remaining_amount = Decimal(0)

    remaining_amount = adjust_payment(session, user_loan, event, amount_to_adjust, debit_book_str)

    # Sometimes payments come in multiple decimal points.
    # adjust_payment() handles this while sliding, but we do this
    # for pre_payment
    remaining_amount = round(remaining_amount, 2)

    if user_loan.should_reinstate_limit_on_payment:
        user_loan.reinstate_limit_on_payment(event=event, amount=amount_to_adjust)

    is_in_write_off = (
        get_account_balance_from_str(session, f"{user_loan.loan_id}/loan/write_off_expenses/e")[1] > 0
    )
    if is_in_write_off:
        recovery_event(user_loan, event)
        _, amount = get_account_balance_from_str(
            session, f"{user_loan.loan_id}/loan/write_off_expenses/e"
        )
        if amount > 0:
            user_loan.loan_status = "SETTLED"
        else:
            user_loan.loan_status = "RECOVERED"

    return remaining_amount


def get_payment_for_loan(
    session: Session, payment_request_data: PaymentRequestsData, user_loan: BaseLoan
) -> Decimal:
    if not payment_request_data.collection_request_id:
        return round(payment_request_data.payment_request_amount, 2)

    collection_data_amount = (
        session.query(CollectionOrders.amount_paid).filter(
            CollectionOrders.row_status == "active",
            CollectionOrders.batch_id == user_loan.loan_id,
            CollectionOrders.collection_request_id == payment_request_data.collection_request_id,
        )
    ).scalar()
    return round(collection_data_amount, 2)


def find_split_to_slide_in_loan(session: Session, user_loan: BaseLoan, total_amount_to_slide: Decimal):
    unpaid_bills = user_loan.get_unpaid_bills()
    unpaid_bill_ids = [unpaid_bill.table.id for unpaid_bill in unpaid_bills]

    split_info = []

    # This includes bill-level and loan-level fees
    # if reversed not added then it add to prepayment as remaining_amount>0 during writeoff on outstanding amount
    all_fees = (
        session.query(Fee)
        .filter(
            Fee.user_id == user_loan.user_id,
            or_(
                and_(Fee.identifier_id.in_(unpaid_bill_ids), Fee.identifier == "bill"),
                and_(Fee.identifier_id == user_loan.id, Fee.identifier == "loan"),
            ),
            Fee.fee_status == "UNPAID",
        )
        .order_by(Fee.id)
        .all()
    )

    if all_fees:
        # higher priority is first
        fees_priority = [
            # Loan level
            "card_activation_fees",
            "card_reload_fees",
            "reset_joining_fees",
            "card_upgrade_fees",
            # Bill level
            "atm_fee",
            "late_fee",
        ]

        # Group fees by type
        all_fees_by_type = {}
        for fee_type in fees_priority:
            all_fees_by_type.setdefault(fee_type, [])

        for fee in all_fees:
            if fee.name in all_fees_by_type:
                all_fees_by_type[fee.name].append(fee)
            else:
                all_fees_by_type.setdefault(fee.name, []).append(fee)

        # Slide fees type-by-type, following the priority order
        for fee_type, fees in all_fees_by_type.items():
            total_fee_amount = sum(fee.remaining_fee_amount for fee in fees)
            total_amount_to_be_adjusted_in_fee = min(total_fee_amount, total_amount_to_slide)
            fee_amount = 0
            for fee in fees:
                # For non-bill aka loan-level fees
                # Thus, they are not slid into bills and simply get added to the split info
                # to be adjusted into the loan later
                if fee.identifier == "loan":
                    amount_to_adjust = min(total_amount_to_be_adjusted_in_fee, fee.gross_amount)
                    x = {
                        "type": "fee",
                        "fee": fee,
                        "amount_to_adjust": amount_to_adjust,
                    }
                    fee_amount += amount_to_adjust
                    split_info.append(x)
                    continue

                # Bill-level fees are slid here and added to split info
                bill = next(bill for bill in unpaid_bills if bill.table.id == fee.identifier_id)
                amount_to_slide_based_on_ratio = mul(
                    fee.remaining_fee_amount / total_fee_amount,
                    total_amount_to_be_adjusted_in_fee,
                )
                fee_amount += amount_to_slide_based_on_ratio
                x = {
                    "type": "fee",
                    "bill": bill,
                    "fee": fee,
                    "amount_to_adjust": amount_to_slide_based_on_ratio,
                }
                split_info.append(x)
            difference = total_amount_to_slide - fee_amount
            if difference < 0:
                split_info[-1]["amount_to_adjust"] += difference

            total_amount_to_slide -= total_amount_to_be_adjusted_in_fee

    # slide interest.
    total_interest_amount = sum(bill.get_interest_due() for bill in unpaid_bills)
    if total_amount_to_slide > 0 and total_interest_amount > 0:
        total_amount_to_be_adjusted_in_interest = min(total_interest_amount, total_amount_to_slide)
        interest_amount = 0
        for bill in unpaid_bills:
            amount_to_slide_based_on_ratio = mul(
                bill.get_interest_due() / total_interest_amount,
                total_amount_to_be_adjusted_in_interest,
            )
            interest_amount += amount_to_slide_based_on_ratio
            if amount_to_slide_based_on_ratio > 0:  # will be 0 for 0 bill with late fee.
                x = {
                    "type": "interest",
                    "bill": bill,
                    "amount_to_adjust": amount_to_slide_based_on_ratio,
                }
                split_info.append(x)
        difference = total_amount_to_slide - interest_amount
        if difference < 0:
            split_info[-1]["amount_to_adjust"] += difference
        total_amount_to_slide -= total_amount_to_be_adjusted_in_interest

    # slide principal.
    total_principal_amount = sum(bill.get_principal_due() for bill in unpaid_bills)
    if total_amount_to_slide > 0 and total_principal_amount > 0:
        total_amount_to_be_adjusted_in_principal = min(total_principal_amount, total_amount_to_slide)
        principal_amount = 0
        for bill in unpaid_bills:
            amount_to_slide_based_on_ratio = mul(
                bill.get_principal_due() / total_principal_amount,
                total_amount_to_be_adjusted_in_principal,
            )
            principal_amount += amount_to_slide_based_on_ratio
            if amount_to_slide_based_on_ratio > 0:
                x = {
                    "type": "principal",
                    "bill": bill,
                    "amount_to_adjust": amount_to_slide_based_on_ratio,
                }
                split_info.append(x)

        difference = total_amount_to_slide - principal_amount
        if difference < 0:
            split_info[-1]["amount_to_adjust"] += difference
        total_amount_to_slide -= total_amount_to_be_adjusted_in_principal
    return split_info


def transaction_refund_event(
    session: Session,
    user_loan: BaseLoan,
    event: LedgerTriggerEvent,
    skip_limit_assignment: bool,
) -> None:
    m2p_pool_account = f"{user_loan.lender_id}/lender/pool_balance/a"
    refund_amount = adjust_payment(session, user_loan, event, event.amount, m2p_pool_account)
    if not skip_limit_assignment:
        limit_assignment_event(
            session=session, loan_id=user_loan.loan_id, event=event, amount=event.amount
        )
    if refund_amount > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_loan.loan_id,
            event_id=event.id,
            amount=refund_amount,
            debit_book_str=m2p_pool_account,
        )

    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_loan.loan_id}/loan/refund_off_balance/l",  # Couldn't find anything relevant.
        amount=Decimal(event.amount),
    )
    create_payment_split(session, event)
    # slide_payment_to_emis(user_loan, event)


def adjust_payment(
    session: Session,
    user_loan: BaseLoan,
    event: LedgerTriggerEvent,
    amount_to_adjust: Decimal,
    debit_book_str: str,
) -> Decimal:
    # for term loans, if user has paid more than max and interest is left to accrue then convert it into fee.
    if not user_loan.can_close_early and amount_to_adjust > user_loan.get_remaining_max(
        event_id=event.id
    ):
        interest_left_to_accure = get_interest_left_to_accrue(session, user_loan)
        if interest_left_to_accure > 0:
            extra_amount = min(
                interest_left_to_accure,
                amount_to_adjust - user_loan.get_remaining_max(event_id=event.id),
            )
            add_early_close_charges(session, user_loan, event.post_date, extra_amount)

    split_data = find_split_to_slide_in_loan(session, user_loan, amount_to_adjust)

    for data in split_data:
        if "bill" in data:
            adjust_for_min_max_accounts(data["bill"], data["amount_to_adjust"], event.id)

        if data["type"] == "fee":
            adjust_for_revenue(
                session=session,
                event_id=event.id,
                payment_to_adjust_from=data["amount_to_adjust"],
                debit_str=debit_book_str,
                fee=data["fee"],
            )
        if data["type"] in ("interest", "principal"):
            remaining_amount = _adjust_bill(
                session,
                data["bill"],
                data["amount_to_adjust"],
                event.id,
                debit_acc_str=debit_book_str,
            )
            # The amount to adjust is computed for this bill. It should all settle.
            assert remaining_amount == 0
            slide_payment_to_emis(user_loan, event, data["amount_to_adjust"])
        amount_to_adjust -= data["amount_to_adjust"]

    # After doing the sliding we check if the loan can be closed.
    if user_loan.can_close_loan(as_of_event_id=event.id):
        close_loan(user_loan, event.post_date)

    return amount_to_adjust


def settle_payment_in_bank(
    session: Session,
    payment_request_id: str,
    gateway_expenses: Decimal,
    gross_payment_amount: Decimal,
    settlement_date: DateTime,
    user_loan: BaseLoan,
) -> None:
    settled_amount = gross_payment_amount - gateway_expenses
    event = LedgerTriggerEvent(
        name="payment_settled",
        loan_id=user_loan.loan_id,
        amount=settled_amount,
        extra_details={"payment_request_id": payment_request_id},
        post_date=settlement_date,
    )
    session.add(event)
    session.flush()

    payment_settlement_event(session=session, user_loan=user_loan, event=event)

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.loan_id == user_loan.loan_id,
        )
        .one()
    )

    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.lender_id}/lender/gateway_expenses/e",
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        amount=gateway_expenses,
    )

    update_journal_entry(user_loan=user_loan, event=payment_ledger_event)


def payment_settlement_event(session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    _, writeoff_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/writeoff_expenses/e"
    )
    if writeoff_balance > 0:
        amount = min(writeoff_balance, event.amount)
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{user_loan.loan_id}/loan/bad_debt_allowance/ca",
            credit_book_str=f"{user_loan.loan_id}/loan/writeoff_expenses/e",
            amount=amount,
        )

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        amount=event.amount,
    )


def adjust_for_min_max_accounts(bill: BaseBill, payment_to_adjust_from: Decimal, event_id: int):
    min_due = bill.get_remaining_min()
    min_to_adjust_in_this_bill = min(min_due, payment_to_adjust_from)
    if min_to_adjust_in_this_bill != 0:
        # Reduce min amount
        create_ledger_entry_from_str(
            bill.session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=min_to_adjust_in_this_bill,
        )

    max_due = bill.get_remaining_max()
    max_to_adjust_in_this_bill = min(max_due, payment_to_adjust_from)
    if max_to_adjust_in_this_bill != 0:
        # Reduce min amount
        create_ledger_entry_from_str(
            bill.session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/max/l",
            credit_book_str=f"{bill.id}/bill/max/a",
            amount=max_to_adjust_in_this_bill,
        )


def get_payment_split_from_event(session: Session, event: LedgerTriggerEvent):
    split_data = (
        session.query(BookAccount.book_name, func.sum(LedgerEntry.amount))
        .filter(
            LedgerEntry.event_id == event.id,
            LedgerEntry.credit_account == BookAccount.id,
        )
        .group_by(BookAccount.book_name)
        .all()
    )
    not_allowed_accounts = (
        "refund_off_balance",
        "min",
        "max",
        "health_limit",
        "pg_account",
        "available_limit",
        "lender_receivable",
        "write_off_expenses",
    )
    # unbilled and principal belong to same component.
    updated_component_names = {
        "principal_receivable": "principal",
        "downpayment": "principal",
        "interest_receivable": "interest",
        "igst_payable": "igst",
        "cgst_payable": "cgst",
        "sgst_payable": "sgst",
    }
    normalized_split_data = {}
    total_amount = 0
    for book_name, amount in split_data:
        if book_name in not_allowed_accounts or amount == 0:
            continue
        if book_name in updated_component_names:
            book_name = updated_component_names[book_name]
        normalized_split_data[book_name] = normalized_split_data.get(book_name, 0) + amount
        total_amount += amount
    if normalized_split_data:
        assert event.amount == total_amount
    return normalized_split_data


def create_payment_split(session: Session, event: LedgerTriggerEvent):
    """
    Create a payment split at ledger level. Has no emi context.
    Only tells how much principal, interest etc. got settled from x amount of payment.
    """
    split_data = get_payment_split_from_event(session, event)
    new_ps_objects = []
    for component, amount in split_data.items():
        new_ps_objects.append(
            {
                "payment_request_id": event.extra_details["payment_request_id"],
                "component": component,
                "amount_settled": amount,
                "loan_id": event.loan_id,
            }
        )
    session.bulk_insert_mappings(PaymentSplit, new_ps_objects)


def customer_prepayment_refund(
    session: Session,
    user_loan: BaseLoan,
    payment_request_id: str,
    refund_source: str,
):
    payment_request_data = (
        session.query(PaymentRequestsData)
        .filter(PaymentRequestsData.payment_request_id == payment_request_id)
        .one_or_none()
    )

    if payment_request_data is None:
        return {"result": "error", "message": "Payment request not found"}

    refund_amount = payment_request_data.payment_request_amount

    _, prepayment_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )

    if refund_amount > prepayment_balance:
        return {"result": "error", "message": "Refund amount greater than pre-payment"}

    lt = LedgerTriggerEvent(
        name="customer_refund",
        loan_id=user_loan.loan_id,
        amount=refund_amount,
        post_date=get_current_ist_time(),
        extra_details={
            "payment_request_id": payment_request_id,
        },
    )
    session.add(lt)
    session.flush()

    if refund_source == "payment_gateway":
        credit_book_str = f"{user_loan.lender_id}/lender/pg_account/a"
    else:
        credit_book_str = f"12345/redcarpet/rc_cash/a"

    create_ledger_entry_from_str(
        session=session,
        event_id=lt.id,
        debit_book_str=f"{user_loan.loan_id}/loan/pre_payment/l",
        credit_book_str=credit_book_str,
        amount=refund_amount,
    )

    update_journal_entry(user_loan=user_loan, event=lt)

    return {"result": "success", "message": "Prepayment Refund successful"}


def remove_fee(session: Session, user_loan: BaseLoan, fee: Fee):
    fee_removed_event = LedgerTriggerEvent(
        name="fee_removed",
        loan_id=user_loan.loan_id,
        amount=fee.gross_amount,
        post_date=get_current_ist_time(),
        extra_details={
            "fee_id": fee.id,
        },
    )
    session.add(fee_removed_event)
    session.flush()

    # Adjust into bill and pre-payment if customer has paid some amount
    # against the fee
    if fee.gross_amount_paid > 0:
        revenue_book_str = get_revenue_book_str_for_fee(fee)

        accounts_to_adjust = [
            {"account_str": revenue_book_str, "amount": fee.net_amount_paid},
            {"account_str": f"{fee.user_id}/user/cgst_payable/l", "amount": fee.cgst_paid},
            {"account_str": f"{fee.user_id}/user/sgst_payable/l", "amount": fee.sgst_paid},
            {"account_str": f"{fee.user_id}/user/igst_payable/l", "amount": fee.igst_paid},
        ]

        for account in accounts_to_adjust:
            if account["amount"] == 0:
                continue
            remaining_amount = adjust_payment(
                session=session,
                user_loan=user_loan,
                event=fee_removed_event,
                amount_to_adjust=account["amount"],
                debit_book_str=account["account_str"],
            )
            account["amount"] = remaining_amount

        # Check if there's still amount that's left. If yes, then we received extra prepayment.
        is_payment_left = any(account["amount"] > 0 for account in accounts_to_adjust)

        if is_payment_left:
            for account in accounts_to_adjust:
                if account["amount"] == 0:
                    continue

                _adjust_for_prepayment(
                    session=session,
                    loan_id=user_loan.loan_id,
                    event_id=fee_removed_event.id,
                    amount=account["amount"],
                    debit_book_str=account["account_str"],
                )

        fee.net_amount_paid = fee.cgst_paid = fee.sgst_paid = fee.igst_paid = fee.gross_amount_paid = 0

    # For updating the bill's min accounts
    if fee.identifier == "bill":
        fee_event = session.query(LedgerTriggerEvent).filter(LedgerTriggerEvent.id == Fee.event_id).one()

        reverse_event(session=session, event_to_reverse=fee_event, event=fee_removed_event)

    update_journal_entry(session=session, user_loan=user_loan, event=fee_removed_event)
    update_event_with_dpd(user_loan=user_loan, event=fee_removed_event)

    fee.fee_status = "REMOVED"

    if user_loan.loan_status == "FEE PAID":
        user_loan.loan_status = "NOT STARTED"

    return {"result": "success", "message": "Fee removal successful"}


def refund_payment_to_customer(
    session: Session,
    payment_request_id: str,
):
    payment_request_data = (
        session.query(PaymentRequestsData)
        .filter(
            PaymentRequestsData.payment_request_id == payment_request_id,
            PaymentRequestsData.payment_request_status == "Paid",
            PaymentRequestsData.row_status == "active",
        )
        .one_or_none()
    )

    if payment_request_data is None:
        return {"result": "error", "message": "No such paid payment request"}

    # check if already refunded, for idempotency
    payment_refunded_lte = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .one_or_none()
    )

    if payment_refunded_lte:
        return {"result": "error", "message": "Payment already refunded."}

    # If this payment was slid into any EMIs, then they need to be adjusted
    mappings_exist = (
        session.query(PaymentMapping)
        .filter(
            PaymentMapping.payment_request_id == payment_request_id,
            PaymentMapping.row_status == "active",
        )
        .all()
    )

    if mappings_exist:
        return {"result": "error", "message": "Payments made against EMIs cannot be refunded."}

    payment_received_lte = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .one()
    )

    refund_event = LedgerTriggerEvent.new(
        session,
        name="payment_refund",
        loan_id=payment_received_lte.loan_id,
        amount=payment_request_data.payment_request_amount,
        post_date=get_current_ist_time(),
        extra_details={
            "payment_request_id": payment_request_data.payment_request_id,
        },
    )
    session.flush()

    reverse_event(session=session, event_to_reverse=payment_received_lte, event=refund_event)

    # If any fees were settled during this payment, we need to mark those as refunded
    # We can determine this by checking if any cgst was settled during this payment
    # We check this from payment_split
    fees_settled_in_payment_request = (
        session.query(PaymentSplit)
        .filter(PaymentSplit.component == "cgst", PaymentSplit.payment_request_id == payment_request_id)
        .one_or_none()
    )

    if fees_settled_in_payment_request:
        fees = (
            session.query(Fee)
            .join(
                BookAccount,
                and_(
                    BookAccount.book_name == Fee.name,
                    BookAccount.identifier == Fee.identifier_id,
                    BookAccount.identifier_type == Fee.identifier,
                ),
            )
            .join(LedgerEntry, LedgerEntry.credit_account == BookAccount.id)
            .filter(LedgerEntry.event_id == payment_received_lte.id)
            .all()
        )

        for fee in fees:
            fee.fee_status = "REFUNDED"

        user_loan = session.query(Loan).filter(Loan.id == refund_event.loan_id).one()

        if user_loan.loan_status == "FEE PAID":
            user_loan.loan_status = "NOT STARTED"

    session.flush()
    return {"result": "success", "message": "Payment refunded"}
