from sqlalchemy.orm import Session, query
from sqlalchemy.sql import func
from sqlalchemy.sql.elements import Case, Null
from sqlalchemy.sql.functions import coalesce, concat, user
from sqlalchemy import and_, any_

from rush.models import (
    EventDpd,
    Fee,
    JournalEntry,
    LedgerTriggerEvent,
    Lenders,
    LoanData,
    Loan,
    LoanMoratorium,
    LoanSchedule,
    MoratoriumInterest,
    PaymentMapping,
    PaymentRequestsData,
    PaymentSplit,
    Product,
    User,
)


def opportunity_data(session: Session, loan_id: Loan.id):
    loan = session.query(Loan).filter(Loan.id == loan_id)
    opportunities = []
    application_id = loan_id
    loan_agreement_date = loan.amortization_date
    disbursal_amount = (
        session.query(sum(LoanData.gross_principal)).filter(LoanData.loan_id == loan.id).scalar()
    )
    if loan.loan_status == "Cancelled":
        disbursal_amount = 0
    max_emi_date = session.query(max(LoanSchedule.due_date)).filter(LoanSchedule.loan_id == loan.id)
    sanction_amount = (
        session.query(sum(LoanData.gross_principal)).filter(LoanData.loan_id == loan.id).scalar()
    )
    tenure_in_months = loan.tenure_in_months
    if tenure_in_months == 1:
        tenure_in_months = 2
    loan_rate = loan.rc_rate_of_interest_monthly or 24
    closedate = loan_agreement_date
    if loan.sub_product_type == "tenure_loan":
        oppty_type = "TL"
    elif loan.sub_product_type == "card":
        oppty_type = "Card Facility"

    # pd = session.query(
    #     min(
    #         coalesce(
    #             PaymentRequestsData.payment_received_in_bank_date,
    #             PaymentRequestsData.intermediary_payment_date.label("min_payment_date"),
    #         )
    #     ),
    #     sum(PaymentRequestsData.payment_request_amount).label("fees_paid"),
    # ).filter(
    #     and_(
    #         PaymentRequestsData.collection_request_id == None,
    #         PaymentRequestsData.row_status == "active",
    #         PaymentRequestsData.payment_request_status == "Paid",
    #         PaymentRequestsData.payment_request_amount > 0,
    #     ).group_by(PaymentRequestsData.user_id)
    # )
    # query_one = (
    #     session.query(
    #         LoanData.loan_id.label("application_id__c"),
    #         coalesce(Loan.rc_rate_of_interest_monthly, 24).label("loan_rate__c"),
    #         (LoanData.principal * Loan.downpayment_percent).label("down_payment__c"),
    #         LoanData.created_at.label("loan_agreement_date__c"),
    #         Loan.tenure_in_months.label("loan_tenor_in_month__c"),
    #     ).filter(Loan.user_id == user_id)
    # )
    # query_two =

    # query_three =

    # data = query_one.union(query_two).union(query_three)
