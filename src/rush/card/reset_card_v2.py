from rush.card import ResetCard
from rush.models import LedgerTriggerEvent


class ResetCardV2(ResetCard):

    __mapper_args__ = {"polymorphic_identity": "term_loan_reset_v2"}

    def disburse(self, **kwargs):
        event = LedgerTriggerEvent(
            performed_by=kwargs["user_id"],
            name="disbursal",
            loan_id=kwargs["loan_id"],
            post_date=kwargs["product_order_date"],
            amount=kwargs["amount"],
        )

        self.session.add(event)
        self.session.flush()

        self.loan_disbursement_event(
            event=event,
            bill_id=kwargs["loan_data"].id,
            downpayment_amount=kwargs.get("actual_downpayment_amount", None),
        )

        from rush.ledger_events import disburse_money_to_card

        disburse_money_to_card(session=self.session, user_loan=self, event=event)

        return event
