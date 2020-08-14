from decimal import Decimal
from typing import List

import pandas as pd
import psycopg2 as pg
from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import (
    Session,
    session,
)
from sqlalchemy.sql.operators import desc_op
from sqlalchemy.sql.sqltypes import Integer

from rush.accrue_financial_charges import (
    accrue_interest_on_all_bills,
    accrue_late_charges,
)
from rush.card import (
    create_user_card,
    get_user_card,
)
from rush.create_bill import (
    bill_generate,
    extend_tenure,
)
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import (
    get_account_balance_from_str,
    is_bill_closed,
)
from rush.lender_funds import (
    lender_disbursal,
    lender_interest_incur,
    m2p_transfer,
)
from rush.models import (
    CardEmis,
    CardKitNumbers,
    CardNames,
    CardTransaction,
    EmiPaymentMapping,
    LedgerTriggerEvent,
    LenderPy,
    Lenders,
    LoanData,
    LoanMoratorium,
    User,
    UserCard,
    UserPy,
)
from rush.payments import (
    payment_received,
    refund_payment,
)

v3_conn = "postgresql://nishant:SAkrlF5WeR@localhost:5013/production"

open_loans = [
    1079788,
    1131509,
    1176634,
    1168937,
    1074775,
    1124534,
    1134863,
    1157934,
    1075150,
    1071848,
    1140884,
    1070254,
    1203125,
    1168166,
    1175884,
    1092283,
    1076255,
    1095962,
    1156303,
    1192374,
    1118930,
    1126360,
    1162620,
    1096339,
    1136317,
    1144622,
    1075375,
    1117426,
    1114879,
    1079743,
    1126842,
    1175099,
    1202033,
    1177548,
    1093260,
    1078915,
    1102395,
    1090669,
    1071392,
    1168221,
    1168887,
    1190251,
    1190855,
    1114736,
    1070444,
    1125401,
    1168580,
    1105685,
    1107458,
    1157616,
]

closed_loans = [
    1132128,
    1074083,
    1134433,
    1080808,
    1149544,
    1127087,
    1072207,
    1082092,
    1201024,
    1072317,
    1175453,
    1092904,
    1115199,
    1205549,
    1080908,
    1072233,
    1075869,
    1076760,
    1081022,
    1119345,
    1167865,
    1149344,
    1107604,
    1193703,
    1102480,
    1134570,
    1077054,
    1138607,
    1078849,
    1191355,
    1131663,
    1079259,
    1143187,
    1193957,
    1169463,
    1092974,
    1143002,
    1075021,
    1194299,
    1146534,
    1072945,
    1082811,
    1193183,
    1190739,
    1095176,
    1091878,
    1092596,
    1142354,
    1092351,
    1191901,
]

open_loans = [1157616]


def create_all_users(session: Session, loan_ids: List[Integer]) -> pd.DataFrame:

    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()

    l1 = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(l1)
    session.flush()

    user_ids = pd.read_sql_query(
        """
        select distinct user_id, loan_id from v3_loan_data
        where row_status = 'active'
        and loan_id in %(loan_ids)s
    """,
        con=v3_conn,
        params={"loan_ids": tuple(loan_ids)},
    )

    for user_id in user_ids["user_id"]:
        u = User(id=user_id, performed_by=123,)
        session.add(u)

        kitno = 11111 + user_id
        ckn = CardKitNumbers(
            kit_number=str(kitno), card_name_id=cn.id, last_5_digits="0000", status="active"
        )
        session.add(ckn)
        session.flush()

        create_user_card(
            session=session,
            user_id=user_id,
            card_activation_date=parse_date("2019-01-01").date(),
            card_type="ruby",
            kit_number=kitno,
            lender_id=62311,
        )

    return user_ids


def get_all_txns(session: Session, user_id: Decimal, loan_id: Decimal) -> pd.DataFrame:

    txns = pd.read_sql_query(
        """
        with bills as (
            select loan_id from v3_loan_data
            where row_status = 'active'
            and parent_id = %(loan_id)s
            and product_status = 'Confirmed'
        )
        select product_price, agreement_date, product_name from v3_loan_data
        where row_status = 'active'
        and parent_id in (select loan_id from bills)
        order by agreement_date
    """,
        con=v3_conn,
        params={"loan_id": loan_id},
    )

    return txns


def get_all_payments(session: Session, loan_id: Decimal) -> pd.DataFrame:

    payments = pd.read_sql_query(
        """
        select com.batch_id as loan_id, prd.payment_request_id, payment_request_amount, intermediary_payment_date
        from v3_payment_requests_data prd 
        join v3_collection_order_mapping com
        on com.collection_request_id = prd.collection_request_id 
        and com.row_status = 'active'
        where prd.row_status = 'active' 
        and payment_request_status = 'Paid' 
        and prd.collection_request_id is not null 
        and collection_by != 'rc_lender_payment'
        and com.batch_id = %(loan_id)s
        order by intermediary_payment_date
        
    """,
        con=v3_conn,
        params={"loan_id": loan_id},
    )

    return payments


def get_all_late_fee(session: Session, loan_id: Decimal) -> pd.DataFrame:

    fees = pd.read_sql_query(
        """
        select led.gross_due_late_payment_fees amount, led.due_date fee_date
        from v3_loan_emis_data led
        where led.row_status = 'active' 
        and led.loan_id = %(loan_id)s
        and led.gross_due_late_payment_fees > 0
        order by led.due_date
    """,
        con=v3_conn,
        params={"loan_id": loan_id},
    )

    return fees


def test_drawdown_open(session: Session) -> None:

    # create all users
    users = create_all_users(session, open_loans)

    for index, row in users.iterrows():
        user_id = Decimal(row["user_id"].item())
        loan_id = Decimal(row["loan_id"].item())
        user_card = get_user_card(session, user_id)
        all_events = []

        # add all txns
        txns = get_all_txns(session, user_id, loan_id)
        for index, row in txns.iterrows():
            all_events.append({"type": "txn", "data": row, "date": row["agreement_date"]})

        # add create bill and interest accrue for each month to events
        for x in range(20):  # 20 is no of months from 2019-01-10 to 2020-08-01
            date = parse_date("2019-02-01").date() + relativedelta(months=x)
            all_events.append({"type": "create_bill", "data": {}, "date": date})
            date = parse_date("2019-02-20").date() + relativedelta(months=x)
            all_events.append({"type": "interest", "data": {}, "date": date})

        # fetch all payments and add to events
        payments = get_all_payments(session, loan_id)
        for index, row in payments.iterrows():
            all_events.append({"type": "payment", "data": row, "date": row["intermediary_payment_date"]})

        # fetch all late fee
        late_fee = get_all_late_fee(session, loan_id)
        for index, row in late_fee.iterrows():
            all_events.append({"type": "late_fee", "data": row, "date": row["fee_date"]})

        # sort events on post date
        all_events = sorted(all_events, key=lambda i: i["date"])

        for event in all_events:
            if event["type"] == "txn":
                create_card_swipe(
                    session=session,
                    user_card=user_card,
                    txn_time=event["data"]["agreement_date"],
                    amount=Decimal(event["data"]["product_price"]),
                    description=event["data"]["product_name"],
                )
            elif event["type"] == "create_bill":
                latest_bill = user_card.get_latest_generated_bill()
                if latest_bill and latest_bill.table.bill_start_date >= event["date"]:
                    continue
                bill_generate(user_card, event["date"])
            elif event["type"] == "interest":
                accrue_interest_on_all_bills(session, event["date"], user_card)
            elif event["type"] == "late_fee":
                accrue_late_charges(session, user_card, event["date"], Decimal(event["data"]["amount"]))
            elif event["type"] == "payment":
                continue
                payment_received(
                    session=session,
                    user_card=user_card,
                    payment_amount=Decimal(event["data"]["payment_request_amount"]),
                    payment_date=event["data"]["intermediary_payment_date"],
                    payment_request_id=event["data"]["payment_request_id"],
                )

    # conn = pg.connect(v3_conn)
    # cursor = conn.cursor()
    # cursor.execute("select * from alembic_version")
    # r = cursor.fetchone()
    # print(r)

    session.commit()
