from dataclasses import dataclass
from typing import Optional
from pendulum import DateTime
from pydantic import EmailStr
from pydantic.dataclasses import dataclass as py_dataclass
from sqlalchemy import (
    JSON,
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    DECIMAL,
    TIMESTAMP
)
from decimal import Decimal as DecimalType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, relationship, sessionmaker
import pendulum

Base = declarative_base()

def get_current_ist_time():
    return pendulum.now('Asia/Kolkata').replace(tzinfo=None)

class AuditMixin(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    performed_by = Column(Integer, nullable=True)
    created_at = Column(TIMESTAMP,default=get_current_ist_time(), nullable=False)
    updated_at = Column(TIMESTAMP,default=get_current_ist_time(), nullable=False)
    performed_by = Column(Integer, nullable=True)


class User(AuditMixin):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(100))
    fullname = Column(String(50))
    nickname = Column(String(12))


@py_dataclass
class AuditMixinPy:
    id: int
    performed_by: int


@py_dataclass
class UserPy(AuditMixinPy):
    user_id: str
    name: str
    email: str
    fullname: str
    nickname: str


class LedgerTriggerEvent(AuditMixin):

    __tablename__ = "ledger_trigger_event"
    name = Column(String(50))
    extra_details = Column(JSON)


@py_dataclass
class LedgerTriggerEventPy(AuditMixinPy):
    name: str
    extra_details: dict


class BookAccount(AuditMixin):
    __tablename__ = 'book_account'
    identifier = Column(String(50))
    book_type = Column(String(50))
    account_type = Column(String(50))

@py_dataclass
class BookAccountPy(AuditMixinPy):
    identifier: str
    book_type: str
    account_type:str

class LedgerEntry(AuditMixin):
    __tablename__ = 'ledger_entry'
    event_id = Column(Integer, ForeignKey(LedgerTriggerEvent.id))
    from_book_account = Column(Integer, ForeignKey(BookAccount.id))
    to_book_account = Column(Integer, ForeignKey(BookAccount.id))
    amount = Column(DECIMAL)
    business_date = Column(TIMESTAMP, nullable=False)

@py_dataclass
class LedgerEntryPy(AuditMixinPy):
    event_id: int
    from_book_account: int
    to_book_account: int
    amount: DecimalType
    business_date: DateTime
