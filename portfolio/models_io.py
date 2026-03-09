from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class BasePosition(BaseModel):
    trade_date: datetime
    account: str = Field(validation_alias='account_id')
    ticker: str
    quantity: float = Field(gt=0, validation_alias='shares')
    market_value: float = Field(gt=0)
    custodian: str | None = Field(validation_alias='custodian_ref')


class BaseTradeCptyA(BaseModel):
    trade_date: datetime = Field(validation_alias='TradeDate')
    account: str = Field(validation_alias='AccountID')
    ticker: str = Field(validation_alias='Ticker')
    quantity: float = Field(gt=0, validation_alias='Quantity')
    price: float | None = Field(gt=0, validation_alias='Price')
    market_value: float | None = Field(default=None)
    trade_type: str = Field(validation_alias='TradeType')
    settle_date: datetime = Field(validation_alias='SettlementDate')


class BaseTradeCptyB(BaseModel):
    trade_date: datetime = Field(validation_alias='REPORT_DATE')
    account: str = Field(validation_alias='ACCOUNT_ID')
    ticker: str = Field(validation_alias='SECURITY_TICKER')
    quantity: float = Field(validation_alias='SHARES')
    price: float | None = Field(default=None)
    market_value: float = Field(validation_alias='MARKET_VALUE')
    trade_type: str | None = Field(default=None)
    custodian: str = Field(validation_alias='SOURCE_SYSTEM')


class InputCheck(BaseModel):
    status: str
    count: Optional[int] = None
    errors: Optional[list[str]] = []
