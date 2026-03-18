from typing import Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class BasePosition(BaseModel):
    trade_date: datetime
    account: str = Field(validation_alias='account_id')
    ticker: str
    quantity: float = Field(validation_alias='shares')
    market_value: float
    custodian: str | None = Field(validation_alias='custodian_ref', default=None)


class BaseTradeCptyA(BaseModel):
    trade_date: datetime = Field(validation_alias='TradeDate')
    account: str = Field(validation_alias='AccountID')
    ticker: str = Field(validation_alias='Ticker')
    quantity: float = Field(validation_alias='Quantity')
    price: float | None = Field(validation_alias='Price')
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

    @field_validator('trade_date', mode='before')
    @classmethod
    def parse_datetime(cls, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.strptime(value, '%Y%m%d')
            except ValueError:
                raise ValueError('Unexpected date format')
        return value


class InputCheck(BaseModel):
    status: str
    count: Optional[int] = None
    errors: Optional[list[str]] = []


# Reporting models

class ReportPosition(BaseModel):
    trade_date: datetime
    account: str
    ticker: str
    quantity: float
    market_value: float


class ReportConcentrationTicker(BaseModel):
    ticker: str
    market_value: float
    mv_pct: float


class ReportConcentration(BaseModel):
    trade_date: datetime
    account: str
    acct_mv: float
    breach_details: list[ReportConcentrationTicker]


class ReportReconciliationDetail(BaseModel):
    quantity: float | None
    market_value: float | None


class ReportReconciliation(BaseModel):
    trade_date: datetime
    account: str
    ticker: str
    trade: ReportReconciliationDetail
    position: ReportReconciliationDetail
