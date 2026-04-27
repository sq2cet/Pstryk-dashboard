from datetime import UTC, date, datetime

from sqlmodel import Field, SQLModel


def utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str
    is_secret: bool = False
    updated_at: datetime = Field(default_factory=utcnow_naive)


class PstrykPrice(SQLModel, table=True):
    ts_utc: datetime = Field(primary_key=True)
    price_pln_per_kwh: float
    kind: str  # 'historical' | 'forecast'
    raw_json: str | None = None
    fetched_at: datetime = Field(default_factory=utcnow_naive)


class MeterReading(SQLModel, table=True):
    ts_utc: datetime = Field(primary_key=True)
    active_power_w: float
    energy_kwh_total: float | None = None
    raw_json: str | None = None


class DailyAggregate(SQLModel, table=True):
    day: date = Field(primary_key=True)
    kwh: float
    avg_price_pln_per_kwh: float
    cost_pln: float
    computed_at: datetime = Field(default_factory=utcnow_naive)
