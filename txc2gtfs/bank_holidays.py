import json
from dataclasses import dataclass
from datetime import datetime
from functools import total_ordering
from typing import cast

import pandas as pd

from .util.network import download_cached

_BANK_HOLIDAYS_JSON_URL = "https://www.gov.uk/bank-holidays.json"

type Event = dict[str, str | bool]


@total_ordering
@dataclass(slots=True, frozen=True)
class BankHoliday:
    title: str
    date: datetime
    notes: str
    bunting: bool

    def __hash__(self) -> int:
        return int(self.date.timestamp())

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, BankHoliday)
        return self.date == other.date

    def __gt__(self, other: object) -> bool:
        assert isinstance(other, BankHoliday)
        return self.date > other.date

    @staticmethod
    def from_event(event: Event) -> "BankHoliday":
        return BankHoliday(
            title=cast(str, event["title"]),
            date=datetime.strptime(cast(str, event["date"]), "%Y-%m-%d"),
            notes=cast(str, event["notes"]),
            bunting=cast(bool, event["bunting"]),
        )


def get_bank_holidays() -> set[BankHoliday]:
    bank_holidays_path = download_cached(_BANK_HOLIDAYS_JSON_URL)

    with bank_holidays_path.open("r", encoding="utf-8") as fp:
        bank_holidays: dict[str, dict[str, str | list[Event]]] = json.load(fp)

        return {
            BankHoliday.from_event(event)
            for division in bank_holidays.values()
            for event in cast(list[Event], division["events"])
        }


def get_bank_holiday_dates(gtfs_info: pd.DataFrame) -> list[str]:
    """
    Retrieve information about UK bank holidays that are during the feed operative
    period.
    """
    bank_holidays = sorted(get_bank_holidays())

    # Get start and end date of the GTFS feed
    start_date_min = datetime.strptime(
        cast(str, gtfs_info["start_date"].min()), "%Y%m%d"
    )
    end_date = cast(str | float, gtfs_info["end_date"].max(skipna=True))
    end_date_max = (
        datetime.strptime(end_date, "%Y%m%d") if isinstance(end_date, str) else None
    )

    # Select bank holidays that fit the time range
    return [
        bh.date.strftime("%Y%m%d")
        for bh in bank_holidays
        if start_date_min <= bh.date
        and (end_date_max is None or end_date_max >= bh.date)
    ]
