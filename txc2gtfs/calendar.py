import pandas as pd

from txc2gtfs.util.xml import NS, XMLElement

_DAYS_OF_THE_WEEK = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


def get_weekday_info(data: XMLElement) -> str | None:
    """
    Get operating profile information from Services.Service.

    This is used if VehicleJourney does not contain the information.
    """

    weekdays = data.findall(
        "./txc:OperatingProfile/txc:RegularDayType/txc:DaysOfWeek/*", NS
    )
    if not weekdays:
        return None

    return "|".join(weekday.tag.rsplit("}", maxsplit=1)[1] for weekday in weekdays)


def parse_day_range(row: pd.Series) -> pd.Series:
    """Parse day range from TransXChange DayOfWeek element"""

    dayinfo = row["weekdays"].lower()
    row = pd.concat([row, pd.Series({day: 0 for day in _DAYS_OF_THE_WEEK})])

    # Check if dayinfo is specified as day-range
    if "to" in dayinfo:
        start, end = dayinfo.split("to")
        row.loc[start:end] = 1
        return row

    if dayinfo == "weekend":
        row[["saturday", "sunday"]] = 1
    else:
        row[dayinfo.split("|")] = 1
    return row.drop("weekdays")


def get_calendar(gtfs_info: pd.DataFrame):
    """Parse calendar attributes from GTFS info DataFrame"""
    # Parse calendar
    calendar = (
        gtfs_info[["service_id", "weekdays", "start_date", "end_date"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .apply(parse_day_range, axis=1)
    )

    # Fix column order
    return calendar[
        [
            "service_id",
            *_DAYS_OF_THE_WEEK,
            "start_date",
            "end_date",
        ]
    ]
