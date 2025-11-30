import pandas as pd

from txc2gtfs.util.xml import NS, XMLElement


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


def parse_day_range(dayinfo: str) -> pd.DataFrame:
    """Parse day range from TransXChange DayOfWeek element"""
    # Converters
    weekday_to_num = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    num_to_weekday = {
        0: "monday",
        1: "tuesday",
        2: "wednesday",
        3: "thursday",
        4: "friday",
        5: "saturday",
        6: "sunday",
    }

    # Check if dayinfo is specified as day-range
    if "To" in dayinfo:
        day_range = dayinfo.split("To")
        start_i = weekday_to_num[day_range[0].lower()]
        end_i = weekday_to_num[day_range[1].lower()]

        # Get days when the service is active
        active_days = list(range(start_i, end_i + 1))
    else:
        dayinfo = dayinfo.lower()
        if dayinfo == "weekend":
            active_days = [5, 6]
        else:
            active_days = [weekday_to_num[day] for day in dayinfo.split("|")]

    # Generate DF
    return pd.DataFrame(
        {num_to_weekday[n]: [int(n in active_days)] for n in range(0, 7)}
    )


def get_calendar(gtfs_info: pd.DataFrame):
    """Parse calendar attributes from GTFS info DataFrame"""
    # Parse calendar
    calendar = (
        gtfs_info[["service_id", "weekdays", "start_date", "end_date"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Container for final results
    gtfs_calendar = pd.DataFrame()

    # Parse weekday columns
    for idx, row in calendar.iterrows():
        # Get dayinfo
        dayinfo = row["weekdays"]

        # Parse day information
        dayrow = parse_day_range(dayinfo)

        # Add service and operation range info
        dayrow["service_id"] = row["service_id"]
        dayrow["start_date"] = row["start_date"]
        dayrow["end_date"] = row["end_date"]

        # Add to container
        gtfs_calendar = pd.concat(
            [gtfs_calendar, dayrow], ignore_index=True, sort=False
        )

    # Fix column order
    col_order = [
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ]
    gtfs_calendar = gtfs_calendar[col_order]

    # Ensure correct datatypes
    int_types = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    for col in int_types:
        gtfs_calendar[col] = gtfs_calendar[col].astype(int)

    return gtfs_calendar
