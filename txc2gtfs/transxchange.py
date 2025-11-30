from __future__ import annotations

from collections.abc import Generator, Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Literal, cast

import pandas as pd

from txc2gtfs.calendar import get_weekday_info
from txc2gtfs.calendar_dates import (
    get_non_operation_days,
)
from txc2gtfs.routes import get_mode
from txc2gtfs.util.xml import NS, XMLElement, XMLTree, get_text


@dataclass
class Line:
    id: str
    name: str


@dataclass
class Service:
    code: str
    journey_patterns: pd.DataFrame
    operation_days: str | None
    non_operation_days: str | None
    lines: dict[str, Line]


def get_last_stop_time_info(
    link: XMLElement,
    hour: int,
    current_date: date,
    current_dt: datetime,
    duration: int,
    stop_num: int,
    boarding_time: int,
) -> pd.DataFrame:
    # Parse stop_id for TO
    stop_id = get_text(link, "./txc:To/txc:StopPointRef")
    # Get arrival time for the last one
    current_dt = current_dt + timedelta(seconds=duration)
    departure_dt = current_dt + timedelta(seconds=boarding_time)
    # Get hour info
    arrival_hour = current_dt.hour
    departure_hour = departure_dt.hour
    # Ensure trips passing midnight are formatted correctly
    arrival_hour, departure_hour = get_midnight_formatted_times(
        arrival_hour, departure_hour, hour, current_date, current_dt, departure_dt
    )

    return pd.DataFrame(
        {
            "stop_id": [stop_id],
            "stop_sequence": [stop_num],
            "arrival_time": [f"{arrival_hour:02}:{current_dt.strftime('%M:%S')}"],
            "departure_time": [f"{departure_hour:02}:{departure_dt.strftime('%M:%S')}"],
        }
    )


def get_midnight_formatted_times(
    arrival_hour: int,
    departure_hour: int,
    hour: int,
    current_date: date,
    current_dt: datetime,
    departure_dt: datetime,
) -> tuple[int, int]:
    # If the arrival / departure hour is smaller than the initialized hour,
    # it means that the trip is extending to the next day. In that case,
    # the hour info should be extending to numbers over 24. E.g. if trip starts
    # at 23:30 and ends at 00:25, the arrival_time should be determined as 24:25
    # to avoid negative time hops.
    if arrival_hour < hour:
        # Calculate time delta (in hours) between the initial trip datetime and the
        # current and add 1 to hop over the midnight to the next day
        last_second_of_day = datetime.combine(current_date, time(23, 59, 59))
        arrival_over_midnight_surplus = (
            int(((current_dt - last_second_of_day) / 60 / 60).seconds) + 1
        )
        departure_over_midnight_surplus = (
            int(((departure_dt - last_second_of_day) / 60 / 60).seconds) + 1
        )

        # Update the hour values with midnight surplus
        arrival_hour = 23 + arrival_over_midnight_surplus
        departure_hour = 23 + departure_over_midnight_surplus

    return arrival_hour, departure_hour


_VEHICLE_JOURNEYS_COLUMNS = [
    "vehicle_journey_id",
    "service_ref",
    "journey_pattern_id",
    "weekdays",
    "non_operative_days",
]


def get_vehicle_journeys(journeys: Iterator[XMLElement]) -> pd.DataFrame:
    """Process vehicle journeys"""

    # Iterate over VehicleJourneys
    def process_vehicle_journey(
        journey: XMLElement,
    ) -> tuple[str, str, str, str | None, str | None]:
        # Get service reference
        service_ref = get_text(journey, "txc:ServiceRef")

        # Journey pattern reference
        journey_pattern_id = get_text(journey, "txc:JourneyPatternRef")

        # Vehicle journey id ==> will be used to generate service_id (identifies
        # operative weekdays)
        vehicle_journey_id = get_text(journey, "txc:VehicleJourneyCode")

        # Parse weekday operation times from VehicleJourney
        weekdays = get_weekday_info(journey)

        # Parse calendar dates (exceptions in operation)
        non_operative_days = get_non_operation_days(journey)

        # Create gtfs_info row
        return (
            vehicle_journey_id,
            service_ref,
            journey_pattern_id,
            weekdays,
            non_operative_days,
        )

    return pd.DataFrame(
        (process_vehicle_journey(journey) for journey in journeys),
        columns=_VEHICLE_JOURNEYS_COLUMNS,
    )


_SECTION_TIMES_COLS = [
    "stop_id",
    "stop_sequence",
    "timepoint",
    "arrival_time",
    "departure_time",
    "route_link_ref",
    "agency_id",
    "trip_id",
    "route_id",
    "vehicle_journey_id",
    "service_ref",
    "direction_id",
    "line_name",
    "travel_mode",
    "trip_headsign",
    "vehicle_type",
    "start_date",
    "end_date",
    "weekdays",
    "non_operative_days",
]


def process_vehicle_journey(
    journey: XMLElement,
    sections: list[XMLElement],
    services: dict[str, Service],
) -> pd.DataFrame | None:
    # Get current date for time reference
    current_date = datetime.now().date()

    # If additional boarding time is needed, specify it here
    # Boarding time in seconds
    boarding_time = 0

    # Get service reference
    service_ref = get_text(journey, "txc:ServiceRef")
    service = services[service_ref]

    # Get line reference
    line_ref = get_text(journey, "txc:LineRef")
    line = service.lines[line_ref]

    # Journey pattern reference
    journey_pattern_id = get_text(journey, "txc:JourneyPatternRef")

    # Vehicle journey id ==> will be used to generate service_id (identifies operative
    # weekdays)
    vehicle_journey_id = get_text(journey, "txc:VehicleJourneyCode")

    # Parse weekday operation times from VehicleJourney
    operation_days = get_weekday_info(journey) or service.operation_days

    # Parse calendar dates (exceptions in operation)
    non_operative_days = get_non_operation_days(journey) or service.non_operation_days

    # Select service journey patterns for given service id
    service_journey_patterns = service.journey_patterns.loc[
        service.journey_patterns["journey_pattern_id"] == journey_pattern_id
    ]

    # Get Journey Pattern Section reference
    jp_section_references = cast(
        list[str], service_journey_patterns["jp_section_reference"].to_list()
    )

    # Parse direction, line_name, travel mode, trip_headsign, vehicle_type, agency_id
    cols = [
        "agency_id",
        "route_id",
        "direction_id",
        "travel_mode",
        "trip_headsign",
        "vehicle_type",
        "start_date",
        "end_date",
    ]
    (
        agency_id,
        route_id,
        direction_id,
        travel_mode,
        trip_headsign,
        vehicle_type,
        start_date,
        end_date,
    ) = service_journey_patterns[cols].values[0]

    # Ensure integer values
    direction_id = int(direction_id)
    travel_mode = int(travel_mode)

    # Get departure time
    departure_time = get_text(journey, "txc:DepartureTime")
    hour, minute, _ = [int(s) for s in departure_time.split(":", maxsplit=2)]

    current_dt: datetime | None = None
    section_times: pd.DataFrame | None = None

    # Iterate over a single departure section
    stop_num = 1
    for section in sections:
        # Section reference
        section_id = section.get("id")
        assert section_id

        # Generate trip_id (same section id might occur with different calendar info,
        # hence attach weekday info as part of trip_id)
        trip_id = f"{section_id}_{operation_days}_{hour:02}{minute:02}"

        # Check that section-ids match
        if section_id not in jp_section_references:
            continue

        links = section.findall("txc:JourneyPatternTimingLink", NS)

        def get_duration(link: XMLElement) -> int:
            # Get leg runtime code
            runtime = get_text(link, "txc:RunTime")

            # Parse duration in seconds
            return int(parse_runtime_duration(runtime))

        def gen_timing_links() -> Generator[tuple[Any, ...], None, None]:
            nonlocal current_dt, stop_num

            # For the given departure section calculate arrival/departure times
            # for all possible trip departure times
            for link in links:
                duration = get_duration(link)

                # Generate datetime for the start time
                if current_dt is None:
                    # On the first stop arrival and departure time should be identical
                    current_dt = datetime.combine(current_date, time(hour, minute))
                    departure_dt = current_dt
                    # Timepoint
                    timepoint = 1

                else:
                    current_dt = current_dt + timedelta(seconds=duration)

                    # Timepoint
                    timepoint = 0

                    departure_dt = current_dt + timedelta(seconds=boarding_time)

                # Get hour info
                arrival_hour = current_dt.hour
                departure_hour = departure_dt.hour

                # Ensure trips passing midnight are formatted correctly
                arrival_hour, departure_hour = get_midnight_formatted_times(
                    arrival_hour,
                    departure_hour,
                    hour,
                    current_date,
                    current_dt,
                    departure_dt,
                )

                # Convert to string
                arrival_t = "{arrival_hour}:{minsecs}".format(
                    arrival_hour=str(arrival_hour).zfill(2),
                    minsecs=current_dt.strftime("%M:%S"),
                )
                departure_t = "{departure_hour}:{minsecs}".format(
                    departure_hour=str(departure_hour).zfill(2),
                    minsecs=departure_dt.strftime("%M:%S"),
                )

                # Parse stop_id for FROM
                stop_id = get_text(link, "./txc:From/txc:StopPointRef")

                # Route link reference
                route_link_ref = get_text(link, "txc:RouteLinkRef")

                # Create gtfs_info row
                yield (
                    stop_id,
                    stop_num,
                    timepoint,
                    arrival_t,
                    departure_t,
                    route_link_ref,
                    agency_id,
                    trip_id,
                    route_id,
                    vehicle_journey_id,
                    service_ref,
                    direction_id,
                    line.name,
                    travel_mode,
                    trip_headsign,
                    vehicle_type,
                    start_date,
                    end_date,
                    operation_days,
                    non_operative_days,
                )

                # Update stop number
                stop_num += 1

        section_times = pd.DataFrame(gen_timing_links(), columns=_SECTION_TIMES_COLS)

        # After timing links have been iterated over,
        # the last stop needs to be added separately
        link = links[-1]
        assert current_dt is not None
        last_stop = get_last_stop_time_info(
            link,
            hour,
            current_date,
            current_dt,
            get_duration(link),
            stop_num,
            boarding_time,
        )
        last_stop["timepoint"] = 0
        last_stop["route_link_ref"] = get_text(link, "txc:RouteLinkRef")
        last_stop["agency_id"] = agency_id
        last_stop["trip_id"] = trip_id
        last_stop["route_id"] = route_id
        last_stop["vehicle_journey_id"] = vehicle_journey_id
        last_stop["service_ref"] = service_ref
        last_stop["direction_id"] = direction_id
        last_stop["line_name"] = line.name
        last_stop["travel_mode"] = travel_mode
        last_stop["trip_headsign"] = trip_headsign
        last_stop["vehicle_type"] = vehicle_type
        last_stop["start_date"] = start_date
        last_stop["end_date"] = end_date
        last_stop["weekdays"] = operation_days
        last_stop["non_operative_days"] = non_operative_days
        section_times = pd.concat([section_times, last_stop], ignore_index=True)

    return section_times


def generate_lines(service: XMLElement) -> Generator[tuple[str, Line], None, None]:
    for line in service.iterfind("./txc:Lines/txc:Line", NS):
        id = line.get("id")
        assert id
        name = get_text(line, "txc:LineName")
        yield (id, Line(id, name))


def generate_service_id(stop_times: pd.DataFrame) -> pd.DataFrame:
    """Generate service_id into stop_times DataFrame"""

    # Create column for service_id
    stop_times["service_id"] = None

    # Parse calendar info
    calendar_info = stop_times.drop_duplicates(subset=["vehicle_journey_id"])

    # Group by weekdays
    calendar_groups = calendar_info.groupby("weekdays")  # type: ignore

    # Iterate over groups and create a service_id
    for _, cgroup in calendar_groups:
        # Parse all vehicle journey ids
        vehicle_journey_ids = cast(list[str], cgroup["vehicle_journey_id"].to_list())

        # Parse other items
        service_ref = cgroup["service_ref"].unique()[0]
        daygroup = cgroup["weekdays"].unique()[0]
        start_d = cgroup["start_date"].unique()[0]
        end_d = cgroup["end_date"].unique()[0]

        # Generate service_id
        service_id = f"{service_ref}_{start_d}_{end_d}_{daygroup}"

        # Update stop_times service_id
        stop_times.loc[
            stop_times["vehicle_journey_id"].isin(vehicle_journey_ids), "service_id"  # type: ignore
        ] = service_id
    return stop_times


def get_gtfs_info(data: XMLTree) -> pd.DataFrame:
    """
    Get GTFS info from TransXChange elements.

    Info:
        - VehicleJourney element includes the departure time information
        - JourneyPatternRef element includes information about the trip_id
        - JourneyPatternSections include the leg duration information
        - ServiceJourneyPatterns include information about which JourneyPatternSections
          belong to a given VehicleJourney.

    GTFS fields - required/optional available from TransXChange - <fieldName> shows
    foreign keys between layers:
        - Stop_times: <trip_id>, arrival_time, departure_time, stop_id, stop_sequence
          (and optional: shape_dist_travelled, timepoint)
        - Trips: <route_id>, service_id, <trip_id>, (+ optional: trip_headsign,
          direction_id, trip_shortname)
        - Routes: <route_id>, agency_id, route_type, route_short_name, route_long_name
    """
    sections = data.findall(
        "./txc:JourneyPatternSections/txc:JourneyPatternSection", NS
    )
    journeys = data.iterfind("./txc:VehicleJourneys/txc:VehicleJourney", NS)

    def generate_services() -> Generator[Service, None, None]:
        for service in data.iterfind("./txc:Services/txc:Service", NS):
            code = get_text(service, "txc:ServiceCode")
            yield Service(
                code=code,
                journey_patterns=get_service_journey_patterns(service),
                operation_days=get_weekday_info(service),
                non_operation_days=get_non_operation_days(service),
                lines=dict(generate_lines(service)),
            )

    # Get all service journey pattern info
    services = {service.code: service for service in generate_services()}

    # Process
    gtfs_info = pd.concat(
        process_vehicle_journey(
            journey,
            sections,
            services,
        )
        for journey in journeys
    )

    # Generate service_id column into the table
    gtfs_info = generate_service_id(gtfs_info)

    return gtfs_info


def parse_runtime_duration(runtime: str) -> int:
    """Parse duration information from TransXChange runtime code"""
    time = 0
    runtime = runtime.split("PT")[1]

    if "H" in runtime:
        split = runtime.split("H")
        time = time + int(split[0]) * 60 * 60
        runtime = split[1]
    if "M" in runtime:
        split = runtime.split("M")
        time = time + int(split[0]) * 60
        runtime = split[1]
    if "S" in runtime:
        split = runtime.split("S")
        time = time + int(split[0]) * 60
    return time


def get_direction(direction_id: str) -> Literal[0] | Literal[1]:
    """Return boolean direction id"""
    if direction_id == "inbound":
        return 0
    elif direction_id == "outbound":
        return 1

    raise ValueError(f"Cannot determine direction from {direction_id}")


_JOURNEY_PATTERN_COLUMNS = [
    "journey_pattern_id",
    "service_code",
    "agency_id",
    "line_name",
    "travel_mode",
    "service_description",
    "trip_headsign",
    # Links to trips
    "jp_section_reference",
    "direction_id",
    # Route_id linking to routes
    "route_id",
    "vehicle_type",
    "vehicle_description",
    "start_date",
    "end_date",
]


def get_service_journey_patterns(service: XMLElement) -> pd.DataFrame:
    """Retrieve a DataFrame of all JourneyPatterns of the service"""

    def process_service(service: XMLElement) -> Generator[tuple[Any, ...], None, None]:
        # Service description
        service_description: str | None = None
        if service_description_el := service.find("txc:Description", NS):
            service_description = service_description_el.text

        # Travel mode
        mode = get_mode(service)

        # Line name
        line_name = get_text(service, "./txc:Lines/txc:Line/txc:LineName")

        # Service code
        service_code = get_text(service, "txc:ServiceCode")

        # Operator reference code
        agency_id = get_text(service, "txc:RegisteredOperatorRef")

        # Start and end date
        start_date = datetime.strftime(
            datetime.strptime(
                get_text(service, "./txc:OperatingPeriod/txc:StartDate"), "%Y-%m-%d"
            ),
            "%Y%m%d",
        )
        end_date = None
        if end_dateget_text := get_text(
            service, "./txc:OperatingPeriod/txc:EndDate", default=None
        ):
            end_date = datetime.strftime(
                datetime.strptime(end_dateget_text, "%Y-%m-%d"),
                "%Y%m%d",
            )

        origin = get_text(service, "./txc:StandardService/txc:Origin")
        destination = get_text(service, "./txc:StandardService/txc:Destination")

        for jp in service.iterfind("./txc:StandardService/txc:JourneyPattern", NS):
            # Journey pattern id
            journey_pattern_id = jp.get("id")

            # Section reference
            section_ref = get_text(jp, "./txc:JourneyPatternSectionRefs")

            # Direction
            direction = get_direction(get_text(jp, "./txc:Direction"))

            # Headsign
            headsign = origin if direction == 0 else destination
            # Route Reference
            route_ref = get_text(jp, "txc:RouteRef")

            vehicle_type = get_text(
                jp,
                "./txc:Operational/txc:VehicleType/txc:VehicleTypeCode",
                default=None,
            )

            vehicle_description = get_text(
                jp, "./txc:Operational/txc:VehicleType/txc:Description", default=None
            )

            yield (
                journey_pattern_id,
                service_code,
                agency_id,
                line_name,
                mode,
                service_description,
                headsign,
                section_ref,
                direction,
                route_ref,
                vehicle_type,
                vehicle_description,
                start_date,
                end_date,
            )

    return pd.DataFrame(
        process_service(service),
        columns=_JOURNEY_PATTERN_COLUMNS,
    )
