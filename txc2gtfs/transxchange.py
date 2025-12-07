from collections.abc import Generator, Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd
from lxml import etree

from txc2gtfs.calendar import _parse_service_operation_days
from txc2gtfs.calendar_dates import (
    _parse_service_non_operation_days,
)
from txc2gtfs.util.xml import NS, get_text


@dataclass
class Line:
    id: str
    name: str


@dataclass(slots=True)
class Service:
    code: str
    journey_patterns: pd.DataFrame
    operation_days: str | None
    non_operation_days: str | None
    lines: dict[str, Line]
    mode: int


type Route = tuple[str, str, str, str]
type StopPoint = tuple[str, str]
type Operator = tuple[str, str]


@dataclass(slots=True, frozen=True)
class TransXChange:
    gtfs_info: pd.DataFrame
    routes: list[Route]


@dataclass(slots=True, frozen=True)
class VehicleJourney:
    service_ref: str
    line_ref: str
    journey_pattern_id: str
    vehicle_journey_id: str
    operation_days: str | None
    non_operative_days: str | None
    departure_time: str


def get_last_stop_time_info(
    link: etree.Element,
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


_VEHICLE_JOURNEYS_COLUMNS = pd.Index(
    [
        "vehicle_journey_id",
        "service_ref",
        "journey_pattern_id",
        "weekdays",
        "non_operative_days",
    ]
)


def get_vehicle_journeys(journeys: Iterator[etree.Element]) -> pd.DataFrame:
    """Process vehicle journeys"""

    # Iterate over VehicleJourneys
    def process_vehicle_journey(
        journey: etree.Element,
    ) -> tuple[str, str, str, str | None, str | None]:
        # Get service reference
        service_ref = get_text(journey, "txc:ServiceRef")

        # Journey pattern reference
        journey_pattern_id = get_text(journey, "txc:JourneyPatternRef")

        # Vehicle journey id ==> will be used to generate service_id (identifies
        # operative weekdays)
        vehicle_journey_id = get_text(journey, "txc:VehicleJourneyCode")

        # Parse weekday operation times from VehicleJourney
        weekdays = _parse_service_operation_days(journey)

        # Parse calendar dates (exceptions in operation)
        non_operative_days = _parse_service_non_operation_days(journey)

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


_SECTION_TIMES_COLS = pd.Index(
    [
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
)


def process_vehicle_journey(
    journey: VehicleJourney,
    sections: list[etree.Element],
    services: dict[str, Service],
) -> pd.DataFrame:
    # Get current date for time reference
    current_date = datetime.now().date()

    # If additional boarding time is needed, specify it here
    # Boarding time in seconds
    boarding_time = 0

    # Get service reference
    service = services[journey.service_ref]
    line = service.lines[journey.line_ref]

    # Select service journey patterns for given service id
    journey_pattern = cast(
        pd.Series, service.journey_patterns.loc[journey.journey_pattern_id]
    )

    # Ensure integer values
    direction_id = cast(np.int64, journey_pattern["direction_id"])
    travel_mode = cast(np.int64, journey_pattern["travel_mode"])

    # Get departure time
    hour, minute, _ = [int(s) for s in journey.departure_time.split(":", maxsplit=2)]

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
        trip_id = f"{section_id}_{journey.operation_days}_{hour:02}{minute:02}"

        links = section.findall("txc:JourneyPatternTimingLink", NS)

        def get_duration(link: etree.Element) -> int:
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
                    journey_pattern["agency_id"],
                    trip_id,
                    journey_pattern["route_id"],
                    journey.vehicle_journey_id,
                    journey.service_ref,
                    direction_id,
                    line.name,
                    travel_mode,
                    journey_pattern["trip_headsign"],
                    journey_pattern["vehicle_type"],
                    journey_pattern["start_date"],
                    journey_pattern["end_date"],
                    journey.operation_days,
                    journey.non_operative_days,
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
        last_stop["vehicle_journey_id"] = journey.vehicle_journey_id
        last_stop["service_ref"] = journey.service_ref
        last_stop["direction_id"] = direction_id
        last_stop["line_name"] = line.name
        last_stop["travel_mode"] = travel_mode
        last_stop["trip_headsign"] = trip_headsign
        last_stop["vehicle_type"] = vehicle_type
        last_stop["start_date"] = start_date
        last_stop["end_date"] = end_date
        last_stop["weekdays"] = journey.operation_days
        last_stop["non_operative_days"] = journey.non_operative_days
        section_times = pd.concat([section_times, last_stop], ignore_index=True)

    assert section_times is not None
    return section_times


def _parse_service_lines(
    service: etree.Element,
) -> Generator[tuple[str, Line], None, None]:
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


def _parse_service_mode(service: etree.Element) -> int:
    """Parse mode from TransXChange value"""
    match get_text(service, "txc:Mode", default=None):
        case "tram" | "trolleyBus":
            return 0
        case "underground" | "metro":
            return 1
        case "rail":
            return 2
        case "bus" | "coach":
            return 3
        case "ferry":
            return 4

    return 3  # default to bus


def _parse_vehicle_journey(journey: etree.Element) -> VehicleJourney:
    service_ref = get_text(journey, "txc:ServiceRef")
    # Get line reference
    line_ref = get_text(journey, "txc:LineRef")

    # Journey pattern reference
    journey_pattern_id = get_text(journey, "txc:JourneyPatternRef")

    # Vehicle journey id ==> will be used to generate service_id (identifies operative
    # weekdays)
    vehicle_journey_id = get_text(journey, "txc:VehicleJourneyCode")

    # Parse weekday operation times from VehicleJourney
    operation_days = _parse_service_operation_days(journey)

    # Parse calendar dates (exceptions in operation)
    non_operative_days = _parse_service_non_operation_days(journey)

    departure_time = get_text(journey, "txc:DepartureTime")

    return VehicleJourney(
        service_ref,
        line_ref,
        journey_pattern_id,
        vehicle_journey_id,
        operation_days,
        non_operative_days,
        departure_time,
    )


def _parse_route(route: etree.Element) -> Route:
    # Get route id
    route_id = route.get("id")
    assert route_id

    # Get route long name
    route_long_name = get_text(route, "txc:Description")

    # Get route private id
    route_private_id = get_text(route, "txc:PrivateCode")

    # Route Section reference (might be needed somewhere)
    route_section_id = get_text(route, "txc:RouteSectionRef")

    return (
        route_id,
        route_private_id,
        route_long_name,
        route_section_id,
    )


def _parse_stop_point(stop_point: etree.Element) -> StopPoint:
    # Stop_id
    stop_id_el = stop_point.find("txc:AtcoCode", NS) or stop_point.find(
        "txc:StopPointRef", NS
    )
    assert stop_id_el is not None, "No AtcoCode for StopPoint"
    stop_id = stop_id_el.text
    assert stop_id, "Empty AtcoCode for StopPoint"

    # Name of the stop
    stop_name_el = stop_point.find("txc:CommonName", NS)
    assert stop_name_el is not None, "No CommonName for StopPoint"
    stop_name = stop_name_el.text
    assert stop_name, "Empty CommonName for StopPoint"

    return (stop_id, stop_name)


def _parse_operator(operator: etree.Element) -> Operator:
    agency_id = operator.get("id")
    assert agency_id

    # Agency name
    agency_name_el = operator.find("txc:TradingName", NS)
    assert agency_name_el is not None
    agency_name = agency_name_el.text
    assert agency_name

    return (agency_id, agency_name)


def parse_transxchange_file(path: Path) -> TransXChange:
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
    journey_pattern_sections: list[etree.Element] = []
    vehicle_journeys: list[VehicleJourney] = []
    services: dict[str, Service] = {}
    routes: list[Route] = []
    stop_points: list[StopPoint] = []
    operators: list[Operator] = []

    for _, elem in etree.iterparse(
        path,
        events=("end",),
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
    ):
        tag = cast(str, elem.tag).rsplit("}", maxsplit=1)[-1]
        match tag:
            case "JourneyPatternSection":
                journey_pattern_sections.append(elem)
                continue

            case "VehicleJourney":
                vehicle_journeys.append(_parse_vehicle_journey(elem))
                continue

            case "Service":
                code = get_text(elem, "txc:ServiceCode")
                services[code] = Service(
                    code=code,
                    journey_patterns=_parse_service_journey_patterns(elem),
                    operation_days=_parse_service_operation_days(elem),
                    non_operation_days=_parse_service_non_operation_days(elem),
                    lines=dict(_parse_service_lines(elem)),
                    mode=_parse_service_mode(elem),
                )

            case "Route":
                routes.append(_parse_route(elem))

            case "StopPoint" | "AnnotatedStopPointRef":
                stop_points.append(_parse_stop_point(elem))

            case "Operator":
                operators.append(_parse_operator(elem))

            case _:
                continue

        elem.clear()

    gtfs_info = pd.concat(
        process_vehicle_journey(
            vehicle_journey,
            journey_pattern_sections,
            services,
        )
        for vehicle_journey in vehicle_journeys
    )

    # Generate service_id column into the table
    gtfs_info = generate_service_id(gtfs_info)

    return TransXChange(gtfs_info, routes)


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


_JOURNEY_PATTERN_COLUMNS = pd.Index(
    [
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
)


def _parse_service_journey_patterns(service: etree.Element) -> pd.DataFrame:
    """Retrieve a DataFrame of all JourneyPatterns of the service"""

    def process_service(
        service: etree.Element,
    ) -> Generator[tuple[Any, ...], None, None]:
        # Service description
        service_description: str | None = None
        if service_description_el := service.find("txc:Description", NS):
            service_description = service_description_el.text

        # Travel mode
        mode = _parse_service_mode(service)

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
    ).set_index("journey_pattern_id")
