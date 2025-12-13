from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Literal, cast

import pandas as pd
from lxml import etree

from txc2gtfs.calendar import _parse_service_operation_days
from txc2gtfs.calendar_dates import (
    _parse_service_non_operation_days,
)
from txc2gtfs.util.xml import NS


@dataclass(slots=True, frozen=True)
class TransXChange:
    journey_pattern_sections: pd.DataFrame | None
    vehicle_journeys: pd.DataFrame | None
    services: pd.DataFrame | None
    routes: pd.DataFrame | None
    stop_points: pd.DataFrame | None
    operators: pd.DataFrame | None


def _get_midnight_formatted_times(
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


def _parse_service_journey_pattern_sections(sections: etree.Element) -> pd.DataFrame:
    def create_row(
        journey_pattern_section_id: str,
        journey_pattern_timing_link_id: str,
        point: etree.Element,
    ):
        stop_id = point.findtext("./txc:StopPointRef", None, NS)
        assert stop_id
        activity = point.findtext("./txc:Activity", None, NS)
        assert activity
        timing_point_status = point.findtext("./txc:TimingStatus", None, NS)
        assert timing_point_status
        fare_stage_number = point.findtext("./txc:FareStageNumber", None, NS)
        assert fare_stage_number

        return {
            "journey_pattern_section_id": journey_pattern_section_id,
            "journey_pattern_timing_link_id": journey_pattern_timing_link_id,
            "stop_id": stop_id,
            "activity": activity,
            "timing_point_status": timing_point_status,
            "fare_stage_number": int(fare_stage_number),
        }

    def generate_rows():
        for section in sections.findall("./txc:JourneyPatternSection", NS):
            journey_pattern_section_id = section.get("id")
            assert journey_pattern_section_id
            links = section.findall("./txc:JourneyPatternTimingLink", NS)
            stop_sequence = 1
            for link in links:
                from_point = link.find("./txc:From", NS)
                assert from_point is not None
                journey_pattern_timing_link_id = link.get("id")
                assert journey_pattern_timing_link_id is not None
                yield create_row(
                    journey_pattern_section_id,
                    journey_pattern_timing_link_id,
                    from_point,
                )

                stop_sequence += 1

            # For the last stop, we'll take the 'To' segment.
            to_point = links[-1].find("./txc:To", NS)
            assert to_point is not None
            journey_pattern_timing_link_id = links[-1].get("id")
            assert journey_pattern_timing_link_id
            yield create_row(
                journey_pattern_section_id, journey_pattern_timing_link_id, to_point
            )

    df = pd.DataFrame(generate_rows())
    df.set_index(
        ["journey_pattern_section_id", "journey_pattern_timing_link_id"], inplace=True
    )
    return df


def _parse_service_mode(service: etree.Element) -> int:
    """Parse mode from TransXChange value"""
    match service.findtext("txc:Mode", None, NS):
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


def _parse_vehicle_journeys(journeys: etree.Element) -> pd.DataFrame:
    def generate_rows():
        for journey in journeys.findall("./txc:VehicleJourney", NS):
            service_ref = journey.findtext("txc:ServiceRef", None, NS)
            # Get line reference
            line_ref = journey.findtext("txc:LineRef", None, NS)
            assert line_ref

            # Journey pattern reference
            journey_pattern_id = journey.findtext("txc:JourneyPatternRef", None, NS)
            assert journey_pattern_id

            # Vehicle journey id ==> will be used to generate service_id (identifies
            # operative weekdays)
            vehicle_journey_id = journey.findtext("txc:VehicleJourneyCode", None, NS)
            assert vehicle_journey_id

            # Parse weekday operation times from VehicleJourney
            operation_days = _parse_service_operation_days(journey)

            # Parse calendar dates (exceptions in operation)
            non_operative_days = _parse_service_non_operation_days(journey)

            departure_time = journey.findtext("txc:DepartureTime", None, NS)
            assert departure_time

            yield {
                "service_ref": service_ref,
                "line_ref": line_ref,
                "vehicle_journey_id": vehicle_journey_id,
                "journey_pattern_id": journey_pattern_id,
                "operation_days": operation_days,
                "non_operative_days": non_operative_days,
                "departure_time": departure_time,
            }

    df = pd.DataFrame(generate_rows())
    df.set_index(["service_ref", "line_ref", "vehicle_journey_id"], inplace=True)
    return df


def _parse_routes(routes: etree.Element) -> pd.DataFrame:
    def generate_rows():
        for route in routes.findall("./txc:Route", NS):
            # Get route id
            route_id = route.get("id")
            assert route_id

            route_long_name = route.findtext("./txc:Description", None, NS)
            assert route_long_name

            route_private_id = route.findtext("./txc:PrivateCode", None, NS)
            assert route_private_id

            route_section_id = route.findtext("./txc:RouteSectionRef", None, NS)
            assert route_section_id

            yield {
                "route_id": route_id,
                "route_private_id": route_private_id,
                "route_long_name": route_long_name,
                "route_section_id": route_section_id,
            }

    df = pd.DataFrame(generate_rows())
    df.set_index("route_id", inplace=True)
    return df


def _parse_stop_points(points: etree.Element) -> pd.DataFrame:
    def generate_rows():
        for point in points.findall("./txc:AnnotatedStopPointRef", NS):
            stop_id = point.findtext("./txc:AtcoCode", None, NS) or point.findtext(
                "./txc:StopPointRef", None, NS
            )
            assert stop_id, "No AtcoCode or StopPointRef for StopPoint"

            stop_name = point.findtext("./txc:CommonName", None, NS)
            assert stop_name, "No CommonName for StopPoint"

            yield {"stop_id": stop_id, "stop_name": stop_name}

    df = pd.DataFrame(generate_rows())
    df.set_index("stop_id", inplace=True)
    return df


def _parse_operators(operators: etree.Element) -> pd.DataFrame:
    def generate_rows():
        for operator in operators.findall("./txc:Operator", NS):
            agency_id = operator.get("id")
            assert agency_id

            # Agency name
            agency_name = operator.findtext("txc:TradingName", None, NS)
            assert agency_name

            yield {"agency_id": agency_id, "agency_name": agency_name}

    df = pd.DataFrame(generate_rows())
    df.set_index("agency_id", inplace=True)
    return df


def _parse_runtime_duration(runtime: str) -> int:
    """Parse duration information from TransXChange runtime code"""
    time = 0
    runtime = runtime.split("PT", maxsplit=1)[-1]

    if "H" in runtime:
        split = runtime.split("H", maxsplit=1)
        time = time + int(split[0]) * 60 * 60
        runtime = split[1]
    if "M" in runtime:
        split = runtime.split("M", maxsplit=1)
        time = time + int(split[0]) * 60
        runtime = split[1]
    if "S" in runtime:
        split = runtime.split("S", maxsplit=1)
        time = time + int(split[0])
    return time


def _parse_direction(direction: str) -> Literal[0] | Literal[1]:
    """Return boolean direction id"""
    if direction == "inbound":
        return 0
    elif direction == "outbound":
        return 1

    raise ValueError(f"Cannot determine direction from {direction}")


def _parse_services(services: etree.Element) -> pd.DataFrame:
    def generate_rows():
        for service in services.findall("./txc:Service", NS):
            # Service code
            service_code = service.findtext("txc:ServiceCode", None, NS)

            # Operator reference code
            agency_id = service.findtext("txc:RegisteredOperatorRef", None, NS)

            mode = _parse_service_mode(service)

            start_date = service.findtext(
                "./txc:OperatingPeriod/txc:StartDate", None, NS
            )
            if start_date:
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

            end_date = service.findtext("./txc:OperatingPeriod/txc:EndDate", None, NS)
            if end_date:
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

            origin = service.findtext("./txc:StandardService/txc:Origin", None, NS)
            destination = service.findtext(
                "./txc:StandardService/txc:Destination", None, NS
            )

            for line in service.iterfind("./txc:Lines/txc:Line", NS):
                line_id = line.get("id")
                assert line_id
                line_name = line.findtext("txc:LineName", None, NS)
                assert line_name

                outbound_description = line.findtext(
                    "./txc:OutboundDescription/txc:Description", None, NS
                )

                inbound_description = line.findtext(
                    "./txc:InboundDescription/txc:Description", None, NS
                )

                for jp in service.iterfind(
                    "./txc:StandardService/txc:JourneyPattern", NS
                ):
                    # Journey pattern id
                    journey_pattern_id = jp.get("id")

                    # Section reference
                    section_ref = jp.findtext(
                        "./txc:JourneyPatternSectionRefs", None, NS
                    )

                    # Direction
                    direction = jp.findtext("./txc:Direction", None, NS)
                    assert direction

                    # Headsign
                    headsign = origin if direction == 0 else destination
                    # Route Reference
                    route_ref = jp.findtext("txc:RouteRef", None, NS)

                    yield {
                        "service_code": service_code,
                        "line_id": line_id,
                        "journey_pattern_id": journey_pattern_id,
                        "agency_id": agency_id,
                        "line_name": line_name,
                        "travel_mode": mode,
                        "description": outbound_description
                        if direction == "outbound"
                        else inbound_description,
                        "trip_headsign": headsign,
                        "journey_pattern_section_id": section_ref,
                        "direction_id": direction,
                        "route_id": route_ref,
                        "start_date": start_date,
                        "end_date": end_date,
                    }

    df = pd.DataFrame(generate_rows())
    df.set_index(["service_code", "line_id", "journey_pattern_id"], inplace=True)
    return df


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
    journey_pattern_sections: pd.DataFrame | None = None
    vehicle_journeys: pd.DataFrame | None = None
    services: pd.DataFrame | None = None
    routes: pd.DataFrame | None = None
    stop_points: pd.DataFrame | None = None
    operators: pd.DataFrame | None = None

    for _, elem in etree.iterparse(
        path,
        events=("end",),
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
    ):
        tag = cast(str, elem.tag).rsplit("}", maxsplit=1)[-1]
        match tag:
            case "JourneyPatternSections":
                assert journey_pattern_sections is None
                journey_pattern_sections = _parse_service_journey_pattern_sections(elem)

            case "Routes":
                assert routes is None
                routes = _parse_routes(elem)

            case "StopPoints":
                assert stop_points is None
                stop_points = _parse_stop_points(elem)

            case "VehicleJourneys":
                assert vehicle_journeys is None
                vehicle_journeys = _parse_vehicle_journeys(elem)

            case "Services":
                assert services is None
                services = _parse_services(elem)

            case "Operators":
                assert operators is None
                operators = _parse_operators(elem)

            case _:
                continue

        elem.clear()

    return TransXChange(
        journey_pattern_sections=journey_pattern_sections,
        vehicle_journeys=vehicle_journeys,
        services=services,
        routes=routes,
        stop_points=stop_points,
        operators=operators,
    )
