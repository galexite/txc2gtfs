import dataclasses
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, NotRequired, TypedDict, cast

import pandas as pd
from lxml import etree

from txc2gtfs.calendar import _parse_service_operation_days
from txc2gtfs.calendar_dates import _parse_service_non_operation_days
from txc2gtfs.util.xml import NS


@dataclass(slots=True, frozen=True)
class TransXChangeMeta:
    filename: str
    creation_date: datetime
    modification_date: datetime | None
    revision_num: int

    dataset_id: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_id", f"{self.filename}#{self.revision_num}")


@dataclass(slots=True, frozen=True)
class TransXChange:
    metadata: TransXChangeMeta
    journey_pattern_sections: pd.DataFrame | None = None
    journey_timing_links: pd.DataFrame | None = None
    vehicle_journeys: pd.DataFrame | None = None
    services: pd.DataFrame | None = None
    routes: pd.DataFrame | None = None
    stop_points: pd.DataFrame | None = None
    operators: pd.DataFrame | None = None
    route_sections: pd.DataFrame | None = None
    route_locations: pd.DataFrame | None = None


class _ParseResult(TypedDict):
    """
    Utility type to help type-check dicts returned by parser functions.
    Must be kept up-to-date to match above.
    """

    metadata: NotRequired[TransXChangeMeta]
    journey_pattern_sections: NotRequired[pd.DataFrame]
    journey_timing_links: NotRequired[pd.DataFrame]
    vehicle_journeys: NotRequired[pd.DataFrame]
    services: NotRequired[pd.DataFrame]
    routes: NotRequired[pd.DataFrame]
    stop_points: NotRequired[pd.DataFrame]
    operators: NotRequired[pd.DataFrame]
    route_sections: NotRequired[pd.DataFrame]
    route_locations: NotRequired[pd.DataFrame]


type _ParserFn = Callable[[etree.Element, TransXChangeMeta], _ParseResult]

_PARSERS: dict[str, _ParserFn] = {}


def _register_parser(elem_name: str) -> Callable[[_ParserFn], _ParserFn]:
    def decorate(fn: _ParserFn) -> _ParserFn:
        _PARSERS[elem_name] = fn
        return fn

    return decorate


@_register_parser("JourneyPatternSections")
def _parse_journey_pattern_sections(
    sections: etree.Element, metadata: TransXChangeMeta
) -> _ParseResult:
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
        section_qname = etree.QName(NS["txc"], "JourneyPatternSection")
        link_qname = etree.QName(NS["txc"], "JourneyPatternTimingLink")

        for section in sections.iterchildren(section_qname):
            journey_pattern_section_id = section.get("id")
            assert journey_pattern_section_id
            last_link: etree.Element | None = None
            for link in section.iterchildren(link_qname):
                if last_link is not None:
                    last_link.clear()
                from_point = link.find("./txc:From", NS)
                assert from_point is not None
                journey_pattern_timing_link_id = link.get("id")
                assert journey_pattern_timing_link_id is not None

                yield create_row(
                    journey_pattern_section_id,
                    journey_pattern_timing_link_id,
                    from_point,
                )

                last_link = link

            # For the last stop, we'll take the 'To' segment.
            assert last_link is not None
            to_point = last_link.find("./txc:To", NS)
            assert to_point is not None
            journey_pattern_timing_link_id = last_link.get("id")
            assert journey_pattern_timing_link_id

            yield create_row(
                journey_pattern_section_id, journey_pattern_timing_link_id, to_point
            )

            last_link.clear()
            section.clear()

    df = pd.DataFrame(generate_rows())
    return {"journey_pattern_sections": df}


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

@_register_parser("VehicleJourneys")
def _parse_vehicle_journeys(
    journeys: etree.Element, metadata: TransXChangeMeta
) -> _ParseResult:
    journey_qname = etree.QName(NS["txc"], "VehicleJourney")
    def generate_vehicle_journey_rows():
        for journey in journeys.iterchildren(journey_qname):
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
            departure_time = time.fromisoformat(departure_time)

            yield {
                "service_ref": service_ref,
                "line_ref": line_ref,
                "vehicle_journey_id": vehicle_journey_id,
                "journey_pattern_id": journey_pattern_id,
                "operation_days": operation_days,
                "non_operative_days": non_operative_days,
                "departure_time": departure_time,
            }

    journeys_df = pd.DataFrame(generate_vehicle_journey_rows())

    def generate_timing_link_rows():
        for journey in journeys.iterchildren(journey_qname):
            service_ref = journey.findtext("txc:ServiceRef", None, NS)
            assert service_ref
            # Get line reference
            line_ref = journey.findtext("txc:LineRef", None, NS)
            assert line_ref

            vehicle_journey_id = journey.findtext("txc:VehicleJourneyCode", None, NS)
            assert vehicle_journey_id

            timing_link_qname = etree.QName(NS["txc"], "VehicleJourneyTimingLink")
            runtime_pat = re.compile(r"PT(?:(?P<h>\d+)H)?(?P<m>\d+)M(?P<s>\d+)S")
            for timing_link in journey.iterchildren(timing_link_qname):
                vehicle_journey_timing_link_id = timing_link.get("id")
                assert vehicle_journey_timing_link_id

                journey_pattern_timing_link_id = timing_link.findtext(
                    "txc:JourneyPatternTimingLinkRef", None, NS
                )
                assert journey_pattern_timing_link_id

                if runtime := timing_link.findtext("txc:RunTime", None, NS):
                    runtime_match = runtime_pat.match(runtime)
                    assert runtime_match
                    h, m, s = runtime_match.group("h", "m", "s")

                    runtime_duration = timedelta(
                        hours=int(h or 0), minutes=int(m), seconds=int(s)
                    )
                else:
                    runtime_duration = timedelta()

                yield {
                    "service_ref": service_ref,
                    "line_ref": line_ref,
                    "vehicle_journey_id": vehicle_journey_id,
                    "vehicle_journey_timing_link_id": vehicle_journey_timing_link_id,
                    "journey_pattern_timing_link_id": journey_pattern_timing_link_id,
                    "runtime": runtime_duration,
                }

    timing_links_df = pd.DataFrame(generate_timing_link_rows())
    return {"journey_timing_links": timing_links_df, "vehicle_journeys": journeys_df}


@_register_parser("Routes")
def _parse_routes(routes: etree.Element, metadata: TransXChangeMeta) -> _ParseResult:
    def generate_rows():
        route_qname = etree.QName(NS["txc"], "Route")

        for route in routes.iterchildren(route_qname):
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

            route.clear()

    df = pd.DataFrame(generate_rows())
    return {"routes": df}


@_register_parser("StopPoints")
def _parse_stop_points(
    points: etree.Element, metadata: TransXChangeMeta
) -> _ParseResult:
    def generate_rows():
        point_qname = etree.QName(NS["txc"], "AnnotatedStopPointRef")

        for point in points.iterchildren(point_qname):
            stop_id = point.findtext("./txc:AtcoCode", None, NS) or point.findtext(
                "./txc:StopPointRef", None, NS
            )
            assert stop_id, "No AtcoCode or StopPointRef for StopPoint"

            stop_name = point.findtext("./txc:CommonName", None, NS)
            assert stop_name, "No CommonName for StopPoint"

            yield {"stop_id": stop_id, "stop_name": stop_name}

            point.clear()

    df = pd.DataFrame(generate_rows())
    return {"stop_points": df}


@_register_parser("Operators")
def _parse_operators(
    operators: etree.Element, metadata: TransXChangeMeta
) -> _ParseResult:
    def generate_rows():
        operator_qname = etree.QName(NS["txc"], "Operator")

        for operator in operators.iterchildren(operator_qname):
            agency_id = operator.get("id")
            assert agency_id

            # Agency name
            agency_name = operator.findtext("txc:TradingName", None, NS)
            assert agency_name

            yield {"agency_id": agency_id, "agency_name": agency_name}

            operator.clear()

    df = pd.DataFrame(generate_rows())
    return {"operators": df}


def _parse_direction(direction: str) -> Literal[0] | Literal[1]:
    """Return boolean direction id"""
    if direction == "inbound":
        return 0
    elif direction == "outbound":
        return 1

    raise ValueError(f"Cannot determine direction from {direction}")


@_register_parser("Services")
def _parse_services(
    services: etree.Element, metadata: TransXChangeMeta
) -> _ParseResult:
    def generate_rows():
        service_qname = etree.QName(NS["txc"], "Service")

        for service in services.iterchildren(service_qname):
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

                for pattern in service.iterfind(
                    "./txc:StandardService/txc:JourneyPattern", NS
                ):
                    # Journey pattern id
                    journey_pattern_id = pattern.get("id")

                    # Section reference
                    section_ref = pattern.findtext(
                        "./txc:JourneyPatternSectionRefs", None, NS
                    )

                    # Direction
                    direction = pattern.findtext("./txc:Direction", None, NS)
                    assert direction

                    # Headsign
                    headsign = origin if direction == 0 else destination
                    # Route Reference
                    route_ref = pattern.findtext("txc:RouteRef", None, NS)

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

                    pattern.clear()

                line.clear()

            service.clear()

    df = pd.DataFrame(generate_rows())
    return {"services": df}


@_register_parser("RouteSections")
def _parse_route_sections(
    route_sections: etree.Element, metadata: TransXChangeMeta
) -> _ParseResult:
    def generate_route_link_rows():
        route_section_qname = etree.QName(NS["txc"], "RouteSection")
        route_link_qname = etree.QName(NS["txc"], "RouteLink")

        for route_section in route_sections.iterchildren(route_section_qname):
            id = route_section.get("id")
            assert id
            for link_seq_num, link in enumerate(
                route_section.iterchildren(route_link_qname)
            ):
                link_id = link.get("id")
                assert link_id
                link_from = link.findtext("./txc:From/txc:StopPointRef", None, NS)
                assert link_from
                link_to = link.findtext("./txc:To/txc:StopPointRef", None, NS)
                assert link_to
                distance = link.findtext("./txc:Distance", None, NS)
                assert distance

                yield {
                    "route_section_id": id,
                    "route_link_id": link_id,
                    "route_link_seq_num": link_seq_num,
                    "route_link_from": link_from,
                    "route_link_to": link_to,
                    "route_link_distance": int(distance),
                }

    route_sections_df = pd.DataFrame(generate_route_link_rows())

    def generate_route_track_rows():
        for link in route_sections.iterfind("./txc:RouteSection/txc:RouteLink", NS):
            link_id = link.get("id")
            for loc_seq_num, loc in enumerate(
                link.iterfind("./txc:Track/txc:Mapping/txc:Location", NS)
            ):
                loc_id = loc.get("id")
                assert loc_id
                loc_lat = loc.findtext("./txc:Latitude", None, NS)
                assert loc_lat
                loc_lon = loc.findtext("./txc:Longitude", None, NS)
                assert loc_lon

                yield {
                    "route_link_id": link_id,
                    "location_id": loc_id,
                    "location_seq_num": loc_seq_num,
                    "latitude": Decimal(loc_lat),
                    "longitude": Decimal(loc_lon),
                }

                loc.clear()

            loc.clear()

    route_locations_df = pd.DataFrame(generate_route_track_rows())

    return {"route_sections": route_sections_df, "route_locations": route_locations_df}


def _parse_metadata(elem: etree.Element) -> TransXChangeMeta:
    filename = elem.get("FileName")
    assert filename is not None
    creation_date = elem.get("CreationDateTime")
    assert creation_date is not None
    creation_date = datetime.fromisoformat(creation_date)
    modification_date = elem.get("ModificationDateTime")
    if modification_date is not None:
        modification_date = datetime.fromisoformat(modification_date)
    revision_num = elem.get("RevisionNumber")
    assert revision_num is not None
    revision_num = int(revision_num)

    return TransXChangeMeta(
        filename=filename,
        creation_date=creation_date,
        modification_date=modification_date,
        revision_num=revision_num,
    )


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
    parsers = _PARSERS
    kwargs: _ParseResult = {}
    metadata: TransXChangeMeta | None = None

    for event, elem in etree.iterparse(
        path,
        ("start", "end"),
        tag=[
            etree.QName(NS["txc"], "TransXChange"),
            *(etree.QName(NS["txc"], tag) for tag in parsers.keys()),
        ],
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
    ):
        tag = etree.QName(cast(str, elem.tag)).localname
        if event == "start":
            if tag == "TransXChange":
                metadata = _parse_metadata(elem)
                kwargs["metadata"] = metadata
            continue
        elif tag == "TransXChange":
            continue

        assert metadata is not None
        kwargs.update(parsers[tag](elem, metadata))

        elem.clear()

    assert "metadata" in kwargs

    return TransXChange(**kwargs)
