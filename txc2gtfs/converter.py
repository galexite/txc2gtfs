"""
Convert transXchange data format to GTFS format.

The TransXChange model) has seven basic concepts: Service, Registration, Operator,
Route, StopPoint, JourneyPattern, and VehicleJourney.

- A Service brings together the information about a registered bus service, and may
  contain two types of component service: Standard or Flexible; a mix of both
  types is allowed within a single Service.

- A normal bus schedule is described by a StandardService and a Route. A Route
  describes the physical path taken by buses on the service as a set of routelinks.

- A FlexibleService describes a bus service that does not have a fixed route, but
  only a catchment area or a few variable stops with no prescribed pattern of use.

- A StandardService has one or more JourneyPattern elements to describe the common
  logical path of traversal of the stops of the Route as a sequence of timing
  links (see later) and one or more VehicleJourney elements, which describe
  individual scheduled journeys by buses over the Route and JourneyPattern at a
  specific time.

- Both types of service have a registered Operator, who runs the service. Other
  associated operator roles can also be specified.

- Route, JourneyPattern and VehicleJourney follow a sequence of NaPTAN StopPoints. A
  Route specifies in effect an ordered list of StopPoints. A JourneyPattern specifies an
  ordered list of links between these points, giving relative times between each
  stop; a VehicleJourney follows the same list of stops at specific absolute passing
  times. (The detailed timing Link and elements that connect VehicleJourneys,
  JourneyPatterns etc to StopPoints are not shown in Figure 3-1). StopPoints may be
  grouped within StopAreas.

- The StopPoints used in a JourneyPattern or Route are either declared locally or by
  referenced to an external definition using an AnnotatedStopRef.

- A Registration specifies the registration details for a service. It is mandatory
  in the registration schema.

Author
------
Dr. Henrikki Tenkanen, University College London

License
-------

MIT.
"""

from __future__ import annotations

import dataclasses
import tempfile
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
from duckdb import DuckDBPyConnection

from transxchange import TransXChange

from .calendar import get_calendar
from .stop_times import get_stop_times
from .trips import get_trips

if TYPE_CHECKING:
    from _typeshed import StrPath


def _register_with_duckdb(txc: TransXChange, conn: DuckDBPyConnection) -> None:
    for field in dataclasses.fields(txc):
        if field.name == "metadata":
            continue
        conn.register(f"txc_{field.name}", getattr(txc, field.name))


def get_stops(conn: DuckDBPyConnection) -> None:
    conn.execute(
        """
    CREATE OR REPLACE TABLE stops (
        stop_id VARCHAR PRIMARY KEY,
        stop_name VARCHAR,
        stop_lat DECIMAL(8, 6),
        stop_lon DECIMAL(9, 6)
    );

    INSERT INTO stops
    SELECT
        stop_id,
        stop_name,
        NULL as stop_lat,
        NULL as stop_lon
    FROM txc_stop_points
    """
    )


def get_routes(conn: DuckDBPyConnection) -> None:
    conn.execute(
        """
    CREATE OR REPLACE TABLE routes (
        route_id VARCHAR PRIMARY KEY,
        agency_id VARCHAR,
        route_short_name VARCHAR,
        route_long_name VARCHAR,
        route_type INTEGER
    );

    INSERT INTO routes
    SELECT DISTINCT
        line_id AS route_id,
        agency_id,
        line_name as route_short_name,
        description as route_long_name,
        travel_mode AS route_type
    FROM txc_services
    WHERE direction_id = 'outbound'
    """
    )


def get_agency(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE agency (
        agency_id VARCHAR PRIMARY KEY,
        agency_name VARCHAR,
        agency_url VARCHAR,
        agency_timezone VARCHAR
    );

    INSERT INTO agency
    SELECT
        agency_id,
        agency_name,
        '' as agency_url,
        'Europe/London' as agency_timezone
    FROM txc_operators
    """)


def append_txc_to_duckdb_conn(path: Path, conn: DuckDBPyConnection) -> None:
    # Parse GTFS info containing data about trips, calendar, stop_times and
    # calendar_dates
    txc = TransXChange.from_file(path)

    _register_with_duckdb(txc, conn)

    get_stops(conn)
    get_stop_times(conn)
    get_trips(conn)
    get_calendar(conn)
    # get_calendar_dates(conn)
    get_routes(conn)
    get_agency(conn)


GTFS_FILES = [
    "agency.txt",
    "calendar.txt",
    "routes.txt",
    "stop_times.txt",
    "stops.txt",
    "trips.txt",
]


def convert(
    input: Iterable[StrPath],
    output: StrPath,
) -> None:
    """
    Converts TransXchange formatted schedule data into GTFS feed.

    input_filepath : str
        File path to data directory or a ZipFile containing one or multiple TransXchange
        .xml files. Also nested ZipFiles are supported (i.e. a ZipFile with ZipFile(s)
        containing .xml files.)
    output_filepath : str
        Full filepath to the output GTFS zip-file, e.g. '/home/myuser/data/my_gtfs.zip'
    """
    with duckdb.connect() as conn:
        for txc_file in input:
            append_txc_to_duckdb_conn(Path(txc_file), conn)

        with (
            tempfile.TemporaryDirectory(prefix="txc2gtfs-") as temp_dir,
            zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_STORED) as zf,
        ):
            for txt_file_name in GTFS_FILES:
                path = Path(temp_dir) / txt_file_name
                conn.execute(
                    f"COPY {txt_file_name[:-4]} TO ? (FORMAT csv, DATEFORMAT '%Y%m%d')",
                    [str(path)],
                )
                zf.write(path, txt_file_name)
