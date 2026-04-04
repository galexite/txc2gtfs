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
import os
import textwrap
from collections.abc import Generator, Iterable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import duckdb
from duckdb import DuckDBPyConnection

from .calendar import get_calendar
from .gtfs import export_to_zip
from .stop_times import get_stop_times
from .transxchange import TransXChange, parse_transxchange_file
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
        textwrap.dedent("""
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
    """)
    )


def get_routes(conn: DuckDBPyConnection) -> None:
    conn.execute(
        textwrap.dedent("""
    -- CREATE TYPE route_type_type AS ENUM (
    --     'tram_streetcar_rail', 'subway_metro', 'rail', 'bus', 'ferry', 'cable_tram',
    --     'aerial_lift', 'funicular'
    -- );

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
    """)
    )


def parse_txc_to_sql_conn(path: Path, conn: DuckDBPyConnection) -> None:
    # Parse GTFS info containing data about trips, calendar, stop_times and
    # calendar_dates
    txc = parse_transxchange_file(path)

    _register_with_duckdb(txc, conn)

    get_stops(conn)
    get_stop_times(conn)
    get_trips(conn)
    get_calendar(conn)
    # get_calendar_dates(conn)
    get_routes(conn)
    # get_agency(conn)


def _iterate_paths(input: Iterable[StrPath]) -> Generator[Path, None, None]:
    for path in input:
        path = Path(path)
        if path.is_dir():
            yield from path.glob("*.xml")
            continue
        yield path


def convert(
    input: Iterable[StrPath],
    output: StrPath,
    append_to_existing: bool = False,
    num_workers: int = 1,
) -> None:
    """
    Converts TransXchange formatted schedule data into GTFS feed.

    input_filepath : str
        File path to data directory or a ZipFile containing one or multiple TransXchange
        .xml files. Also nested ZipFiles are supported (i.e. a ZipFile with ZipFile(s)
        containing .xml files.)
    output_filepath : str
        Full filepath to the output GTFS zip-file, e.g. '/home/myuser/data/my_gtfs.zip'
    append_to_existing : bool (default is False)
        Flag for appending to existing gtfs-database. This might be useful if you have
        TransXchange .xml files distributed into multiple directories (e.g. separate
        files for train data, tube data and bus data) and you want to merge all those
        datasets into a single GTFS feed.
    worker_cnt : int
        Number of workers to distribute the conversion process. By default the number of
        CPUs is used.
    """
    input = _iterate_paths(input)
    with TemporaryDirectory(prefix="txc2gtfs-") as temp:
        temp_path = Path(temp)

        def do_parse_txc_to_sql(inp: tuple[int, Path]) -> Path:
            i, txc_file = inp
            with duckdb.connect() as conn:
                parse_txc_to_sql_conn(txc_file, conn)

                path = temp_path / f"worker-{i}"
                path.mkdir()

                conn.execute(
                    textwrap.dedent("""
                COPY stops TO concat($path, 'stops.csv')
                COPY stop_times TO concat($path, 'stop_times.csv')
                COPY trips TO concat($path, 'trips.csv')
                COPY calendar TO concat($path, 'calendar.csv')
                COPY routes TO concat($path, 'routes.csv')
                """),
                    {"path": f"{path}{os.path.sep}"},
                )

                return path

        # Create workers
        if num_workers > 1:
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                worker_output = executor.map(do_parse_txc_to_sql, enumerate(input))
        else:
            worker_output = (do_parse_txc_to_sql((0, txc_file)) for txc_file in input)

        export_to_zip(Path(output), worker_output)
