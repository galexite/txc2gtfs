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

import contextlib
import dataclasses
import tempfile
import zipfile
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
from duckdb import DuckDBPyConnection

from transxchange import Timetable
from txc2gtfs.bank_holidays import load_bank_holidays

if TYPE_CHECKING:
    from _typeshed import StrPath


def _create_stops(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE stops (
        stop_id VARCHAR PRIMARY KEY,
        stop_name VARCHAR,
        stop_lat DECIMAL(8, 6),
        stop_lon DECIMAL(9, 6)
    )
    """)


def _insert_stops(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO stops
    SELECT
        stop_id,
        stop_name,
        NULL as stop_lat,
        NULL as stop_lon
    FROM txc_stop_points
    """)


def _create_routes(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE routes (
        route_id VARCHAR PRIMARY KEY,
        agency_id VARCHAR,
        route_short_name VARCHAR,
        route_long_name VARCHAR,
        route_type INTEGER
    )
    """)


def _insert_routes(conn: DuckDBPyConnection) -> None:
    conn.execute("""
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


def _create_agency(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE agency (
        agency_id VARCHAR PRIMARY KEY,
        agency_name VARCHAR,
        agency_url VARCHAR,
        agency_timezone VARCHAR
    )
    """)


def _insert_agency(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO agency
    SELECT
        agency_id,
        agency_name,
        '' AS agency_url,
        'Europe/London' AS agency_timezone
    FROM txc_operators
    """)


def _create_stop_times(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE stop_times (
        trip_id VARCHAR,
        arrival_time TIME,
        departure_time TIME,
        stop_id VARCHAR,
        stop_sequence INTEGER,
        pickup_type INTEGER,
        dropoff_type INTEGER,
        timepoint INTEGER
    )
    """)


def _insert_stop_times(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO stop_times
    SELECT
        concat(service_ref, ':', journey_pattern_id) AS trip_id,
        departure_time + (COALESCE(SUM(epoch(runtime)) OVER (
            PARTITION BY journey_pattern_id
            ORDER BY stop_sequence ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::BIGINT * INTERVAL '1 second') AS arrival_time,
        departure_time + (COALESCE(SUM(epoch(runtime)) OVER (
            PARTITION BY journey_pattern_id
            ORDER BY stop_sequence ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::BIGINT * INTERVAL '1 second') AS departure_time,
        stop_id,
        stop_sequence,
        CASE WHEN activity IN ('pickUp', 'pickUpAndSetDown')
            THEN 0 -- regular
            ELSE 1 -- not available
        END AS pickup_type,
        CASE WHEN activity IN ('setDown', 'pickUpAndSetDown')
            THEN 0 -- regular
            ELSE 1 -- not available
        END AS dropoff_type,
        CASE WHEN timing_point_status = 'principalTimingPoint'
            THEN 1 -- exact
            ELSE 0 -- approximate
        END AS timepoint_type
    FROM (
        SELECT
            vj.service_ref,
            vj.journey_pattern_id,
            vj.departure_time,
            jtl.runtime,
            jps.stop_id,
            jps.activity,
            jps.timing_point_status,
            ROW_NUMBER() OVER (
                PARTITION BY vj.journey_pattern_id
                ORDER BY jtl.vehicle_journey_timing_link_id
            ) AS stop_sequence
        FROM txc_vehicle_journeys vj
        JOIN txc_journey_timing_links jtl
            ON vj.vehicle_journey_id = jtl.vehicle_journey_id
        JOIN txc_journey_pattern_sections jps
            ON jtl.journey_pattern_timing_link_id = jps.journey_pattern_timing_link_id
    )
    """)


def _create_calendar(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE calendar (
        service_id VARCHAR,
        monday INTEGER,
        tuesday INTEGER,
        wednesday INTEGER,
        thursday INTEGER,
        friday INTEGER,
        saturday INTEGER,
        sunday INTEGER,
        start_date DATE,
        end_date DATE
    );
    """)


def _insert_calendar(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO calendar
    SELECT
        service_id,
        'Monday' IN operation_days AS monday,
        'Tuesday' IN operation_days AS tuesday,
        'Wednesday' IN operation_days AS wednesday,
        'Thursday' IN operation_days AS thursday,
        'Friday' IN operation_days AS friday,
        'Saturday' IN operation_days OR 'Weekend' IN operation_days AS saturday,
        'Sunday' IN operation_days OR 'Weekend' IN operation_days AS sunday,
        start_date,
        end_date
    FROM (
        SELECT
            s.service_code AS service_id,
            vj.operation_days AS operation_days,
            s.start_date AS start_date,
            s.end_date AS end_date
        FROM txc_services s
        JOIN txc_vehicle_journeys vj
        ON s.service_code = vj.service_ref
    )
    """)


def load_bank_holiday_map(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE bank_holiday_map (
        key VARCHAR PRIMARY KEY,
        value VARCHAR
    );

    INSERT INTO bank_holiday_map (key, value)
    VALUES
        ('NewYearsDayHoliday', 'New Year\u2019s Day'),
        ('Jan2ndScotlandHoliday', '2nd January'),
        ('GoodFriday', 'Good Friday'),
        ('EasterMonday', 'Easter Monday'),
        ('MayDay', 'Early May bank holiday'),
        ('SpringBank', 'Spring bank holiday'),
        ('LateSummerBankHolidayNotScotland', 'Summer bank holiday'),
        ('AugustBankHolidayScotland', 'Summer bank holiday'),
        ('StAndrewsDayHoliday', 'St Andrew\u2019s Day'),
        ('ChristmasDayHoliday', 'Christmas Day'),
        ('BoxingDayHoliday', 'Boxing Day')
    """)


def _create_calendar_dates(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE calendar_dates (
        service_id VARCHAR,
        date DATE,
        exception_type INTEGER
    );
    """)


def _insert_calendar_dates(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    WITH

    fixed_holiday_dates AS (
        SELECT
            s.service_code AS service_id,
            s.start_date,
            s.end_date,
            vj.non_operative_days,
            CASE nod.key
                WHEN 'NewYearsDay'    THEN MAKE_DATE(YEAR(d.date), 1,  1)
                WHEN 'Jan2ndScotland' THEN MAKE_DATE(YEAR(d.date), 1,  2)
                WHEN 'StAndrewsDay'   THEN MAKE_DATE(YEAR(d.date), 11, 30)
                WHEN 'ChristmasDay'   THEN MAKE_DATE(YEAR(d.date), 12, 25)
                WHEN 'BoxingDay'      THEN MAKE_DATE(YEAR(d.date), 12, 26)
            END AS holiday_date
        FROM txc_vehicle_journeys vj
        JOIN txc_services s ON s.service_code = vj.service_ref
        CROSS JOIN UNNEST(vj.non_operative_days) AS nod(key)
        CROSS JOIN (
            SELECT DISTINCT DATE_TRUNC('year', range_date) AS year_start
            FROM GENERATE_SERIES(
                (SELECT MIN(start_date) FROM txc_services),
                (SELECT MAX(end_date)   FROM txc_services),
                INTERVAL '1 day'
            ) AS t(range_date)
        ) AS years
        CROSS JOIN LATERAL (
            SELECT MAKE_DATE(YEAR(years.year_start), 1, 1) AS date
        ) AS d
        WHERE nod.key IN (
            'NewYearsDay', 'Jan2ndScotland', 'StAndrewsDay', 'ChristmasDay', 'BoxingDay'
        )
    ),

    variable_holiday_dates AS (
        SELECT
            s.service_code AS service_id,
            s.start_date,
            s.end_date,
            bh.date AS holiday_date
        FROM txc_vehicle_journeys vj
        CROSS JOIN UNNEST(vj.non_operative_days) AS nod(key)
        JOIN bank_holiday_map bhm ON bhm.key = nod.key
        JOIN bank_holidays bh     ON bh.title = bhm.value
        JOIN txc_services s       ON s.service_code = vj.service_ref
        WHERE bh.date BETWEEN s.start_date AND s.end_date
    ),

    all_holiday_dates AS (
        SELECT service_id, holiday_date
        FROM fixed_holiday_dates
        WHERE holiday_date IS NOT NULL
        AND holiday_date BETWEEN start_date AND end_date

        UNION ALL

        SELECT service_id, holiday_date
        FROM variable_holiday_dates
    )

    INSERT INTO calendar_dates
    SELECT DISTINCT
        service_id,
        holiday_date AS date,
        2 as exception_type
    FROM all_holiday_dates
    ORDER BY holiday_date;
    """)


def _create_trips(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE trips (
        trip_id VARCHAR PRIMARY KEY,
        route_id VARCHAR,
        service_id VARCHAR,
        trip_headsign VARCHAR,
        trip_short_name VARCHAR,
        direction_id INTEGER,
    )
    """)


def _insert_trips(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO trips
    SELECT
        concat(service_code, ':', journey_pattern_id) AS trip_id,
        line_id as route_id,
        service_code as service_id,
        trip_headsign,
        description AS trip_short_name,
        CASE WHEN direction_id = 'inbound' THEN 1 ELSE 0 END
    FROM txc_services
    """)


@contextlib.contextmanager
def _register_with_duckdb(
    txc: Timetable, conn: DuckDBPyConnection
) -> Generator[None, None, None]:
    fields = [f.name for f in dataclasses.fields(txc) if f.name != "metadata"]
    for field in fields:
        conn.register(f"txc_{field}", getattr(txc, field))

    yield

    for field in fields:
        conn.unregister(f"txc_{field}")


def _insert_txc_file(path: Path, conn: DuckDBPyConnection) -> None:
    txc = Timetable.from_file(path)

    with _register_with_duckdb(txc, conn):
        _insert_stops(conn)
        _insert_stop_times(conn)
        _insert_trips(conn)
        _insert_calendar(conn)
        _insert_calendar_dates(conn)
        _insert_routes(conn)
        _insert_agency(conn)


GTFS_FILES = [
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
    "stop_times.txt",
    "stops.txt",
    "trips.txt",
]


def convert(
    input: Iterable[StrPath],
    output: StrPath,
) -> None:
    """Convert TransXchange timetable files into GTFS.

    Args:
        input: Paths to TransXChange timetable XML files to convert
        output: Path to the output GTFS zip file to generate
    """
    with duckdb.connect() as conn:
        _create_stops(conn)
        _create_stop_times(conn)
        _create_trips(conn)
        _create_calendar(conn)
        _create_calendar_dates(conn)
        _create_routes(conn)
        _create_agency(conn)

        load_bank_holidays(conn)
        load_bank_holiday_map(conn)

        for txc_file in input:
            _insert_txc_file(Path(txc_file), conn)

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
