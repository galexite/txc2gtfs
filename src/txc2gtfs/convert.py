"""
Convert TransXChange data format to GTFS format.

TODO: translate from OSGeo coordinate systems
TODO: support a JourneyPattern referencing multiple JourneyPatternSectionRefs
TODO: feedinfo.txt
TODO: check column types
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
from transxchange import Timetable

from txc2gtfs.bank_holidays import load_bank_holidays
from txc2gtfs.naptan import load_naptan_stops

if TYPE_CHECKING:
    from _typeshed import StrPath


def _create_stops(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE stops (
        stop_id VARCHAR PRIMARY KEY,
        stop_name VARCHAR,
        stop_lat DECIMAL(8, 6),
        stop_lon DECIMAL(9, 6)
    )
    """)


def _insert_stops(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO stops
    SELECT
        sp.stop_id,
        sp.stop_name,
        naptan.Latitude as stop_lat,
        naptan.Longitude as stop_lon
    FROM txc_stop_points sp
    JOIN naptan_stops naptan ON sp.stop_id = naptan.ATCOCode
    """)


def _create_routes(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE routes (
        route_id VARCHAR PRIMARY KEY,
        agency_id VARCHAR,
        route_short_name VARCHAR,
        route_type INTEGER
    )
    """)


def _insert_routes(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO routes
    SELECT DISTINCT
        l.line_id AS route_id,
        s.agency_id,
        l.line_name as route_short_name,
        s.mode AS route_type
    FROM txc_services s
    NATURAL JOIN txc_lines l
    """)


def _create_agency(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE agency (
        agency_id VARCHAR PRIMARY KEY,
        agency_name VARCHAR,
        agency_url VARCHAR,
        agency_timezone VARCHAR
    )
    """)


def _insert_agency(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO agency
    SELECT
        agency_id,
        agency_name,
        'https://www.traveline.info/' AS agency_url,
        'Europe/London' AS agency_timezone
    FROM txc_operators
    """)


def _create_stop_times(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE stop_times (
        trip_id VARCHAR,
        arrival_time VARCHAR,
        departure_time VARCHAR,
        stop_id VARCHAR,
        stop_sequence INTEGER,
        pickup_type INTEGER,
        drop_off_type INTEGER,
        timepoint INTEGER
    )
    """)


def _insert_stop_times(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    WITH

    base AS (
        SELECT
            vj.service_ref,
            vj.vehicle_journey_id,
            vj.journey_pattern_id,
            jps.from_stop_point_ref,
            jps.from_activity,
            jps.from_timing_status,
            jps.from_sequence_number,
            jps.to_stop_point_ref,
            jps.to_activity,
            jps.to_timing_status,
            jps.to_sequence_number,
            epoch(vj.departure_time)::UINTEGER as departure_time_s,
            epoch(jtl.runtime)::UINTEGER as runtime_s,
            jps.journey_pattern_section_id
        FROM txc_vehicle_journeys vj
        NATURAL JOIN txc_journey_timing_links jtl
        NATURAL JOIN txc_journey_pattern_sections jps
    ),

    all_points_except_last AS (
        SELECT
            service_ref,
            vehicle_journey_id,
            journey_pattern_id,
            from_stop_point_ref AS stop_id,
            from_activity AS activity,
            from_timing_status AS timing_status,
            from_sequence_number AS stop_sequence,
            service_ref,
            departure_time_s,
            runtime_s
        FROM base
    ),

    last_points AS (
        SELECT
            service_ref,
            vehicle_journey_id,
            journey_pattern_id,
            to_stop_point_ref AS stop_id,
            to_activity AS activity,
            to_timing_status AS timing_status,
            to_sequence_number AS stop_sequence,
            service_ref,
            departure_time_s,
            runtime_s
        FROM base
        WHERE (journey_pattern_section_id, to_sequence_number) IN (
            SELECT
                journey_pattern_section_id,
                MAX(to_sequence_number) AS to_sequence_number
            FROM base
            GROUP BY journey_pattern_section_id
        )
    ),

    all_points AS (
        SELECT
            *,
            departure_time_s + coalesce(
                SUM(runtime_s) OVER (
                    PARTITION BY vehicle_journey_id, journey_pattern_id
                    ORDER BY stop_sequence ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0
            ) AS stop_time
        FROM (
            SELECT * FROM all_points_except_last
            UNION ALL
            SELECT * FROM last_points
        )
    ),

    with_formatted_time AS (
        SELECT
            *,
            format('{:02d}:{:02d}:{:02d}',
                (stop_time // 3600),
                (stop_time // 60) % 60,
                stop_time % 60
            ) as stop_time_formatted
        FROM all_points
    )

    INSERT INTO stop_times
    SELECT
        (service_ref || ':' || vehicle_journey_id || ':' || journey_pattern_id)
            AS trip_id,
        stop_time_formatted as arrival_time,
        stop_time_formatted as departure_time,
        stop_id,
        stop_sequence,
        CASE WHEN activity IN ('pickUp', 'pickUpAndSetDown')
            THEN 0 -- regular
            ELSE 1 -- not available
        END AS pickup_type,
        CASE WHEN activity IN ('setDown', 'pickUpAndSetDown')
            THEN 0 -- regular
            ELSE 1 -- not available
        END AS drop_off_type,
        CASE WHEN timing_status = 'principalTimingPoint'
            THEN 1 -- exact
            ELSE 0 -- approximate
        END AS timepoint_type
    FROM with_formatted_time
    """)


def _create_calendar(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE calendar (
        service_id VARCHAR PRIMARY KEY,
        monday INTEGER NOT NULL,
        tuesday INTEGER NOT NULL,
        wednesday INTEGER NOT NULL,
        thursday INTEGER NOT NULL,
        friday INTEGER NOT NULL,
        saturday INTEGER NOT NULL,
        sunday INTEGER NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
    );
    """)


def _insert_calendar(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO calendar
    SELECT
        service_id,
        'Monday' IN days_of_week AS monday,
        'Tuesday' IN days_of_week AS tuesday,
        'Wednesday' IN days_of_week AS wednesday,
        'Thursday' IN days_of_week AS thursday,
        'Friday' IN days_of_week AS friday,
        'Saturday' IN days_of_week OR 'Weekend' IN days_of_week AS saturday,
        'Sunday' IN days_of_week OR 'Weekend' IN days_of_week AS sunday,
        start_date,
        CASE WHEN end_date IS NULL
            THEN start_date + INTERVAL 5 YEARS
            ELSE end_date
        END AS end_date
    FROM (
        SELECT
            (s.service_code || ':' || vj.vehicle_journey_id) AS service_id,
            vj.days_of_week AS days_of_week,
            s.start_date AS start_date,
            s.end_date AS end_date
        FROM txc_vehicle_journeys vj
        JOIN txc_services s ON vj.service_ref = s.service_code
    )
    """)


def load_bank_holiday_map(conn: duckdb.DuckDBPyConnection) -> None:
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


def _create_calendar_dates(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE calendar_dates (
        service_id VARCHAR,
        date DATE,
        exception_type INTEGER
    );
    """)


def _insert_calendar_dates(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    WITH

    fixed_holiday_dates AS (
        SELECT
            (s.service_code || ':' || vj.vehicle_journey_id) AS service_id,
            s.start_date,
            coalesce(s.end_date, s.start_date + INTERVAL 5 YEARS) AS end_date,
            vj.days_of_week,
            CASE nod.key
                WHEN 'NewYearsDay'    THEN MAKE_DATE(YEAR(d.date), 1,  1)
                WHEN 'Jan2ndScotland' THEN MAKE_DATE(YEAR(d.date), 1,  2)
                WHEN 'StAndrewsDay'   THEN MAKE_DATE(YEAR(d.date), 11, 30)
                WHEN 'ChristmasDay'   THEN MAKE_DATE(YEAR(d.date), 12, 25)
                WHEN 'BoxingDay'      THEN MAKE_DATE(YEAR(d.date), 12, 26)
            END AS holiday_date
        FROM txc_vehicle_journeys vj
        JOIN txc_services s ON s.service_code = vj.service_ref
        CROSS JOIN UNNEST(vj.days_of_non_operation) AS nod(key)
        CROSS JOIN (
            SELECT DISTINCT DATE_TRUNC('year', range_date) AS year_start
            FROM GENERATE_SERIES(
                (SELECT MIN(start_date) FROM txc_services),
                coalesce(
                    (SELECT MAX(end_date) FROM txc_services),
                    ((SELECT MAX(start_date) FROM txc_services) + INTERVAL 5 YEARS)
                ),
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
            (s.service_code || ':' || vj.vehicle_journey_id) AS service_id,
            s.start_date,
            s.end_date,
            vj.days_of_week,
            bh.date AS holiday_date
        FROM txc_vehicle_journeys vj
        CROSS JOIN UNNEST(vj.days_of_non_operation) AS nod(key)
        JOIN bank_holiday_map bhm ON bhm.key = nod.key
        JOIN bank_holidays bh     ON bh.title = bhm.value
        JOIN txc_services s       ON vj.service_ref = s.service_code
        WHERE bh.date BETWEEN
            s.start_date AND coalesce(s.end_date, s.start_date + INTERVAL 5 YEARS)
    ),

    all_holiday_dates AS (
        SELECT service_id, holiday_date, days_of_week
        FROM fixed_holiday_dates
        WHERE holiday_date IS NOT NULL
        AND holiday_date BETWEEN start_date AND end_date

        UNION ALL

        SELECT service_id, holiday_date, days_of_week
        FROM variable_holiday_dates
    )

    INSERT INTO calendar_dates
    SELECT DISTINCT
        service_id,
        holiday_date AS date,
        2 as exception_type
    FROM all_holiday_dates
    WHERE dayname(holiday_date) IN days_of_week
    ORDER BY holiday_date;
    """)


def _create_trips(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE trips (
        trip_id VARCHAR PRIMARY KEY,
        route_id VARCHAR,
        service_id VARCHAR,
        trip_headsign VARCHAR,
        trip_short_name VARCHAR,
        direction_id INTEGER,
        shape_id VARCHAR
    )
    """)


def _insert_trips(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO trips
    SELECT
        (s.service_code || ':' || vj.vehicle_journey_id || ':' || jp.journey_pattern_id)
            AS trip_id,
        l.line_id as route_id,
        (s.service_code || ':' || vj.vehicle_journey_id) as service_id,
        CASE WHEN direction_id = 'inbound'
            THEN s.origin
            ELSE s.destination
        END AS trip_headsign,
        CASE WHEN direction_id = 'inbound'
            THEN l.inbound_description
            ELSE l.outbound_description
        END AS trip_short_name,
        CASE WHEN direction_id = 'inbound' THEN 1 ELSE 0 END AS direction_id,
        jp.route_id AS shape_id
    FROM txc_journey_patterns jp
    JOIN txc_vehicle_journeys vj ON vj.journey_pattern_id = jp.journey_pattern_id
    JOIN txc_services s ON vj.service_ref = s.service_code
    JOIN txc_lines l ON vj.service_ref = l.service_code
    """)


def _create_shapes(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE shapes (
        shape_id VARCHAR,
        shape_pt_lat DECIMAL(8, 6),
        shape_pt_lon DECIMAL(9, 6),
        shape_pt_sequence UINTEGER,
        shape_dist_traveled UINTEGER
    )
    """)


def _insert_shapes(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    INSERT INTO shapes
    SELECT
        r.route_id AS shape_id,
        rl.latitude AS shape_pt_lat,
        rl.longitude AS shape_pt_lon,
        ROW_NUMBER() OVER (
            PARTITION BY r.route_id
            ORDER BY rs.route_link_seq_num, rl.location_seq_num ASC
        ) AS shape_pt_sequence,
        coalesce(
            SUM(rs.route_link_distance) OVER (
                PARTITION BY r.route_id
                ORDER BY rs.route_link_seq_num, rl.location_seq_num ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
        ) AS shape_dist_traveled
    FROM txc_routes r
    NATURAL JOIN txc_route_sections rs
    NATURAL JOIN txc_route_locations rl
    """)


@contextlib.contextmanager
def _register_with_duckdb(
    txc: Timetable, conn: duckdb.DuckDBPyConnection
) -> Generator[None, None, None]:
    fields = [f.name for f in dataclasses.fields(txc) if f.name != "metadata"]
    for field in fields:
        conn.register(f"txc_{field}", getattr(txc, field))

    try:
        yield
    finally:
        for field in fields:
            conn.unregister(f"txc_{field}")


def _insert_txc_file(path: Path, conn: duckdb.DuckDBPyConnection) -> None:
    txc = Timetable.from_file(path)

    with _register_with_duckdb(txc, conn):
        _insert_agency(conn)
        _insert_calendar_dates(conn)
        _insert_calendar(conn)
        _insert_routes(conn)
        _insert_shapes(conn)
        _insert_stop_times(conn)
        _insert_stops(conn)
        _insert_trips(conn)


GTFS_FILES = [
    "agency.txt",
    "calendar_dates.txt",
    "calendar.txt",
    "routes.txt",
    "shapes.txt",
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
        _create_agency(conn)
        _create_calendar_dates(conn)
        _create_calendar(conn)
        _create_routes(conn)
        _create_shapes(conn)
        _create_stop_times(conn)
        _create_stops(conn)
        _create_trips(conn)

        load_bank_holidays(conn)
        load_bank_holiday_map(conn)
        load_naptan_stops(conn)

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
