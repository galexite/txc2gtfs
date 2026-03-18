import textwrap
from typing import cast

import pandas as pd
from duckdb import DuckDBPyConnection


def get_stop_times(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Extract stop_times attributes from GTFS info DataFrame"""
    conn.execute(textwrap.dedent("""
    CREATE TYPE pickup_dropoff_type AS ENUM (
        'regular', 'not_available', 'agency_request', 'driver_request'
    );

    CREATE TYPE timepoint_type AS ENUM (
        'approximate', 'exact'
    );

    CREATE OR REPLACE TABLE stop_times (
        trip_id VARCHAR,
        arrival_time TIME,
        departure_time TIME,
        stop_id VARCHAR,
        stop_sequence INTEGER,
        pickup_type pickup_dropoff_type,
        dropoff_type pickup_dropoff_type,
        timepoint timepoint_type
    );

    INSERT INTO stop_times
    SELECT
        concat(service_ref, ':', vehicle_journey_id) AS trip_id,
        departure_time + running_arrival_time AS arrival_time,
        departure_time + running_departure_time AS departure_time,
        stop_id,
        stop_sequence,
        CASE WHEN activity IN ('pickUp', 'pickUpAndSetDown')
            THEN 'regular'::pickup_dropoff_type
            ELSE 'not_available'::pickup_dropoff_type
        END AS pickup_type,
        CASE WHEN activity IN ('setDown', 'pickUpAndSetDown')
            THEN 'regular'::pickup_dropoff_type
            ELSE 'not_available'::pickup_dropoff_type
        END AS dropoff_type,
        CASE WHEN timing_point_status = 'principleTimingPoint'
            THEN 'exact'::timepoint_type
            ELSE 'approximate'::timepoint_type
        END AS timepoint_type
    FROM
        -- TODO
    """))
