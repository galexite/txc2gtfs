from __future__ import annotations

from duckdb import DuckDBPyConnection


def get_trips(conn: DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE TABLE trips (
        trip_id VARCHAR PRIMARY KEY,
        route_id VARCHAR,
        service_id VARCHAR,
        trip_headsign VARCHAR,
        trip_short_name VARCHAR,
        direction_id INTEGER,
    );

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
