import textwrap

from duckdb import DuckDBPyConnection


def get_stop_times(conn: DuckDBPyConnection) -> None:
    conn.execute(
        textwrap.dedent("""
    CREATE OR REPLACE TABLE stop_times (
        trip_id VARCHAR,
        arrival_time TIME,
        departure_time TIME,
        stop_id VARCHAR,
        stop_sequence INTEGER,
        pickup_type INTEGER,
        dropoff_type INTEGER,
        timepoint INTEGER
    );

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
            THEN 0
            ELSE 1 -- 'not available'
        END AS pickup_type,
        CASE WHEN activity IN ('setDown', 'pickUpAndSetDown')
            THEN 0
            ELSE 1 -- 'not available'
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
    )
