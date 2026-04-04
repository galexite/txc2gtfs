import textwrap

from duckdb import DuckDBPyConnection


def get_calendar(conn: DuckDBPyConnection) -> None:
    conn.execute(
        textwrap.dedent("""
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

    INSERT INTO calendar
    SELECT
        service_id,
        'monday' IN operation_days AS monday,
        'tuesday' IN operation_days AS tuesday,
        'wednesday' IN operation_days AS wednesday,
        'thursday' IN operation_days AS thursday,
        'friday' IN operation_days AS friday,
        'saturday' IN operation_days OR 'weekend' IN operation_days AS saturday,
        'sunday' IN operation_days OR 'weekend' IN operation_days AS sunday,
        start_date,
        end_date
    FROM (
        SELECT
            s.service_code AS service_id,
            lower(vj.operation_days) AS operation_days,
            s.start_date AS start_date,
            s.end_date AS end_date
        FROM txc_services s
        JOIN txc_vehicle_journeys vj
        ON s.service_code = vj.service_ref
    )
    """)
    )
