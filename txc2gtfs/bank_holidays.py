from duckdb import DuckDBPyConnection

from .util.network import download_cached

_BANK_HOLIDAYS_JSON_URL = "https://www.gov.uk/bank-holidays.json"


def load_bank_holidays(conn: DuckDBPyConnection) -> None:
    bank_holidays_path = download_cached(_BANK_HOLIDAYS_JSON_URL)

    conn.execute("""
    CREATE TABLE bank_holidays AS
    WITH
        divisions AS (
            SELECT
                key AS division,
                UNNEST(
                    json_extract(content, '$.' || key || '.events')::JSON[]
                ) AS event
            FROM (
                SELECT
                    UNNEST(json_keys(content)) AS key,
                    content
                FROM (
                    SELECT json AS content
                    FROM read_json_objects(?, format = 'unstructured')
                )
            )
        ),
        events AS (
            SELECT
                (event->>'date')::DATE AS date,
                event->>'title' AS title,
                event->>'notes' AS notes,
                (event->>'bunting')::BOOLEAN AS bunting,
                division
            FROM divisions
        )
    SELECT
        date,
        title,
        notes,
        ANY_VALUE(bunting)  AS bunting,
        array_agg(division) AS divisions
    FROM events
    GROUP BY date, title, notes
    ORDER BY date
    """, [str(bank_holidays_path)])
