from duckdb import DuckDBPyConnection

from .util.network import download_cached

_NAPTAN_CSV_URL = "https://beta-naptan.dft.gov.uk/Download/National/csv"


def load_naptan_stops(conn: DuckDBPyConnection) -> None:
    stops_csv = download_cached(_NAPTAN_CSV_URL, "Stops.csv")
    conn.execute(
        """
    CREATE TABLE naptan_stops(
        ATCOCode VARCHAR PRIMARY KEY,
        Latitude DECIMAL(8, 6),
        Longitude DECIMAL(9, 6)
    );

    INSERT INTO naptan_stops
    SELECT ATCOCode, Latitude, Longitude FROM read_csv(?, header = true)
    """,
        [str(stops_csv)],
    )
