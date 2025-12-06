from collections.abc import Generator
from sqlite3 import Cursor

import pandas as pd

from .util.network import download_cached
from .util.table import Table
from .util.xml import NS, XMLTree

_NAPTAN_CSV_URL = "https://beta-naptan.dft.gov.uk/Download/National/csv"
_COLUMNS = {
    "ATCOCode": "id",
    "CommonName": "name",
    "Latitude": "lat",
    "Longitude": "lon",
}


def read_naptan_stops() -> pd.DataFrame:
    """
    Reads NaPTAN stops, downloading them if necessary.
    """
    naptan_fp = download_cached(_NAPTAN_CSV_URL, "Stops.csv")

    stops = pd.read_csv(naptan_fp, header=0, usecols=_COLUMNS.keys(), low_memory=False)  # type: ignore

    # Rename required columns into GTFS format
    return stops.rename(columns=_COLUMNS)


class StopsTable(Table):
    def __init__(self, cur: Cursor) -> None:
        cur.execute("""
CREATE VIRTUAL TABLE IF NOT EXISTS stops USING rtree(
    id,
    minX, maxX,      -- Minimum and maximum X coordinate
    minY, maxY,      -- Minimum and maximum Y coordinate
    +atco TEXT,
    +name TEXT,
)
""")

    def populate(self, cur: Cursor, data: XMLTree, gtfs_info: pd.DataFrame) -> None:
        stop_points = data.find("txc:StopPoints", NS)
        if stop_points is None:
            raise ValueError("No StopPoints element. Could not parse stop information.")

        # Get stop database
        naptan_stops = read_naptan_stops()

        def gen_stoppoint_ids() -> Generator[str, None, None]:
            for point in stop_points.iterfind("./txc:StopPoints/txc:StopPoint", NS):
                # Name of the stop
                stop_name_el = point.find("./txc:Descriptor/txc:CommonName", NS)
                assert stop_name_el is not None, "No CommonName for StopPoint"
                stop_name = stop_name_el.text
                assert stop_name, "Empty CommonName for StopPoint"

                # Stop_id
                stop_id_el = point.find("./txc:AtcoCode", NS)
                assert stop_id_el is not None, "No AtcoCode for StopPoint"
                stop_id = stop_id_el.text
                assert stop_id, "Empty AtcoCode for StopPoint"
                yield stop_id

        def gen_annotatedstoppoint_ids() -> Generator[str, None, None]:
            # Iterate over stop points using TransXchange version 2.1
            for point in stop_points.iterfind("./txc:AnnotatedStopPointRef", NS):
                # Stop_id
                stop_ref_el = point.find("./txc:StopPointRef", NS)
                assert stop_ref_el is not None, "Invalid AnnotatedStopPointRef"
                stop_id = stop_ref_el.text
                assert stop_id, "Empty StopPointRef"
                yield stop_id

        if stop_points.find("txc:StopPoint", NS) is not None:
            stop_ids = pd.Series(list(gen_stoppoint_ids()))
        elif stop_points.find("txc:AnnotatedStopPointRef", NS):
            stop_ids = pd.Series(list(gen_annotatedstoppoint_ids()))
        else:
            raise ValueError("No StopPoint or AnnotatedStopPointRef elements.")

        unrecognized_stop_ids = stop_ids[~stop_ids.isin(naptan_stops["id"])]
        assert unrecognized_stop_ids.empty, (
            f"Unrecognised stop ids: {unrecognized_stop_ids.to_list()}"
        )

        def gen_sql_rows() -> Generator[tuple[str, str, float, float]]:
            for _, row in naptan_stops[naptan_stops["id"].isin(stop_ids)].iterrows():
                yield row["id"], row["name"], row["lat"], row["lon"]

        cur.executemany(
            "INSERT OR IGNORE INTO stops(id, name, lat, lon) VALUES (?, ?, ?, ?)",
            gen_sql_rows(),
        )
