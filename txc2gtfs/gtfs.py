from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd


def export_to_zip(db: Path, output: Path) -> None:
    """Reads the gtfs database and generates an export dictionary for GTFS"""
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as zf:

        def write(name: str, data: pd.DataFrame) -> None:
            with zf.open(name, "w") as f:
                data.to_csv(
                    f,
                    sep=",",
                    index=False,
                    quotechar='"',
                    quoting=csv.QUOTE_NONNUMERIC,
                )

        with sqlite3.connect(db) as conn:
            # Stops
            # -----
            stops = pd.read_sql_query("SELECT * FROM stops", conn)
            # Drop duplicates based on stop_id
            write(
                "stops.txt",
                stops.rename(
                    {
                        "id": "stop_id",
                        "name": "stop_name",
                        "lat": "stop_lat",
                        "lon": "stop_lon",
                    }
                ),
            )

            # Agency
            # ------
            agency = pd.read_sql_query("SELECT * FROM agency", conn)
            # Drop duplicates
            write(
                "agency.txt",
                agency.rename(
                    {
                        "id": "agency_id",
                        "name": "agency_name",
                        "url": "agency_url",
                        "timezone": "agency_timezone",
                        "lang": "agency_lang",
                    }
                ),
            )
            # Routes
            # ------
            routes = pd.read_sql_query("SELECT * FROM routes", conn)
            if "index" in routes.columns:
                routes = routes.drop("index", axis=1)
            # Drop duplicates
            write("routes.txt", routes.drop_duplicates(subset=["route_id"]))

            # Trips
            # -----
            trips = pd.read_sql_query("SELECT * FROM trips", conn)
            if "index" in trips.columns:
                trips = trips.drop("index", axis=1)

            # Drop duplicates
            write("trips.txt", trips.drop_duplicates(subset=["trip_id"]))

            # Stop_times
            # ----------
            stop_times = pd.read_sql_query("SELECT * FROM stop_times", conn)
            if "index" in stop_times.columns:
                stop_times = stop_times.drop("index", axis=1)

            # Drop duplicates
            write("stop_times.txt", stop_times.drop_duplicates())

            # Calendar
            # --------
            calendar = pd.read_sql_query("SELECT * FROM calendar", conn)
            if "index" in calendar.columns:
                calendar = calendar.drop("index", axis=1)
            # Drop duplicates
            write("calendar.txt", calendar.drop_duplicates(subset=["service_id"]))

            # Calendar dates
            # --------------
            calendar_dates = pd.read_sql_query("SELECT * FROM calendar_dates", conn)
            if "index" in calendar_dates.columns:
                calendar_dates = calendar_dates.drop("index", axis=1)
            # Drop duplicates
            write(
                "calendar_dates.txt",
                calendar_dates.drop_duplicates(subset=["service_id"]),
            )
