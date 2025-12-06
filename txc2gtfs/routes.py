from __future__ import annotations

from collections.abc import Generator
from sqlite3 import Cursor
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import sqlite3

from .util.table import Table
from .util.xml import NS, XMLElement, XMLTree, get_text


def get_mode(service: XMLElement) -> int:
    """Parse mode from TransXChange value"""
    mode = get_text(service, "txc:Mode", default=None)
    if mode in ["tram", "trolleyBus"]:
        return 0
    elif mode in ["underground", "metro"]:
        return 1
    elif mode == "rail":
        return 2
    elif mode in ["bus", "coach"]:
        return 3
    elif mode == "ferry":
        return 4

    return 3  # default to bus


def get_route_type(data: XMLTree) -> int:
    """Returns the route type according GTFS reference"""
    mode = data.find("./txc:Services/txc:Service", NS)
    return get_mode(mode) if mode is not None else 3


_ROUTES_COLS = [
    "route_id",
    "agency_id",
    "route_private_id",
    "route_long_name",
    "route_short_name",
    "route_type",
    "route_section_id",
]


class RoutesTable(Table):
    def __init__(self, cur: sqlite3.Cursor) -> None:
        cur.execute("""
CREATE TABLE IF NOT EXISTS routes (
    id TEXT PRIMARY KEY,
    agency_id TEXT,
    private_id TEXT,
    long_name TEXT,
    short_name TEXT,
    type NUMERIC,
    section_id TEXT,
    FOREIGN KEY(agency_id) REFERENCES agency(id)
)
""")

    def populate(self, cur: Cursor, data: XMLTree, gtfs_info: pd.DataFrame) -> None:
        def parse_routes() -> Generator[tuple[str | int | None, ...], None, None]:
            for r in data.iterfind("./txc:Routes/txc:Route", NS):
                # Get route id
                route_id = r.get("id")

                # Get agency_id
                row = gtfs_info.loc[gtfs_info["route_id"] == route_id].iloc[0]
                agency_id: str = row["agency_id"]
                line_name: str = row["line_name"]

                # Get route long name
                route_long_name = get_text(r, "txc:Description")

                # Get route private id
                route_private_id = get_text(r, "txc:PrivateCode")

                # Route Section reference (might be needed somewhere)
                route_section_id = get_text(r, "txc:RouteSectionRef")

                # Get route_type
                route_type = get_route_type(data)

                # Generate row
                yield (
                    route_id,
                    agency_id,
                    route_private_id,
                    route_long_name,
                    line_name,
                    route_type,
                    route_section_id,
                )

        cur.executemany(
            "INSERT OR IGNORE INTO routes(id, agency_id, private_id, long_name, "
            "short_name, type, section_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            parse_routes(),
        )
