from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import sqlite3

from .util.table import Table
from .util.xml import NS, XMLTree


class AgencyTable(Table):
    def __init__(self, cur: sqlite3.Cursor) -> None:
        cur.execute("""
CREATE TABLE IF NOT EXISTS agency (
    id TEXT PRIMARY KEY,
    name TEXT,
    url TEXT DEFAULT 'N/A',
    timezone TEXT DEFAULT 'Europe/London',
    lang TEXT DEFAULT 'en'
)
""")

    def populate(
        self, cur: sqlite3.Cursor, data: XMLTree, gtfs_info: pd.DataFrame
    ) -> None:
        def gen_agencies() -> Generator[tuple[str, str], None, None]:
            # Agency id
            for operator_el in data.iterfind("./txc:Operators/txc:Operator", NS):
                agency_id = operator_el.get("id")
                assert agency_id

                # Agency name
                agency_name_el = operator_el.find("txc:TradingName", NS)
                assert agency_name_el is not None
                agency_name = agency_name_el.text
                assert agency_name

                yield (
                    agency_id,
                    agency_name,
                )

        cur.executemany(
            "INSERT OR IGNORE INTO agency(id, name) VALUES (?, ?)",
            gen_agencies(),
        )
