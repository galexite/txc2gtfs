from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

from .util.table import Table
from .util.xml import NS, XMLTree


class AgencyTable(Table):
    def __init__(self, cur: sqlite3.Cursor) -> None:
        cur.execute("""
CREATE TABLE IF NOT EXISTS agency (
    id CHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    url VARCHAR(255) DEFAULT 'N/A',
    timezone VARCHAR(255) DEFAULT 'Europe/London',
    lang CHAR(2) DEFAULT 'en'
)
""")

    def populate(self, cur: sqlite3.Cursor, data: XMLTree) -> None:
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
