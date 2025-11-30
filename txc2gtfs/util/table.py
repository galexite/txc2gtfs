from __future__ import annotations

import abc
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import sqlite3

    from .xml import XMLTree


class Table(abc.ABC):
    @abc.abstractmethod
    def __init__(self, cur: sqlite3.Cursor) -> None: ...

    @abc.abstractmethod
    def populate(
        self, cur: sqlite3.Cursor, data: XMLTree, gtfs_info: pd.DataFrame
    ) -> None: ...
