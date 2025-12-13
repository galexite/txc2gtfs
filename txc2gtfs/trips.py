from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import pandas as pd


def get_trips(gtfs_info: pd.DataFrame) -> pd.DataFrame:
    """Extract trips attributes from GTFS info DataFrame"""
    # Extract trips from GTFS info
    return cast(
        pd.DataFrame,
        gtfs_info[
            ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"]
        ]
        .drop_duplicates()
        .reset_index(drop=True),
    )
