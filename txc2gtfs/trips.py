from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def get_trips(gtfs_info: pd.DataFrame) -> pd.DataFrame:
    """Extract trips attributes from GTFS info DataFrame"""
    # Extract trips from GTFS info
    trips = (
        gtfs_info[
            ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Ensure correct data types
    trips["direction_id"] = trips["direction_id"].astype(int)

    return trips
