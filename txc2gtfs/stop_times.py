from typing import cast

import pandas as pd

from txc2gtfs.transxchange import TransXChange


def get_stop_times(txc: TransXChange) -> pd.DataFrame:
    """Extract stop_times attributes from GTFS info DataFrame"""
    return cast(pd.DataFrame, txc.services)[
        [
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
            "timepoint",
        ]
    ].dropna()
