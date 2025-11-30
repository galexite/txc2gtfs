import pandas as pd


def get_stop_times(gtfs_info: pd.DataFrame) -> pd.DataFrame:
    """Extract stop_times attributes from GTFS info DataFrame"""
    # Select columns
    return gtfs_info[
        [
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
            "timepoint",
        ]
    ].dropna()
