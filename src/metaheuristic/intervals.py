from collections import defaultdict
from typing import Any, DefaultDict, Dict, Hashable, List, Self, Tuple, cast

import pandas as pd

from .constants import INVALID_ID

ListForInterval = List[Tuple[pd.Timestamp, pd.Timestamp, List[Any]]]


class Intervals:
    """
    A list of intervals with additional data
    to identify intervals belonging to different categories.
    If two different intervals have the same additional data,
    they must not overlap
    """

    data: pd.DataFrame
    """
    The underlying data in the format

    Index:
        pd.RangeIndex
    Columns:
        Name: start_time,                                       dtype: pd.Timestamp
        Name: end_time,                                         dtype: pd.Timestamp
        Name: <id_column_names[0]>,                             dtype: ?
        ...
        Name: <id_column_names[len(id_column_names) - 1]>,      dtype: ?
    """

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        start_tag: int,
        end_tag: int,
        cols_to_keep: List[str] = [],
    ) -> Self:
        """
        Create Intervals from a dataframe
        Requires the dataframe to be sorted on its DatetimeIndex
        and all intervals to be "finished", i.e. have an end.
        Stores columns with names in cols_to_keep and requires these
        to be the same at the beginning and end of interval to detect
        intervals properly

        :param df: dataframe to process
        Index:
            pd.DatetimeIndex
        Columns:
            Name: event_type,                           dtype: int64
            Name: cols_to_keep[0],                      dtype: ?
            ...
            Name: cols_to_keep[len(cols_to_keep) - 1],  dtype: ?


        :param start_tag: value in "event_type" column which signifies a start of the interval
        :param end_tag: value in `tag_column` which signifies an end of the interval
        :param tag_column: column in df to use to detect start/end of an interval
        :param cols_to_keep: which columns to

        :returns: a sorted dataframe of intervals
        :rtype: pd.DataFrame

        Index:
            pd.RangeIndex
        Columns:
            Name: start_time,                           dtype: pd.Timestamp
            Name: end_time,                             dtype: pd.Timestamp
            Name: cols_to_keep[0],                      dtype: ?
            ...
            Name: cols_to_keep[len(cols_to_keep) - 1],  dtype: ?
        """
        # Time should be sorted
        assert df.index.is_monotonic_increasing

        start_times: List[pd.Timestamp] = []
        kept_data: Dict[str, List[Any]] = {col: [] for col in cols_to_keep}
        kept_data["start_time"] = []
        kept_data["end_time"] = []

        # Keep track of variables needed for parsing,
        # separately for each combination of data

        # Default initial time is 0
        last_time_for_data: DefaultDict[Hashable, pd.Timestamp] = defaultdict(
            lambda: pd.to_datetime(0, origin="unix")
        )
        # first event starts creating an interval
        is_new_interval_for_data: DefaultDict[Hashable, bool] = defaultdict(
            lambda: True
        )

        # Only keep the rows relating to the intervals
        df = df[df["event_type"].isin([start_tag, end_tag])]

        for time, row in df.iterrows():
            time = cast(pd.Timestamp, time)
            data = row[cols_to_keep]
            hashable_data: Hashable = tuple(data.to_list())

            is_new_interval = is_new_interval_for_data[hashable_data]
            last_time = last_time_for_data[hashable_data]

            if is_new_interval:
                assert row["event_type"] == start_tag
            else:
                assert row["event_type"] == end_tag

                # Create an "unoccupied window"
                start_times.append(last_time)
                for col in cols_to_keep:
                    kept_data[col].append(data[col])
                kept_data["start_time"].append(last_time)
                kept_data["end_time"].append(time)

            last_time_for_data[hashable_data] = time
            is_new_interval_for_data[hashable_data] = not is_new_interval

        # Check that all intervals have been closed
        for hashable_data in last_time_for_data:
            assert is_new_interval_for_data[hashable_data]

        out = cls()
        out.data = pd.DataFrame(
            data=kept_data,
            columns=(["start_time", "end_time"] + cols_to_keep),
            index=start_times,
        )

        return out

    @classmethod
    def from_list(
        cls,
        data: ListForInterval,
        column_names: List[str],
    ) -> Self:
        """
        Creates Intervals from a list of tuples

        :param data: List of times and other data to use for construction
        :param column_names: names of columns for extra data
        """

        squashed_data = [
            [start_time, end_time] + rest
            for start_time, end_time, rest in data
        ]

        intervals = pd.DataFrame(
            squashed_data,
            columns=(["start_time", "end_time"] + column_names),
        ).sort_values(by=["start_time"])

        # TODO: Check that intervals are valid: start_time<=end_time and
        # non-overlapping
        # start_times = intervals["start_time"]
        # end_times = intervals["end_time"]
        # assert (start_times <= end_times).all()
        # # Non-overlapping
        # assert (end_times[:-1] <= start_times[1:]).all()

        out = cls()
        out.data = intervals
        return out

    def intersect_on_column(
        self,
        other: Self,
        self_col: str,
        other_col: str,
        self_cols_to_keep: List[str],
        other_cols_to_keep: List[str],
    ) -> Self:
        """
        Returns a an intersection of two sets of intervals, "joining"
        on `self_col` and `other_col`. Specifically,

        For each interval I in `self`, if it has `data[self_col]` set to v,
        return an intersection of I and (union of intervals in
        `other` that have the same value of `data[other_col]`)

        :param self_cols_to_keep: names of columns to keep from self's data
        :param other_cols_to_keep: names of columns to keep from other's data
        """

        new_intervals: List[pd.DataFrame] = []

        for _, interval in self.data.iterrows():
            start_time: pd.Timestamp = interval["start_time"]
            end_time: pd.Timestamp = interval["end_time"]
            data: Any = interval[self_col]

            assert data is not INVALID_ID

            # Select constraint intervals which intersect interval
            # and have relevant data
            constraint_intervals: pd.DataFrame = other.data[
                other.data[other_col]
                == data & (other.data["end_time"] >= start_time)
                # index is start_time
                & (other.data["start_time"] <= end_time)
            ]
            constraint_intervals = constraint_intervals[
                ["start_time", "end_time"] + other_cols_to_keep
            ].copy()

            # Add columns from self
            constraint_intervals[self_cols_to_keep] = interval[
                self_cols_to_keep
            ]

            # We need to clamp constraint_intervals to start
            # and end at appropriate times
            if not constraint_intervals.empty:
                start_time_column_index = constraint_intervals.columns.get_loc(
                    "start_time"
                )
                end_time_column_index = constraint_intervals.columns.get_loc(
                    "end_time"
                )
                assert type(start_time_column_index) is int
                assert type(end_time_column_index) is int

                first_start_time = constraint_intervals.iloc[
                    0, start_time_column_index
                ]
                assert type(first_start_time) is pd.Timestamp
                last_end_time = constraint_intervals.iloc[
                    -1, end_time_column_index
                ]
                assert type(last_end_time) is pd.Timestamp

                # Clamp by start_time below
                if first_start_time < start_time:
                    constraint_intervals.iloc[0, start_time_column_index] = (
                        start_time
                    )

                # Clamp by end_time above
                if last_end_time > end_time:
                    constraint_intervals.iloc[-1, end_time_column_index] = (
                        end_time
                    )

                new_intervals.append(constraint_intervals)

        out = type(self)()
        out.data = pd.concat(new_intervals)
        return out

    def __repr__(self):
        return f"Intervals:\n {self.data}"
