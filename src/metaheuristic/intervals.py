from collections.abc import Iterator
from typing import Any, Callable, Hashable, List, Optional, Self, Tuple, cast

import pandas as pd

ListForInterval = List[Tuple[pd.Timestamp, pd.Timestamp, Tuple]]


# TODO: might even relax the restriction that it is sorted
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
    def from_row(cls, data: pd.Series) -> Self:
        """
        Create an Intervals object for single interval data
        """

        out = cls()
        out.data = data.to_frame().T

        Intervals.__sort_and_assert_valid(out.data)

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

        # Reformat data and filter out 0-width intervals
        squashed_data = [
            [start_time, end_time] + list(rest)
            for start_time, end_time, rest in data
        ]

        intervals: pd.DataFrame = pd.DataFrame(
            squashed_data,
            columns=(["start_time", "end_time"] + column_names),
        )

        Intervals.__sort_and_assert_valid(intervals)

        out = cls()
        out.data = intervals
        return out

    @staticmethod
    def __sort_and_assert_valid(data: pd.DataFrame):
        """
        Given data, checks if it is valid data for `Intervals`
        """

        assert "start_time" in data.columns
        assert "end_time" in data.columns

        # Check types, if there are any rows
        if data.shape[0] > 0:
            # TODO: use a more robust check, to check all values in columns
            # or find a way to properly check column type
            assert type(data["start_time"].iloc[0]) is pd.Timestamp
            assert type(data["end_time"].iloc[0]) is pd.Timestamp

        # If the values are sorted by start_time,
        # it makes it easier to check e.g. that the intervals are
        # pairwise not overlapping, by going through
        # the intervals in order
        data.sort_values("start_time", inplace=True)

        # Assert intervals have a positive length
        assert (data["start_time"] < data["end_time"]).all()

        # Check that intervals non-overlapping
        for _, group in data.groupby(
            list(data.columns.drop(["start_time", "end_time"]))
        ):
            interval1 = group.iloc[0]
            for _, interval2 in group.iloc[1:].iterrows():
                assert (interval1["end_time"] <= interval2["start_time"]) or (
                    interval2["end_time"] <= interval1["start_time"]
                )
                interval1 = interval2

    def concat(
        self,
        other: Self,
    ) -> Self:
        """
        Returns a copy of `self` which is concatenation of `self` and `other`
        """
        out = type(self)()

        # Don't concat if one of them is empty
        if self.data.shape[0] == 0:
            out.data = other.data
        elif other.data.shape[0] == 0:
            out.data = self.data
        else:
            out.data = pd.concat([self.data, other.data], ignore_index=True)

        Intervals.__sort_and_assert_valid(out.data)

        return out

    def filter_predicate(self, pred: Callable[[pd.Series], bool]) -> Self:
        """
        Return a copy of `self` consisting of exactly the intervals
        that `pred` returns `True` on
        """
        out = type(self)()
        out.data = self.data[self.data.apply(pred, axis=1)]
        return out

    def filter_column(self, col: str, val: Any) -> Self:
        """
        Return a copy of `self` consisting of exactly the intervals
        that have value `val` in column `col`
        """
        out = type(self)()
        out.data = self.data[self.data[col] == val]
        return out

    def extract_interval(
        self, start_time: pd.Timestamp, end_time: pd.Timestamp
    ) -> pd.Series:
        """
        Find and delete interval containing [start_time, end_time].
        The interval is returned. If the interval doesn't exist or is
        not unique, raise an error

        :param start_time: start time of interval to look for
        :param end_time: end time of interval to look for
        :raises: ValueError if 0 or more than 1 intervals match
        """
        matching_intervals = self.data[
            (self.data["start_time"] <= start_time)
            & (end_time <= self.data["end_time"])
        ]
        num_mathces = matching_intervals.shape[0]
        if num_mathces != 1:
            raise ValueError(
                f"Expected exactly one interval to match, got {num_mathces}"
            )

        # Delete the row
        self.data.drop(matching_intervals.index[0], inplace=True)
        return matching_intervals.iloc[0]

    def shift_by(
        self,
        by: Callable[[pd.Series], pd.Timedelta],
    ) -> Self:
        """
        Returns a copy of `self`, with each interval
        shifted by `by(interval)`. Note that `by` has
        some restrictions:

        :param by: A mapping of intervals (rows) to shifts. `by` doesn't
        receive `start_time` and `end_time` and has to be a pure function -
        otherwise, we can't guarantee that the intervals aren't overlapping
        for each additional data
        """

        def shift(row: pd.Series):
            row[["start_time", "end_time"]] += by(
                row.drop(["start_time", "end_time"])
            )
            return row

        new_data = self.data.apply(shift, axis=1)
        out = type(self)()
        out.data = new_data
        return out

    def limit_time(
        self,
        start_time: Optional[pd.Timestamp],
        end_time: Optional[pd.Timestamp],
    ) -> Self:
        """
        Return a copy of `self` with all intervals "trimmed"
        to remove everything that occurs before start_time or after end_time
        """

        def trim(row: pd.Series):
            if start_time is not None:
                row["start_time"] = max(start_time, row["start_time"])
            if end_time is not None:
                row["end_time"] = min(end_time, row["end_time"])
            return row

        data = self.data.apply(trim, axis=1)
        # Remove invalid intervals
        data = data[data["start_time"] < data["end_time"]]
        out = type(self)()
        out.data = data
        return out

    def earliest(self) -> Optional[pd.Timestamp]:
        """
        Return earliest time captured by the intervals
        """
        if self.data.empty:
            return None
        return self.data["start_time"].min()

    def latest(self) -> Optional[pd.Timestamp]:
        """
        Return latest time captured by the intervals
        """
        if self.data.empty:
            return None
        return self.data["end_time"].max()

    def intersect_on_column(
        self,
        other: Self | pd.Series,
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

        :param other: `Intervals` to intersect on
        :param self_cols_to_keep: names of columns to keep from self's data
        :param other_cols_to_keep: names of columns to keep from other's data
        """

        new_intervals: List[pd.DataFrame] = []

        if type(other) is pd.Series:
            other = cast(Self, Intervals.from_row(other))

        for _, interval in self.data.iterrows():
            start_time: pd.Timestamp = interval["start_time"]
            end_time: pd.Timestamp = interval["end_time"]
            data: Any = interval[self_col]

            # Select constraint intervals which intersect interval
            # and have relevant data
            constraint_intervals: pd.DataFrame = other.data[
                (other.data[other_col] == data)
                & (other.data["end_time"] >= start_time)
                # index is start_time
                & (other.data["start_time"] <= end_time)
                # Then select only the columns we want
            ][["start_time", "end_time"] + other_cols_to_keep].copy()

            # Add values from self_cols_to_keep
            constraint_intervals = constraint_intervals.assign(
                **{col: interval[col] for col in self_cols_to_keep}
            )

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
        if len(new_intervals) > 0:
            out.data = pd.concat(new_intervals)
        else:
            out.data = pd.DataFrame(
                {
                    col: []
                    for col in ["start_time", "end_time"]
                    + other_cols_to_keep
                    + self_cols_to_keep
                }
            )
        return out

    def copy(self) -> Self:
        out = type(self)()
        out.data = self.data.copy()
        return out

    def __repr__(self):
        return repr(self.data)

    def __iter__(self) -> Iterator[tuple[Hashable, pd.Series]]:
        return self.data.iterrows().__iter__()
