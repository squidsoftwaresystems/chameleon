from typing import Dict, List, Optional, Tuple, cast

import pandas as pd

from .constants import INVALID_ID, TruckEvent


class UnoccupiedWindows:
    """
    Represents a collection of non-overlapping intervals of time
    where a specific truck is available
    """

    intervals: pd.DataFrame
    """
    List of intervals in order, with accompanying data.

    Index:
        pd.DatetimeIndex, start time
    Columns:
        Name: end_time,     dtype: pd.Timestamp
        Name: terminal,     dtype: int64,
    """

    def __init__(
        self,
        df: pd.DataFrame,
    ):
        """
        Create a collection of intervals between deliveries based on a dataframe.
        Requires the dataframe to be sorted on its DatetimeIndex
        and all intervals to be "finished", i.e. have an end.

        :param df: dataframe to process
        Index:
            pd.DatetimeIndex
        Columns:
            Name: event_type,   dtype: int64
            Name: terminal,     dtype: int64,       terminal at which the event occurred
        """

        # Find intervals where there is nothing booked, and
        # where the truck needs to go during that time.
        # "unoccupied windows" start once a delivery ends and vice-versa
        self.intervals = UnoccupiedWindows.create_intervals(
            df, TruckEvent.DELIVERY_END, TruckEvent.DELIVERY_START
        )

    @staticmethod
    def create_intervals(
        df: pd.DataFrame,
        start_tag: int,
        end_tag: int,
        assert_same_terminal: bool = True,
    ) -> pd.DataFrame:
        """
        Create a collection of intervals based on a dataframe.
        Requires the dataframe to be sorted on its DatetimeIndex
        and all intervals to be "finished", i.e. have an end.

        :param df: dataframe to process
        Index:
            pd.DatetimeIndex
        Columns:
            Name: event_type,   dtype: int64
            Name: from_terminal,     dtype: int64,       terminal at which the event occurred
            Name: to_terminal,     dtype: int64,       terminal at which the event occurred

        :param start_tag: value in "event_type" column which signifies a start of the interval
        :param end_tag: value in `tag_column` which signifies an end of the interval
        :param tag_column: column in df to use to detect start/end of an interval
        :param assert_same_terminal: check that the terminal at the beginning and end of interval is the same

        :returns: a sorted dataframe of intervals
        :rtype: pd.DataFrame

        Index:
            pd.DatetimeIndex, start time
        Columns:
            Name: end_time,     dtype: pd.Timestamp
            Name: terminal,     dtype: int64, terminal at which the event occurs
        """
        # Time should be sorted
        assert df.index.is_monotonic_increasing

        start_times: List[pd.Timestamp] = []
        end_times: List[pd.Timestamp] = []
        from_terminals: List[int] = []
        to_terminals: List[int] = []

        # Start at the first possible time
        last_time: pd.Timestamp = pd.to_datetime(0, origin="unix")
        # first event starts creating an interval
        last_tag_was_end = True
        last_terminal: int = INVALID_ID

        # Only keep the rows relating to the intervals
        df = df[df["event_type"].isin([start_tag, end_tag])]

        for time, row in df.iterrows():
            time = cast(pd.Timestamp, time)
            terminal = row["terminal"]

            if last_tag_was_end:
                assert row["event_type"] == start_tag
            else:
                assert row["event_type"] == end_tag
                # Create an "unoccupied window"
                start_times.append(last_time)
                end_times.append(time)
                from_terminals.append(last_terminal)
                to_terminals.append(terminal)

                if assert_same_terminal:
                    assert last_terminal == terminal

            last_time = time
            last_terminal = terminal
            last_tag_was_end = not last_tag_was_end

        # Make sure the last interval was closed

        return pd.DataFrame(
            data={
                "end_time": end_times,
                "from_terminal": from_terminals,
                "to_terminal": to_terminals,
            },
            index=start_times,
        )

    # TODO: consider purging empty intervals in operations

    def intersect_on_terminals(self, intervals_by_terminal: Dict[int, pd.DataFrame]):
        """
        Update `self` so that if `self` has an interval `I` at terminal `t`,
        it is replaced by intersection of `I` and (union of `intervals_by_terminal[t]`),
        represented as a list of intervals

        :param intervals_by_terminal: mapping from terminal index to a
        collection of intervals in format of OccupiedWindows.create_intervals
        """

        new_intervals: List[pd.DataFrame] = []

        for index, interval in self.intervals.iterrows():
            start_time: pd.Timestamp = cast(pd.Timestamp, index)
            end_time: pd.Timestamp = interval["end_time"]
            terminal: int = interval["from_terminal"]

            assert terminal is not INVALID_ID
            assert terminal == interval["to_terminal"]

            # Select constraint intervals which occur at a relevant time
            constraint_intervals = intervals_by_terminal[terminal]
            constraint_intervals = constraint_intervals[
                (constraint_intervals["end_time"] >= start_time)
                # index is start_time
                & (constraint_intervals.index <= end_time)
            ].copy()

            # The result will be the overlapping constraint intervals,
            # bounded below by start_time and above by end_time
            if not constraint_intervals.empty:
                # Clamp by start_time below
                if constraint_intervals.index[0] < start_time:
                    constraint_intervals.index[0] = start_time

                # Clamp by end_time above
                end_time_column_index = constraint_intervals.columns.get_loc("end_time")
                if constraint_intervals.iloc[-1, end_time_column_index] > end_time:
                    constraint_intervals.iloc[-1, end_time_column_index] = end_time

                new_intervals.append(constraint_intervals)

        self.intervals = pd.concat(new_intervals)

    def __repr__(self):
        return f"Unoccupied Windows:\n{repr(self.intervals)}"
