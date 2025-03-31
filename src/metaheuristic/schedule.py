from typing import Dict, List, Tuple, cast

import pandas as pd

import chameleon_rust
from chameleon_rust import Booking

# TODO: collapse 2 consecutive empty transports into 1

Time = int
TimeDelta = int

Terminal = int
Cargo = int
Truck = int


def make_schedule_generator(
    terminal_data: pd.DataFrame,
    truck_data: pd.DataFrame,
    requested_transports: pd.DataFrame,
) -> chameleon_rust.ScheduleGenerator:
    """
    Creates a blank schedule, given dataframes for data

    :param terminal_data: dataframe on terminals
        Index:
            pd.Index, dtype=int64: id of the terminal
        Columns:
            Name: opening_time,     dtype: datatime64[ns], in minutes
            Name: closing_time,     dtype: datatime64[ns], in minutes

    :param truck_data: dataframe on trucks
        Index:
            pd.Index, dtype=int64: id of the truck
        Columns:
            Name: starting_terminal,     dtype: int64      terminal where
            truck starts at the beginning of the day

    :param requested_transports: dataframe on transports
        Index:
            pd.Index, dtype=int64: id of the transports (one leg of the journey)
        Columns:
            Name: cargo,                dtype: int64            id of cargo to be transported
            Name: from_terminal,        dtype: int64            id of terminal to be transported from
            Name: to_terminal,          dtype: int64            id of terminal to be transported to
            Name: pickup_open_time,     dtype: datetime64[ns]   Time from which cargo can be picked up
            Name: pickup_close_time,    dtype: datetime64[ns]  Time before which cargo must be picked up
            Name: dropoff_open_time,    dtype: datetime64[ns]  Time from which cargo can be dropped off
            Name: dropoff_close_time,   dtype: datetime64[ns] Time before which cargo must be dropped off
            Name: driving_time,      dtype: timedelta64[ns]
    """

    def timestamp_to_seconds(timestamp: pd.Timestamp):
        return int(timestamp.timestamp())

    def timedelta_to_seconds(timestamp: pd.Timedelta):
        return int(timestamp.total_seconds())

    # Repack the data into the format used by the bindings
    _terminal_data: Dict[Terminal, Tuple[Time, Time]] = {
        cast(int, terminal): (
            timestamp_to_seconds(row["opening_time"]),
            timestamp_to_seconds(row["closing_time"]),
        )
        for terminal, row in terminal_data.iterrows()
    }

    _truck_data: Dict[Truck, Terminal] = {
        cast(int, truck): row["starting_terminal"]
        for truck, row in truck_data.iterrows()
    }

    _transpost_data: List[Booking] = [
        Booking(
            cargo=row["cargo"],
            from_terminal=row["from_terminal"],
            to_terminal=row["to_terminal"],
            pickup_open_time=timestamp_to_seconds(row["pickup_open_time"]),
            pickup_close_time=timestamp_to_seconds(row["pickup_close_time"]),
            dropoff_open_time=timestamp_to_seconds(row["dropoff_open_time"]),
            dropoff_close_time=timestamp_to_seconds(row["dropoff_close_time"]),
            direct_driving_time=timedelta_to_seconds(row["driving_time"]),
        )
        for transport_id, row in requested_transports.iterrows()
    ]

    # The first time when we can do anything is when
    # cargo becomes available and a terminal opens
    start_time = max(
        terminal_data["opening_time"].min(),
        requested_transports["pickup_open_time"].min(),
    )

    # We can end once all terminals and dropoffs close
    end_time = min(
        terminal_data["closing_time"].max(),
        requested_transports["dropoff_close_time"].max(),
    )

    # TODO: a more intelligent calculation of planning period
    _planning_period: Tuple[Time, Time] = (
        timestamp_to_seconds(start_time),
        timestamp_to_seconds(end_time),
    )

    return chameleon_rust.ScheduleGenerator(
        _terminal_data, _truck_data, _transpost_data, _planning_period
    )
