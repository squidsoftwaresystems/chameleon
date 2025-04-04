import os
import pickle
from typing import Callable, Dict, List, Tuple, cast

import numpy as np
import numpy.typing as npt
import pandas as pd

from chameleon_rust import Booking, Schedule, ScheduleGenerator
from src.api import SquidAPI

# TODO: collapse 2 consecutive empty transports into 1

Time = int
TimeDelta = int

TerminalID = str
CargoID = str
TruckID = str


def make_schedule_generator(
    terminal_data: pd.DataFrame,
    truck_data: pd.DataFrame,
    requested_transports: pd.DataFrame,
    planning_period: Tuple[pd.Timestamp, pd.Timestamp],
) -> ScheduleGenerator:
    """
    Creates a blank schedule, given dataframes for data

    :param terminal_data: dataframe on terminals
        Index:
            pd.Index, dtype=str: id of the terminal
        Columns:
            Name: opening_time,     dtype: datatime64[ns], in minutes
            Name: closing_time,     dtype: datatime64[ns], in minutes

    :param truck_data: dataframe on trucks
        Index:
            pd.Index, dtype=str: id of the truck
        Columns:
            Name: starting_terminal,     dtype: str      terminal id where
            truck starts at the beginning of the day

    :param requested_transports: dataframe on transports
        Index:
            pd.Index, dtype=str: id of the transports (one leg of the journey)
        Columns:
            Name: cargo,                dtype: str            id of cargo to be transported
            Name: from_terminal,        dtype: str            id of terminal to be transported from
            Name: to_terminal,          dtype: str            id of terminal to be transported to
            Name: pickup_open_time,     dtype: datetime64[ns]   Time from which cargo can be picked up
            Name: pickup_close_time,    dtype: datetime64[ns]  Time before which cargo must be picked up
            Name: dropoff_open_time,    dtype: datetime64[ns]  Time from which cargo can be dropped off
            Name: dropoff_close_time,   dtype: datetime64[ns] Time before which cargo must be dropped off
            Name: driving_time,      dtype: timedelta64[ns]
    :param planning_period an interval during which all the operations need
    to be planned to take place
    """

    def timestamp_to_seconds(timestamp: pd.Timestamp):
        return int(timestamp.timestamp())

    def timedelta_to_seconds(timestamp: pd.Timedelta):
        return int(timestamp.total_seconds())

    # Repack the data into the format used by the bindings
    _terminal_data: Dict[TerminalID, Tuple[Time, Time]] = {
        cast(str, terminal): (
            timestamp_to_seconds(row["opening_time"]),
            timestamp_to_seconds(row["closing_time"]),
        )
        for terminal, row in terminal_data.iterrows()
    }

    _truck_data: Dict[TruckID, TerminalID] = {
        cast(str, truck): row["starting_terminal"]
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

    _planning_period: Tuple[Time, Time] = (
        timestamp_to_seconds(planning_period[0]),
        timestamp_to_seconds(planning_period[1]),
    )

    return ScheduleGenerator(
        _terminal_data, _truck_data, _transpost_data, _planning_period
    )


def __make_schedule_data_from_api(
    api: SquidAPI,
    planning_period: Tuple[pd.Timestamp, pd.Timestamp],
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    Tuple[pd.Timestamp, pd.Timestamp],
]:
    """
    Create a schedule using the API data
    """

    planning_period_start = planning_period[0]
    planning_period_end = planning_period[1]
    assert planning_period_start < planning_period_end

    """LOCATIONS"""
    # NOTE: it looks like none of the locations here are closed at any time at all
    # so we *ignore* opening_time and closing_time
    raw_locations = api.getLocations()
    num_locations = raw_locations.shape[0]
    terminal_data: pd.DataFrame = pd.DataFrame(
        data={
            "opening_time": [planning_period_start] * num_locations,
            "closing_time": [planning_period_end] * num_locations,
        },
        index=raw_locations.index,
    )

    """TRUCKS"""
    raw_trucks = api.getTrucks()
    raw_truck_starts = api.getTruckStarts().rename(
        columns={
            # NOTE: currently, we assume that all trucks can work all day, so
            # this is not considered yet
            # "start_time":
            "location_id": "starting_terminal"
        }
    )

    # Joins both on index by default
    truck_data: pd.DataFrame = raw_trucks.join(raw_truck_starts, how="inner")

    """requested_transports"""
    raw_bookings = api.getBookings()

    # Amend the bookings to replace the pickup and dropoff interval endpoints
    # which are null with numbers

    # We chose to represent timestamps as unsigned integers,
    # so we can't use pd.Timestamp.min, which is represented by a negative
    # number of seconds
    min_timestamp = pd.Timestamp(pd.to_datetime(0, origin="unix", utc=True))
    max_timestamp = pd.Timestamp.max.tz_localize("UTC")
    raw_bookings.fillna(
        {
            "cargo_opening": min_timestamp,
            "cargo_closing": max_timestamp,
            "first_pickup": min_timestamp,
            "last_pickup": max_timestamp,
        },
        inplace=True,
    )

    # Convert to pd.Timestamp
    column_names = [
        "cargo_opening",
        "cargo_closing",
        "first_pickup",
        "last_pickup",
    ]
    # Convert to UTC, timezone-naive time
    raw_bookings[column_names] = raw_bookings[column_names].map(pd.to_datetime)

    assert (min_timestamp <= raw_bookings[column_names]).all().all()

    # Remove invalid rows
    raw_bookings = raw_bookings[
        (raw_bookings["cargo_opening"] < raw_bookings["cargo_closing"])
        & (raw_bookings["first_pickup"] < raw_bookings["last_pickup"])
    ]

    # Change the format of bookings and only add ones that
    # have corresponding routes.
    # For now, for the sake of simplicity,
    # we consider the task to be going from the very first
    # waypoint straight to the very last one.
    bookings = []
    for booking_id, booking in raw_bookings.iterrows():
        routes = api.getRoutesForBooking(booking_id)
        if routes.empty:
            continue
        # TODO: ignore the deliveries to non-port areas

        # We will have issues if we are asked to deliver from a location to itself
        assert routes.shape[0] > 1

        if pd.isna(booking["container_id"]):
            continue

        bookings.append(
            {
                "transport_id": booking_id,
                "cargo": booking["container_id"],
                "pickup_open_time": booking["first_pickup"],
                "pickup_close_time": booking["last_pickup"],
                "dropoff_open_time": booking["cargo_opening"],
                "dropoff_close_time": booking["cargo_closing"],
                "from_terminal": routes.iloc[-1]["location_id"],
                "to_terminal": routes.iloc[0]["location_id"],
                "driving_time": pd.to_timedelta(
                    10000, unit="m"
                ),  # TODO: remove this from here,
                # calculate it differently
            }
        )

    requested_transports = pd.DataFrame(bookings).set_index("transport_id")

    # TODO: add more driving times, potentially by passing in a callback
    # for calculating driving times

    return (terminal_data, truck_data, requested_transports, planning_period)


def invalidate_schedule_data_cache(
    pkl_filepath: str = "data/schedule_data.pkl",
) -> bool:
    """
    Try invalidate the cache, return whether we were successful or not
    """
    try:
        os.remove(pkl_filepath)
        return True
    except OSError:
        return False


def cached_make_schedule_generator_from_api(
    api: SquidAPI,
    planning_period: Tuple[pd.Timestamp, pd.Timestamp],
    pkl_filepath: str = "data/schedule_data.pkl",
) -> ScheduleGenerator:
    """
    Try to load a cache and make a schedule generator from that
    If not successful, instead load data from API, cache it, and
    return a schedule generator based on that
    """

    def refetch():
        # Could not load data, need to re-compute and cache
        data = __make_schedule_data_from_api(api, planning_period)
        with open(pkl_filepath, "wb") as f:
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
        return data

    try:
        with open(pkl_filepath, "rb") as f:
            data = pickle.load(f)

        # Check if the planning period is the same
        if data[3] != planning_period:
            invalidate_schedule_data_cache(pkl_filepath)
            data = refetch()
    except OSError:
        data = refetch()

    return make_schedule_generator(*data)


def get_scores_calculator(
    schedule_generator: ScheduleGenerator,
) -> Callable[[Schedule], npt.NDArray]:
    def scores_calculator(schedule: Schedule) -> npt.NDArray:
        return np.array(schedule_generator.scores(schedule))

    return scores_calculator
