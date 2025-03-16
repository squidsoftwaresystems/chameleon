from abc import ABC
from enum import IntEnum
from typing import Dict, List, Optional, assert_type, cast

import pandas as pd

INVALID_ID = -1


class Event(IntEnum):
    TERMINAL_OPEN = 0
    TERMINAL_CLOSE = 1
    PICKUP_OPEN = 2
    PICKUP_CLOSE = 3
    DROPOFF_OPEN = 4
    DROPOFF_CLOSE = 5
    DELIVERY_END = 6
    DELIVERY_START = 7


class TerminalScheduleModification(ABC):
    """
    A class representing a change in schedule of a terminal.

    Instances of this class are stored together with from_terminal,
    and define a single change in the schedule of a terminal
    """

    pass


class Schedule:
    """
    A class representing a timetable for deliveries which
    doesn't violate hard constraints
    """

    terminal_events: Dict[int, pd.DataFrame]
    """
    A dict mapping terminal id to a dataframe containing all events that happen to the terminal
    Index:
        pandas.DatetimeIndex
    Columns:
        Name: event_type,   dtype: int64
        Name: truck,        dtype: int64,           nullable: true
        Name: cargo,        dtype: int64,           nullable: true
        Name: hidden,       dtype: boolean

    Note that terminal, truck, cargo, event_type refer to respective ids

    If an event is hidden, it means that it is accounted for and can be safely ignored when planning routes.
    For example, if a cargo is delivered, its pickup and dropoff events can be ignored when planning other routes.

    Invariant: cargo can't be delivered to terminal it has been to before
    """

    possible_modifications: Dict[int, List[TerminalScheduleModification]]

    uncached_terminals: List[int]
    """
    A list of terminals for which the cached 
    """

    # TODO: allow multiple legs of transport per cargo
    def __init__(
        self, terminals: pd.DataFrame, trucks: pd.DataFrame, transports: pd.DataFrame
    ):
        """
        Creates a blank schedule, given dataframes for data

        :param terminals: dataframe on terminals
            Index:
                pd.Index, dtype=int64: id of the terminal
            Columns:
                Name: opening_time,     dtype: datatime64[ns]
                Name: closing_time,     dtype: datatime64[ns]

        :param trucks: dataframe on trucks
            Index:
                pd.Index, dtype=int64: id of the truck
            Columns:
                Name: starting_terminal,     dtype: int64      terminal where truck starts at the beginning of the day

        :param transports: dataframe on transports
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
                Name: travel_duration,      dtype: timedelta64[ns]
        """
        # Record the data corresponding to each terminal in the same order
        _timestamps: Dict[int, List[int]] = {}
        _event_types: Dict[int, List[int]] = {}
        _trucks: Dict[int, List[Optional[int]]] = {}
        _cargo: Dict[int, List[Optional[int]]] = {}
        _hidden: Dict[int, List[bool]] = {}

        def add_event(
            terminal: int,
            timestamp: int,
            event_type: int,
            truck: Optional[int] = INVALID_ID,
            cargo: Optional[int] = INVALID_ID,
            hidden: bool = False,
        ):
            _timestamps[terminal].append(timestamp)
            _event_types[terminal].append(event_type)
            _trucks[terminal].append(truck)
            _cargo[terminal].append(cargo)
            _hidden[terminal].append(hidden)

        # Add terminal opening and closing times
        for terminal, row in terminals.iterrows():
            terminal = cast(int, terminal)

            # Create lists for this terminal
            _timestamps[terminal] = []
            _event_types[terminal] = []
            _trucks[terminal] = []
            _cargo[terminal] = []
            _hidden[terminal] = []

            add_event(terminal, row["opening_time"], Event.TERMINAL_OPEN)
            add_event(terminal, row["closing_time"], Event.TERMINAL_CLOSE)

        # Specify where trucks start their working days
        for truck, row in trucks.iterrows():
            truck = cast(int, truck)

            # Add a dummy event to specify starting location
            starting_terminal: int = row["starting_terminal"]
            opening_time = cast(int, terminals.loc[starting_terminal, "opening_time"])

            # Trucks become available at terminal at this time
            add_event(starting_terminal, opening_time, Event.DELIVERY_END, truck)

        # When can cargo be moved?
        for transport, row in transports.iterrows():
            transport = cast(int, transport)

            # Add a dummy event to specify starting location
            cargo: int = row["cargo"]
            from_terminal: int = row["from_terminal"]
            to_terminal: int = row["to_terminal"]
            pickup_open_time: int = row["pickup_open_time"]
            pickup_close_time: int = row["pickup_close_time"]
            dropoff_open_time: int = row["dropoff_open_time"]
            dropoff_close_time: int = row["dropoff_close_time"]

            add_event(
                from_terminal, pickup_open_time, Event.PICKUP_OPEN, INVALID_ID, cargo
            )
            add_event(
                from_terminal, pickup_close_time, Event.PICKUP_CLOSE, INVALID_ID, cargo
            )
            add_event(
                to_terminal, dropoff_open_time, Event.DROPOFF_OPEN, INVALID_ID, cargo
            )
            add_event(
                to_terminal, dropoff_close_time, Event.DROPOFF_CLOSE, INVALID_ID, cargo
            )

        # Create the dataframes, sort them by timestamp
        self.terminal_events = {}
        for terminal, _ in terminals.iterrows():
            terminal = cast(int, terminal)
            df = pd.DataFrame(
                data={
                    "event_type": _event_types[terminal],
                    "truck": _trucks[terminal],
                    "cargo": _cargo[terminal],
                    "hidden": _hidden[terminal],
                },
                index=_timestamps[terminal],
            )

            self.terminal_events[terminal] = df.sort_index()

    def get_number_of_deliveries(self):
        """
        Returns number of deliveries in the schedule

        :returns: the number of cargo items delivered under this schedule
        :rtype: int
        """
        pass

    def __repr__(self):
        out = "Schedule:\n\nEvents:\n\n"

        for terminal, events in self.terminal_events.items():
            out += "-------\n"
            out += f"Terminal {terminal}:\n"
            # Replace event type numbers with enum names
            events = events.copy()
            events["event_type"] = events["event_type"].apply(lambda x: Event(x).name)
            out += repr(events) + "\n\n"

        return out
