from abc import ABC
from typing import Dict, List, Tuple, cast

import numpy as np
import pandas as pd

from .constants import INVALID_ID, FixedEvent, TruckEvent
from .unoccupied_windows import UnoccupiedWindows


class TruckScheduleChange(ABC):
    """
    An abstract class representing a change in schedule of a terminal,
    from a point of view of a truck.

    Instances of this class are stored together with the truck,
    so don't contain a reference to the truck inside the class.
    """

    pass


class AddTransition(TruckScheduleChange):
    """
    A class representing addition of a transition to a schedule
    """

    from_terminal: int
    """
    Id of a terminal from which the truck departs
    """

    to_terminal: int
    """
    Id of a terminal to which the truck arrives
    """

    time_departure: pd.Timestamp
    """
    Timestamp of when truck leaves
    """

    time_arrival: pd.Timestamp
    """
    Timestamp of when truck arrives
    """

    cargo: int
    """
    A possibly INVALID_ID id of a cargo to be transported
    """


class RemoveTransitions(TruckScheduleChange):
    """
    A class representing deletion of all transitions of a truck after some point
    """

    pass


class RescheduleTransition(TruckScheduleChange):
    """
    A class representing changing the time of a transition
    """

    pass


class Schedule:
    """
    A class representing a timetable for deliveries which
    doesn't violate hard constraints
    """

    fixed_events: pd.DataFrame
    """
    A dataframe containing all events that can't be changed
    Index:
        pd.DatetimeIndex
    Columns:
        Name: event_type,   dtype: int64
        Name: terminal,     dtype: int64
        Name: cargo,        dtype: int64,
        Name: hidden,       dtype: boolean

    Note that terminal, cargo, event_type refer to respective ids

    If an event is hidden, it means that it is accounted for and can be safely ignored when planning routes.
    For example, if a cargo is delivered, its pickup and dropoff events can be ignored when planning other routes.

    Invariant: cargo can't be delivered to terminal it has been to before
    """

    truck_events: Dict[int, pd.DataFrame]
    """
    A dict mapping terminal id to a dataframe containing all events that happen to the truck
    Index:
        pd.DatetimeIndex
    Columns:
        Name: event_type,   dtype: int64
        Name: terminal,     dtype: int64,
        Name: cargo,        dtype: int64,

    Note that terminal, truck, cargo, event_type refer to respective ids

    Invariant: cargo can't be delivered to terminal it has been to before
    """

    unoccupied_windows: Dict[int, UnoccupiedWindows]
    """
    A mapping of truck ids to the set of intervals at which they are idle
    Note that trucks corresponding to uncached_trucks may
    contain stale modification lists, and need to be updated
    before using them.
    """

    possible_changes: Dict[int, List[TruckScheduleChange]]
    """
    A map of trucks to modifications that can be made to their schedule.
    Note that trucks corresponding to uncached_trucks may
    contain stale modification lists, and need to be updated
    before using them.
    """

    # TODO: does it make sense to cache on both truck and terminal?
    uncached_trucks: List[int]
    """
    A list of trucks for which possible_changes and unoccupied_windows might be stale
    """

    # TODO: allow multiple legs of transport per cargo
    # NOTE: assumes that each piece of cargo is shipped to a
    # specific terminal at most once
    transports: pd.DataFrame
    """
    A list of pieces of cargo that need to be transported
    """

    terminal_open_intervals: Dict[int, pd.DataFrame]
    """
    A dict mapping terminal ids to Dataframe of intervals in format of 
    OccupiedWindows.create_intervals, describing when each terminal is open
    """

    def __init__(
        self, terminals: pd.DataFrame, trucks: pd.DataFrame, transports: pd.DataFrame
    ):
        """
        Creates a blank schedule, given dataframes for data

        :param terminals: dataframe on terminals
            Index:
                pd.Index, dtype=int64: id of the terminal
            Columns:
                Name: opening_time,     dtype: datatime64[ns], in minutes
                Name: closing_time,     dtype: datatime64[ns], in minutes

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

        self.transports = transports

        # Record the data corresponding to each terminal in the same order
        fixed_event_timestamps: List[pd.Timestamp] = []
        fixed_event_event_types: List[int] = []
        fixed_event_terminals: List[int] = []
        fixed_event_cargo: List[int] = []
        fixed_event_hidden: List[bool] = []

        truck_timestamps: Dict[int, List[pd.Timestamp]] = {}
        truck_event_types: Dict[int, List[int]] = {}
        truck_terminal: Dict[int, List[int]] = {}
        truck_cargo: Dict[int, List[int]] = {}

        def add_fixed_event(
            terminal: int,
            timestamp: pd.Timestamp,
            event_type: int,
            cargo: int = INVALID_ID,
            hidden: bool = False,
        ):
            fixed_event_timestamps.append(timestamp)
            fixed_event_event_types.append(event_type)
            fixed_event_terminals.append(terminal)
            fixed_event_cargo.append(cargo)
            fixed_event_hidden.append(hidden)

        def add_truck_event(
            truck: int,
            timestamp: pd.Timestamp,
            event_type: int,
            terminal: int,
            cargo: int = INVALID_ID,
        ):
            truck_timestamps[truck].append(timestamp)
            truck_event_types[truck].append(event_type)
            truck_terminal[truck].append(terminal)
            truck_cargo[truck].append(cargo)

        # Add terminal opening and closing times
        for terminal, row in terminals.iterrows():
            terminal = cast(int, terminal)

            add_fixed_event(terminal, row["opening_time"], FixedEvent.TERMINAL_OPEN)
            add_fixed_event(terminal, row["closing_time"], FixedEvent.TERMINAL_CLOSE)

        self.possible_changes = {}
        self.unoccupied_windows = {}
        self.uncached_trucks = []
        # Specify where trucks start their working days
        for truck, row in trucks.iterrows():
            # TODO: find a way to consider each transport once among all trucks,
            # despite different driver working times.
            truck = cast(int, truck)

            truck_timestamps[truck] = []
            truck_event_types[truck] = []
            truck_terminal[truck] = []
            truck_cargo[truck] = []

            self.uncached_trucks.append(truck)

            # Add a dummy event to specify starting location
            starting_terminal: int = row["starting_terminal"]
            opening_time = cast(
                pd.Timestamp, terminals.loc[starting_terminal, "opening_time"]
            )

            # Trucks become available at terminal at this time
            add_truck_event(
                truck, opening_time, TruckEvent.DELIVERY_END, starting_terminal
            )
            # TODO: put a more sane upper bound on when deliveries can finish

            # TODO: allow changing last terminal when adding new route,
            # since we don't care about where truck ends up
            add_truck_event(
                truck,
                opening_time + pd.to_timedelta(24, unit="h"),
                TruckEvent.DELIVERY_START,
                starting_terminal,
            )

        # When can cargo be moved?
        for transport, row in transports.iterrows():
            transport = cast(int, transport)

            # Add a dummy event to specify starting location
            cargo: int = row["cargo"]
            from_terminal: int = row["from_terminal"]
            to_terminal: int = row["to_terminal"]
            pickup_open_time: pd.Timestamp = row["pickup_open_time"]
            pickup_close_time: pd.Timestamp = row["pickup_close_time"]
            dropoff_open_time: pd.Timestamp = row["dropoff_open_time"]
            dropoff_close_time: pd.Timestamp = row["dropoff_close_time"]

            # Add events for pickup and dropoff slots
            add_fixed_event(
                from_terminal,
                pickup_open_time,
                FixedEvent.PICKUP_OPEN,
                cargo,
            )
            add_fixed_event(
                from_terminal,
                pickup_close_time,
                FixedEvent.PICKUP_CLOSE,
                cargo,
            )
            add_fixed_event(
                to_terminal,
                dropoff_open_time,
                FixedEvent.DROPOFF_OPEN,
                cargo,
            )
            add_fixed_event(
                to_terminal,
                dropoff_close_time,
                FixedEvent.DROPOFF_CLOSE,
                cargo,
            )

        # Create the dataframes, sort them by timestamp
        self.fixed_events = pd.DataFrame(
            data={
                "event_type": fixed_event_event_types,
                "terminal": fixed_event_terminals,
                "cargo": fixed_event_cargo,
                "hidden": fixed_event_hidden,
            },
            index=fixed_event_timestamps,
        ).sort_index()

        self.truck_events = {}
        for truck, _ in trucks.iterrows():
            truck = cast(int, truck)
            df = pd.DataFrame(
                data={
                    "event_type": truck_event_types[truck],
                    "terminal": truck_terminal[truck],
                    "cargo": truck_cargo[truck],
                },
                index=truck_timestamps[truck],
            ).sort_index()

            self.truck_events[truck] = df

        # For each terminal, store its opening times
        self.terminal_open_intervals = {}
        for terminal, row in terminals.iterrows():
            terminal = cast(int, terminal)
            self.terminal_open_intervals[terminal] = UnoccupiedWindows.create_intervals(
                self.fixed_events[self.fixed_events["terminal"] == terminal],
                FixedEvent.TERMINAL_OPEN,
                FixedEvent.TERMINAL_CLOSE,
            )

        self.recalculate_possible_changes()

    def recalculate_possible_changes(self) -> None:
        for truck in self.uncached_trucks:
            assert truck is not INVALID_ID

            self.possible_changes[truck] = []

            truck_events = self.truck_events[truck]

            # Find intervals between deliveries
            unoccupied_windows = UnoccupiedWindows(truck_events)

            # Enforce that deliveries can occur while terminal is working
            unoccupied_windows.intersect_on_terminals(self.terminal_open_intervals)

            self.unoccupied_windows[truck] = unoccupied_windows

    # TODO: also take expired deliveries into account when
    # evaluating a schedule
    def get_number_of_deliveries(self):
        """
        Returns number of deliveries in the schedule

        :returns: the number of cargo items delivered under this schedule
        :rtype: int
        """
        pass

    def __repr__(self):
        out = "Schedule:\n\nTerminals:\n\n"

        for terminal in np.sort(self.fixed_events["terminal"].unique()):
            out += "-------\n"
            out += f"Terminal {terminal}:\nEvents:\n"
            events = self.fixed_events[self.fixed_events["terminal"] == terminal].copy()

            # Replace event type numbers with enum names
            events["event_type"] = events["event_type"].apply(
                lambda x: FixedEvent(x).name
            )
            out += repr(events) + "\n"
            out += "Opening times:\n"
            out += repr(self.terminal_open_intervals[terminal]) + "\n\n"

        out += "Truck events:\n\n"

        for truck, events in self.truck_events.items():
            out += "-------\n"
            out += f"Truck {truck}:\n\n"
            # Replace event type numbers with enum names
            events = events.copy()
            events["event_type"] = events["event_type"].apply(
                lambda x: TruckEvent(x).name
            )
            out += repr(events) + "\n"

        out += "Truck unoccupied windows:\n\n"
        for truck, events in self.truck_events.items():
            out += "-------\n"
            out += f"Truck {truck}:\n"
            out += repr(self.unoccupied_windows[truck]) + "\n\n"

        return out
