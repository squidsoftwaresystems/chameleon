import warnings
from abc import ABC
from typing import Dict, List, Tuple, cast

import pandas as pd

from .constants import INVALID_ID, TruckEvent
from .intervals import Intervals, ListForInterval


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


# TODO: collapse 2 consecutive empty transports into 1


class Schedule:
    """
    A class representing a timetable for deliveries which
    doesn't violate hard constraints
    """

    truck_events: Dict[int, pd.DataFrame]
    """
    A dict mapping terminal id to a dataframe containing all events that
    happen to the truck

    Index:
        pd.DatetimeIndex
    Columns:
        Name: event_type,   dtype: int64
        Name: terminal,     dtype: int64,
        Name: cargo,        dtype: int64,

    Note that terminal, truck, cargo, event_type refer to respective ids

    Invariant: cargo can't be delivered to terminal it has been to before
    """

    # unoccupied_windows: Dict[int, UnoccupiedWindows]
    # """
    # A mapping of truck ids to the set of intervals at which they are idle
    # Note that trucks corresponding to uncached_trucks may
    # contain stale modification lists, and need to be updated
    # before using them.
    # """
    #
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
    A list of trucks for which possible_changes
    might be stale
    """

    # TODO: allow multiple legs of transport per cargo
    # NOTE: assumes that each piece of cargo is shipped to a
    # specific terminal at most once
    transports: pd.DataFrame
    """
    A list of pieces of cargo that need to be transported
    """

    time_to_terminal: Dict[Tuple[int, int], pd.Timedelta]
    """
    A map going from (from_terminal, to_terminal) to how long
    it takes to drive between the two terminals
    """

    terminal_open_intervals: Intervals
    """
    Intervals of terminal opining times, keeping track of terminals
    """

    terminal_cargo_pickup_intervals: Intervals
    """
    Intervals of cargo pickup, keeping track of terminals, cargo and driving_time
    """

    terminal_cargo_dropoff_intervals: Intervals
    """
    Intervals of cargo dropoff, keeping track of terminals and cargo
    """

    direct_delivery_start_intervals: Intervals
    """
    List of intervals during which a direct delivery can start, based on
    satisfying constraints such as pickup and dropoff window and travel time.
    Keeps track of cargo, from_terminal, to_terminal, driving_time.
    """

    driving_times: Dict[Tuple[int, int], pd.Timedelta]
    """
    A mapping of (from_terminal, to_terminal) to length of time needed to drive
    between them.
    """

    def __init__(
        self,
        terminal_data: pd.DataFrame,
        truck_data: pd.DataFrame,
        transport_data: pd.DataFrame,
    ):
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

        :param transport_data: dataframe on transports
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

        self.transports = transport_data

        truck_timestamps: Dict[int, List[pd.Timestamp]] = {}
        truck_event_types: Dict[int, List[int]] = {}
        truck_terminal: Dict[int, List[int]] = {}
        truck_cargo: Dict[int, List[int]] = {}

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
        terminal_open_intervals = [
            (row["opening_time"], row["closing_time"], [terminal])
            for terminal, row in terminal_data.iterrows()
        ]
        self.terminal_open_intervals = Intervals.from_list(
            terminal_open_intervals, column_names=["terminal"]
        )

        # Specify where trucks start their working days
        self.possible_changes = {}
        self.uncached_trucks = []
        for truck, row in truck_data.iterrows():
            # TODO: find a way to consider each transport once
            # among all trucks, despite different driver working times.
            truck = cast(int, truck)

            truck_timestamps[truck] = []
            truck_event_types[truck] = []
            truck_terminal[truck] = []
            truck_cargo[truck] = []

            self.uncached_trucks.append(truck)

            # Add a dummy event to specify starting location
            starting_terminal: int = row["starting_terminal"]
            opening_time = cast(
                pd.Timestamp,
                terminal_data.loc[starting_terminal, "opening_time"],
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

        self.truck_events = {}
        for truck, _ in truck_data.iterrows():
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

        # Map of terminals to list of pickup slots in form
        # (start_time, end_time, cargo_id)
        terminal_pickups: ListForInterval = []
        terminal_dropoffs: ListForInterval = []

        driving_times: Dict[Tuple[int, int], pd.Timedelta] = {}
        # How long does it take to drive from "from_terminal" to "to_terminal"
        # for this cargo?
        cargo_direct_delivery_times: Dict[int, pd.Timedelta] = {}
        # When can cargo be moved?
        for transport, row in transport_data.iterrows():
            transport = cast(int, transport)

            # Add a dummy event to specify starting location
            cargo: int = row["cargo"]
            from_terminal: int = row["from_terminal"]
            to_terminal: int = row["to_terminal"]
            pickup_open_time: pd.Timestamp = row["pickup_open_time"]
            pickup_close_time: pd.Timestamp = row["pickup_close_time"]
            dropoff_open_time: pd.Timestamp = row["dropoff_open_time"]
            dropoff_close_time: pd.Timestamp = row["dropoff_close_time"]
            travel_duration: pd.Timedelta = row["travel_duration"]

            driving_times[(from_terminal, to_terminal)] = travel_duration
            # Assume same length in both directions
            driving_times[(to_terminal, from_terminal)] = travel_duration

            cargo_direct_delivery_times[cargo] = travel_duration

            # Add events for pickup and dropoff slots
            terminal_pickups.append(
                (
                    pickup_open_time,
                    pickup_close_time,
                    [from_terminal, cargo, travel_duration],
                )
            )
            terminal_dropoffs.append(
                (dropoff_open_time, dropoff_close_time, [to_terminal, cargo])
            )
        # Create the intervals
        self.terminal_cargo_pickup_intervals = Intervals.from_list(
            terminal_pickups,
            column_names=["from_terminal", "cargo", "driving_time"],
        )
        self.terminal_cargo_dropoff_intervals = Intervals.from_list(
            terminal_dropoffs, column_names=["to_terminal", "cargo"]
        )

        # Restrict them to only work during terminal opening times
        self.terminal_cargo_pickup_intervals = (
            self.terminal_cargo_pickup_intervals.intersect_on_column(
                self.terminal_open_intervals,
                self_col="from_terminal",
                other_col="terminal",
                self_cols_to_keep=["from_terminal", "cargo", "driving_time"],
                other_cols_to_keep=[],
            )
        )
        self.terminal_cargo_dropoff_intervals = (
            self.terminal_cargo_dropoff_intervals.intersect_on_column(
                self.terminal_open_intervals,
                self_col="to_terminal",
                other_col="terminal",
                self_cols_to_keep=["to_terminal", "cargo"],
                other_cols_to_keep=[],
            )
        )

        self.driving_times = driving_times

        # Find the intervals during which the delivery can start
        # and then the truck will arrive while the to_terminal is open
        self.direct_delivery_start_intervals = (
            self.terminal_cargo_pickup_intervals.intersect_on_column(
                other=self.terminal_cargo_dropoff_intervals.shift_by(
                    lambda interval: -cargo_direct_delivery_times[
                        interval["cargo"]
                    ]
                ),
                self_col="cargo",
                other_col="cargo",
                self_cols_to_keep=["cargo", "from_terminal", "driving_time"],
                other_cols_to_keep=["to_terminal"],
            )
        )

        self.recalculate_possible_changes()

    def recalculate_possible_changes(self) -> None:
        for truck in self.uncached_trucks:
            assert truck is not INVALID_ID

            self.possible_changes[truck] = []

            truck_events = self.truck_events[truck]

            # Find intervals between deliveries
            unoccupied_windows = Intervals.from_dataframe(
                truck_events,
                TruckEvent.DELIVERY_END,
                TruckEvent.DELIVERY_START,
                cols_to_keep=["terminal"],
            )

            # Enforce that deliveries can occur while terminal is working
            unoccupied_windows = unoccupied_windows.intersect_on_column(
                self.terminal_open_intervals,
                self_col="terminal",
                other_col="terminal",
                self_cols_to_keep=[],
                other_cols_to_keep=["terminal"],
            )

            # Try to schedule deliveries during these windows

            # Each interval for a possible start of a direct delivery
            # is extended to also include completion time; so
            # each interval I is replaced with the union of intervals
            # [t, t + delivery time] for each t in I
            delivery_intervals: Intervals = (
                self.direct_delivery_start_intervals.shift_by(
                    lambda interval: interval["driving_time"],
                    shift_end_time=True,
                    shift_start_time=False,
                )
            )

            delivery_intervals_for_truck: Intervals = (
                unoccupied_windows.intersect_on_column(
                    delivery_intervals,
                    self_col="terminal",
                    other_col="from_terminal",
                    self_cols_to_keep=[],
                    other_cols_to_keep=[
                        "from_terminal",
                        "to_terminal",
                        "cargo",
                        "driving_time",
                    ],
                )
            )

            # First, try to deliver directly from current terminal to
            # another terminal and then return without cargo
            # TODO: is there any circumstance where always picking the earliest
            # allowed departure time is a worse option?
            # TODO: check that the truck can still do this delivery.
            # For example, we could have starting intervals
            # [            ]    [           ]
            # which were then expanded to
            # [                                        ]
            # And a truck could be available in
            #                [ ]
            # Which might give us a false positive

            def has_time_to_deliver_and_return(row: pd.Series) -> bool:
                start_time = row["start_time"]
                end_time = row["end_time"]
                from_terminal = row["from_terminal"]
                to_terminal = row["to_terminal"]
                return (
                    self.driving_times[(from_terminal, to_terminal)]
                    + self.driving_times[(to_terminal, from_terminal)]
                    <= end_time - start_time
                )

            # Pick earliest possible departure time and check
            # that it is possible to do this
            short_enough_intervals = delivery_intervals_for_truck.data[
                delivery_intervals_for_truck.data.apply(
                    has_time_to_deliver_and_return, axis=1
                )
            ]

            print(truck)
            print(short_enough_intervals)

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
        separator = "-------\n"

        out = "Schedule:\n\nTerminals:\n\n"

        out += "Opening times:\n"
        out += repr(self.terminal_open_intervals) + "\n\n"

        out += "Possible direct delivery start times:\n"
        # Reorder for ease of reading
        out += (
            repr(
                self.direct_delivery_start_intervals.data[
                    [
                        "start_time",
                        "end_time",
                        "from_terminal",
                        "to_terminal",
                        "cargo",
                    ]
                ]
            )
            + "\n\n"
        )

        out += "Truck events:\n\n"

        for truck, events in self.truck_events.items():
            out += separator
            out += f"Truck {truck}:\n\n"
            # Replace event type numbers with enum names
            events = events.copy()
            events["event_type"] = events["event_type"].apply(
                lambda x: TruckEvent(x).name
            )
            out += repr(events) + "\n"

        out += "Cargo:\n\n"
        out += "Pickup:\n"
        out += repr(self.terminal_cargo_pickup_intervals) + "\n"
        out += separator
        out += "Dropoff:\n"
        out += repr(self.terminal_cargo_dropoff_intervals) + "\n"

        # out += "Truck unoccupied windows:\n\n"
        # for truck, events in self.truck_events.items():
        #     out += "-------\n"
        #     out += f"Truck {truck}:\n"
        #     out += repr(self.unoccupied_windows[truck]) + "\n\n"

        return out
