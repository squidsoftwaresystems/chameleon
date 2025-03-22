from copy import deepcopy
from random import random
from typing import Dict, List, Self, Set, Tuple, cast

import pandas as pd

from .intervals import Intervals, ListForInterval
from .transition import AddTransition, RemoveTransitions, TruckScheduleChange

# TODO: collapse 2 consecutive empty transports into 1


class Schedule:
    """
    A class representing a timetable for deliveries which
    doesn't violate hard constraints
    """

    # truck_events: Dict[int, pd.DataFrame]
    # """
    # A dict mapping terminal id to a dataframe containing all events that
    # happen to the truck
    #
    # Index:
    #     pd.DatetimeIndex
    # Columns:
    #     Name: event_type,   dtype: int64
    #     Name: terminal,     dtype: int64,
    #     Name: cargo,        dtype: int64,
    #
    # Note that terminal, truck, cargo, event_type refer to respective ids
    #
    # Invariant: cargo can't be delivered to terminal it has been to before
    # """
    #
    unoccupied_windows: Dict[int, Intervals]
    """
    A mapping of truck ids to the set of intervals at which they are idle.
    """

    possible_changes: Dict[int, List[TruckScheduleChange]]
    """
    A map of trucks to modifications that can be made to their schedule.
    """

    # TODO: allow multiple legs of transport per cargo
    # NOTE: assumes that each piece of cargo is shipped to a
    # specific terminal at most once
    requested_transports: pd.DataFrame
    """
    A dataframe of pieces of cargo that need to be transported
    """

    transitions: Dict[int, Intervals]
    """
    A map from trucks to transitions planned for them
    If we have a transition between terminals A -> B, and then C -> D,
    then it is implied that there is also a transition B -> C without cargo
    in between
    """
    # TODO: think of a way to allow hauling cargo during such B->C transition

    unplanned_cargo: Set[int]
    """
    A set of cargo ids which need to be planned for
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
    satisfying constraints such as pickup and dropoff window and driving time.
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
        requested_transports: pd.DataFrame,
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
                Name: driving_time,      dtype: timedelta64[ns]
        """

        self.requested_transports = requested_transports

        # Add terminal opening and closing times
        terminal_open_intervals = [
            (row["opening_time"], row["closing_time"], (terminal,))
            for terminal, row in terminal_data.iterrows()
        ]
        self.terminal_open_intervals = Intervals.from_list(
            terminal_open_intervals, column_names=["terminal"]
        )

        unoccupied_windows_by_truck: Dict[int, Intervals] = {}

        # Start with no deliveries planned
        self.transitions = {}

        # Specify where trucks start their working days
        for truck, row in truck_data.iterrows():
            # TODO: find a way to consider each transport once
            # among all trucks, despite different driver working times.
            truck = cast(int, truck)

            self.transitions[truck] = Intervals.from_list(
                [], column_names=["from_terminal", "to_terminal", "cargo"]
            )

            # Add a dummy event to specify starting location
            starting_terminal: int = row["starting_terminal"]
            opening_time = cast(
                pd.Timestamp,
                terminal_data.loc[starting_terminal, "opening_time"],
            )

            # TODO: put a more sane upper bound on when deliveries can finish

            # TODO: allow changing last terminal when adding new route,
            # since we don't care about where truck ends up
            unoccupied_windows_by_truck[truck] = Intervals.from_list(
                [
                    (
                        opening_time,
                        opening_time + pd.to_timedelta(24, unit="h"),
                        (starting_terminal, starting_terminal),
                    )
                ],
                column_names=["from_terminal", "to_terminal"],
            )

        self.unoccupied_windows = unoccupied_windows_by_truck

        self.unplanned_cargo = set()
        # Map of terminals to list of pickup slots in form
        # (start_time, end_time, cargo_id)
        terminal_pickups: ListForInterval = []
        terminal_dropoffs: ListForInterval = []

        driving_times: Dict[Tuple[int, int], pd.Timedelta] = {}
        # How long does it take to drive from "from_terminal" to "to_terminal"
        # for this cargo?
        cargo_direct_delivery_times: Dict[int, pd.Timedelta] = {}
        # When can cargo be moved?
        for _, row in requested_transports.iterrows():

            # Add a dummy event to specify starting location
            cargo: int = row["cargo"]
            from_terminal: int = row["from_terminal"]
            to_terminal: int = row["to_terminal"]
            pickup_open_time: pd.Timestamp = row["pickup_open_time"]
            pickup_close_time: pd.Timestamp = row["pickup_close_time"]
            dropoff_open_time: pd.Timestamp = row["dropoff_open_time"]
            dropoff_close_time: pd.Timestamp = row["dropoff_close_time"]
            driving_time: pd.Timedelta = row["driving_time"]

            self.unplanned_cargo.add(cargo)

            driving_times[(from_terminal, to_terminal)] = driving_time
            # Assume same length in both directions
            # TODO: maybe lift this assumption
            driving_times[(to_terminal, from_terminal)] = driving_time

            cargo_direct_delivery_times[cargo] = driving_time

            # Add events for pickup and dropoff slots
            terminal_pickups.append(
                (
                    pickup_open_time,
                    pickup_close_time,
                    (from_terminal, cargo, driving_time),
                )
            )
            terminal_dropoffs.append(
                (dropoff_open_time, dropoff_close_time, (to_terminal, cargo))
            )

        self.driving_times = driving_times

        # Create the intervals
        self.terminal_cargo_pickup_intervals = Intervals.from_list(
            terminal_pickups,
            column_names=[
                "from_terminal",
                "cargo",
                "driving_time",
            ],
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

        # Find the intervals during which the delivery can start
        # such that the truck will arrive to to_terminal while it is open
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

        # Finally, populate possible_changes
        self.possible_changes = {}
        for truck, row in truck_data.iterrows():
            truck = cast(int, truck)
            unoccupied_windows: Intervals = self.unoccupied_windows[truck]
            possible_changes_for_truck = []
            for _, unoccupied_window in unoccupied_windows:
                possible_changes_for_truck += (
                    self.__find_potential_transitions_in_interval(
                        unoccupied_window=unoccupied_window
                    )
                )
            self.possible_changes[truck] = possible_changes_for_truck

    # TODO: also take expired deliveries into account when
    # evaluating a schedule
    def get_number_of_deliveries(self):
        """
        Returns number of deliveries in the schedule

        :returns: the number of cargo items delivered under this schedule
        :rtype: int
        """
        pass

    def get_random_neighbour(self) -> Self:
        """
        Returns a uniformly sampled neighbour
        """

        # Uniformly pick an index among all possible changes
        num_possible_changes = sum(
            [
                len(self.possible_changes[truck])
                for truck in self.possible_changes
            ]
        )
        random_change_index = int(num_possible_changes * random())

        # Pick `random_change_index`th change
        for truck in self.possible_changes:
            changes = self.possible_changes[truck]
            # Skip if incorrect index
            if random_change_index >= len(changes):
                random_change_index -= len(changes)
            else:
                return self.copy().__implement_change(
                    truck, changes[random_change_index]
                )

        raise RuntimeError("Unexpectedly, no change was chosen")

    def get_driving_time(
        self, from_terminal: int, to_terminal: int
    ) -> pd.Timedelta:
        if from_terminal == to_terminal:
            return pd.Timedelta(0)
        else:
            return self.driving_times[(from_terminal, to_terminal)]

    def copy(self) -> Self:
        # Create without initialising
        other: Self = Self.__new__(type(self))

        other.unoccupied_windows = deepcopy(self.unoccupied_windows)

        other.possible_changes = deepcopy(self.possible_changes)

        other.transitions = deepcopy(self.transitions)

        other.unplanned_cargo = deepcopy(self.unplanned_cargo)

        other.requested_transports = self.requested_transports
        other.terminal_open_intervals = self.terminal_open_intervals
        other.terminal_cargo_pickup_intervals = (
            self.terminal_cargo_pickup_intervals
        )
        other.terminal_cargo_dropoff_intervals = (
            self.terminal_cargo_dropoff_intervals
        )
        other.direct_delivery_start_intervals = (
            self.direct_delivery_start_intervals
        )
        other.driving_times = self.driving_times

        return other

    def __implement_change(self, truck: int, change: TruckScheduleChange):
        """
        Modify `self` with schedule change `change` applied

        :param truck: id of the truck to which the change applies
        :param change: change to apply; assumed to be a valid change
        """
        if type(change) is AddTransition:
            change = cast(AddTransition, change)

            start_time = change.start_time
            end_time = change.end_time

            # Find the window in which we are adding the delivery
            old_unoccupied_window = self.unoccupied_windows[
                truck
            ].extract_interval(start_time, end_time)

            window_start_time: pd.Timestamp = old_unoccupied_window[
                "start_time"
            ]
            window_end_time: pd.Timestamp = old_unoccupied_window["end_time"]
            window_from_terminal: int = old_unoccupied_window["from_terminal"]
            window_to_terminal: int = old_unoccupied_window["to_terminal"]

            # Add this as a delivery
            self.unplanned_cargo.remove(change.cargo)
            self.transitions[truck] = self.transitions[truck].concat(
                Intervals.from_list(
                    [
                        (
                            start_time,
                            end_time,
                            (
                                change.from_terminal,
                                change.to_terminal,
                                change.cargo,
                            ),
                        )
                    ],
                    column_names=["from_terminal", "to_terminal", "cargo"],
                )
            )

            # Add the 2 new unoccupied windows formed by cutting out
            new_unoccupied_windows = Intervals.from_list(
                [
                    (
                        window_start_time,
                        start_time,
                        (window_from_terminal, change.from_terminal),
                    ),
                    (
                        end_time,
                        window_end_time,
                        (change.to_terminal, window_to_terminal),
                    ),
                ],
                column_names=["from_terminal", "to_terminal"],
            )
            self.unoccupied_windows[truck] = self.unoccupied_windows[
                truck
            ].concat(new_unoccupied_windows)

            # Update the possible changes. For example, allow removing this transition
            updated_possible_changes: List[TruckScheduleChange] = [
                RemoveTransitions(start_time=start_time, end_time=end_time)
            ]
            # This might invalidate some delivery possibilities
            # that would clash with this delivery. Try to move them.
            for other_change in self.possible_changes[truck]:
                assert type(other_change) in [AddTransition, RemoveTransitions]
                if type(other_change) is AddTransition:
                    other_change = cast(AddTransition, other_change)
                    updated_possible_changes += other_change.update_on_transition_add(
                        old_unoccupied_window=old_unoccupied_window,
                        new_unoccupied_windows=new_unoccupied_windows,
                        get_driving_time=self.get_driving_time,
                        direct_delivery_start_intervals=self.direct_delivery_start_intervals,
                    )
                elif type(other_change) is RemoveTransitions:
                    # Just keep it
                    updated_possible_changes.append(other_change)

            self.possible_changes[truck] = updated_possible_changes
        elif type(change) is RemoveTransitions:
            change = cast(RemoveTransitions, change)
            start_time = change.start_time
            end_time = change.end_time
            # Delete all transitions in the range
            transitions_before: Intervals = self.transitions[
                truck
            ].filter_predicate(lambda row: (row["end_time"] <= start_time))
            transitions_after: Intervals = self.transitions[
                truck
            ].filter_predicate(lambda row: (end_time <= row["start_time"]))
            self.transitions[truck] = transitions_before.concat(
                transitions_after
            )

            # Find an unoccupied_window
            unoccupied_window_start_time = transitions_before.latest()
            unoccupied_window_end_time = transitions_after.earliest()

            assert unoccupied_window_start_time is not None
            assert unoccupied_window_end_time is not None

            # Applying RemoveTransitions might have changed the potential changes
            # update them
            updated_possible_changes = []
            for other_change in self.possible_changes[truck]:
                assert type(other_change) in [AddTransition, RemoveTransitions]
                if type(other_change) is AddTransition:
                    other_change = cast(AddTransition, other_change)
                    # Only keep if doesn't intersect the interval
                    # We will recalculate everything in unoccupied_window
                    # interval, so don't keep transitions in that interval
                    if (
                        other_change.end_time <= unoccupied_window_start_time
                    ) or (
                        unoccupied_window_end_time <= other_change.start_time
                    ):
                        updated_possible_changes.append(other_change)
                elif type(other_change) is RemoveTransitions:
                    other_change = cast(RemoveTransitions, other_change)
                    # There is nothing to remove in unoccupied_window,
                    # so remove unoccupied_window from RemoveTransitions
                    # TODO: rewrite this as removal of intervals

                    # TODO: handle this case, too
                    # You will need to add 2 intervals, one before and one after
                    assert not (
                        other_change.start_time
                        <= unoccupied_window_start_time
                        <= unoccupied_window_end_time
                        <= other_change.end_time
                    )

                    # Remove sections of the interval that are already removed
                    if (
                        unoccupied_window_start_time
                        <= other_change.start_time
                        <= unoccupied_window_end_time
                    ):
                        other_change.start_time = unoccupied_window_end_time
                    if (
                        unoccupied_window_start_time
                        <= other_change.end_time
                        <= unoccupied_window_end_time
                    ):
                        other_change.end_time = unoccupied_window_start_time

                    # If still a valid non-empty interval after changes
                    if other_change.start_time < other_change.end_time:
                        updated_possible_changes.append(other_change)

            # Find from_terminal and to_terminal
            earlier_intervals = transitions_before.data[
                transitions_before.data["end_time"]
                == unoccupied_window_start_time
            ]
            later_intervals = transitions_after.data[
                transitions_after.data["start_time"]
                == unoccupied_window_end_time
            ]

            # Should have exactly one row
            assert earlier_intervals.shape[0] == 1
            assert later_intervals.shape[0] == 1

            # Match the intervals at endpoints
            from_terminal = earlier_intervals["to_terminal"]
            to_terminal = later_intervals["from_terminal"]

            unoccupied_window = pd.Series(
                {
                    "start_time": unoccupied_window_start_time,
                    "end_time": unoccupied_window_end_time,
                    "from_terminal": from_terminal,
                    "to_terminal": to_terminal,
                }
            )

            # Calculate what transitions can be added in this interval
            self.possible_changes[truck] = (
                updated_possible_changes
                + self.__find_potential_transitions_in_interval(
                    unoccupied_window
                )
            )

        else:
            raise RuntimeError(f"Unknown transition type: {type(change)}")

    def __find_potential_transitions_in_interval(
        self,
        unoccupied_window: pd.Series,
    ) -> List[TruckScheduleChange]:
        """
        Returns list of possible AddTransitions that can be added to interval
        `unoccupied_window`.
        """
        window_start_time = unoccupied_window["start_time"]
        window_end_time = unoccupied_window["end_time"]
        from_terminal = unoccupied_window["from_terminal"]
        to_terminal = unoccupied_window["to_terminal"]

        # TODO: find out what cargo could have been taken with us
        # up to this point, and consider it separately for a
        # multi-step delivery

        # Consider direct deliveries

        # Only keep deliveries which start in `unoccupied_window`,
        # as a rough first filter to reduce the amount of cargo considered
        relevant_delivery_starts = (
            self.direct_delivery_start_intervals.limit_time(
                unoccupied_window["start_time"], unoccupied_window["end_time"]
            )
        )
        # only consider unplanned cargo
        relevant_delivery_starts.data = relevant_delivery_starts.data[
            relevant_delivery_starts.data["cargo"].isin(self.unplanned_cargo)
        ]

        out: List[TruckScheduleChange] = []
        for _, delivery_start_interval in relevant_delivery_starts:
            cargo = delivery_start_interval["cargo"]
            delivery_starts_from_terminal = delivery_start_interval[
                "from_terminal"
            ]
            delivery_starts_to_terminal = delivery_start_interval[
                "to_terminal"
            ]

            # Create a preliminary transition to measure time intervals with
            # All data here is correct, except for start_time and end_time.
            # We will reschedule this transition, keeping its duration
            change = AddTransition(
                from_terminal=delivery_starts_from_terminal,
                to_terminal=delivery_starts_to_terminal,
                start_time=window_start_time,
                end_time=window_start_time
                + self.get_driving_time(
                    delivery_starts_from_terminal,
                    delivery_starts_to_terminal,
                ),
                cargo=cargo,
            )

            # TODO: extract this and end of AddTransition.update_on_transition_add
            # into its own function

            duration, (left_padding, right_padding) = (
                change.get_duration_and_padding(
                    from_terminal, to_terminal, self.get_driving_time
                )
            )

            # Now check if we can fit this into the window
            # First see if we can add it to beginning of the window
            possible_start_intervals = Intervals.from_row(
                delivery_start_interval
            )
            possible_start_intervals = possible_start_intervals.limit_time(
                start_time=(window_start_time + left_padding),
                end_time=(window_end_time - right_padding - duration),
            )

            # Try moving it as early as possible
            if (time := possible_start_intervals.earliest()) is not None:
                out.append(change.reschedule_start(time))
            # Try moving as late as possible
            if (time := possible_start_intervals.latest()) is not None:
                out.append(change.reschedule_start(time))

        return out

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
