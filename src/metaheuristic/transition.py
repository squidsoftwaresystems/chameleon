from abc import ABC
from typing import Callable, Dict, List, Self, Tuple, cast

import pandas as pd

from .intervals import Intervals


class TruckScheduleChange(ABC):
    """
    An abstract class representing a change in schedule of a terminal,
    from a point of view of a truck.

    Instances of this class are stored together with the truck,
    so don't contain a reference to the truck inside the class.
    """

    pass


# TODO: instead consider it as a series of routes + checkpoints
# Checkpoints necessitate returning to the checkpoint terminal by checkpoint time
class AddTransition(TruckScheduleChange):
    """
    A class representing addition of a transition
    from_terminal -> to_terminal while carrying cargo. If the next
    transition starts at a different terminal, it implies a transition without
    cargo to that terminal
    """

    from_terminal: int
    """
    Id of a terminal from which the truck leaves
    """

    to_terminal: int
    """
    Id of a terminal to which the truck arrives
    """

    start_time: pd.Timestamp
    """
    Timestamp of when truck leaves
    """

    end_time: pd.Timestamp
    """
    Timestamp of when truck arrives
    """

    cargo: int
    """
    The id of cargo transported
    """

    def __init__(
        self,
        from_terminal: int,
        to_terminal: int,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        cargo: int,
    ):
        self.from_terminal = from_terminal
        self.to_terminal = to_terminal
        self.start_time = start_time
        self.end_time = end_time
        self.cargo = cargo

    @classmethod
    def from_interval(cls, interval: pd.Series) -> Self:
        """
        :param interval: an interval in the format of a row of `Intervals`
        """
        return cls(
            from_terminal=interval["from_terminal"],
            to_terminal=interval["to_terminal"],
            start_time=interval["start_time"],
            end_time=interval["end_time"],
            cargo=interval["cargo"],
        )

    def get_duration_and_padding(
        self,
        from_terminal: int,
        to_terminal: int,
        get_driving_time: Callable[[int, int], pd.Timedelta],
    ) -> Tuple[pd.Timedelta, Tuple[pd.Timedelta, pd.Timedelta]]:
        """
        If we are adding this transition to an unoccupied window starting
        at `from_terminal` and ending at `to_terminal`, return how long
        of a time period we need for the delivery to be added. For example,
        if `from_terminal`=A, `self.from_terminal`=B, `self.to_terminal`=C,
        `to_terminal`=D, then the padding is `driving_times[(A, B)]` and
        `driving_times[(C, D)]` and the duration is total time spent

        :param from_terminal: terminal where unoccupied window starts
        :param to_terminal: terminal where unoccupied window stops
        :param driving_times: map from (from_terminal, to_terminal) to how long it takes to drive

        :return: [delivery_duration, [left_padding, right_padding]] where the truck
        moves to `self.from_terminal` in `left_padding` time, then does the delivery in
        `delivery_duration` time and then moves to `to_terminal` in `right_padding` time
        """
        if from_terminal == self.from_terminal:
            left_padding = pd.Timedelta(0)
        else:
            left_padding = get_driving_time(from_terminal, self.from_terminal)
        if to_terminal == self.to_terminal:
            right_padding = pd.Timedelta(0)
        else:
            right_padding = get_driving_time(self.from_terminal, to_terminal)

        delivery_duration = get_driving_time(
            self.from_terminal, self.to_terminal
        )
        assert delivery_duration == self.end_time - self.start_time

        return (
            delivery_duration,
            (left_padding, right_padding),
        )

    def reschedule_start(self, start_time: pd.Timestamp) -> Self:
        """
        Create a copy of `self` that starts at `start_time`
        """
        return type(self)(
            from_terminal=self.from_terminal,
            to_terminal=self.to_terminal,
            start_time=start_time,
            end_time=self.end_time + (start_time - self.start_time),
            cargo=self.cargo,
        )

    # def reschedule_end(self, end_time: pd.Timestamp) -> Self:
    #     """
    #     Create a copy of `self` that ends at `end_time`
    #     """
    #     return type(self)(
    #         from_terminal=self.from_terminal,
    #         to_terminal=self.to_terminal,
    #         start_time=self.start_time + (end_time - self.end_time),
    #         end_time=end_time,
    #         cargo=self.cargo,
    #     )

    def update_on_transition_add(
        self,
        old_unoccupied_window: pd.Series,
        new_unoccupied_windows: Intervals,
        get_driving_time: Callable[[int, int], pd.Timedelta],
        direct_delivery_start_intervals: Intervals,
    ) -> List[Self]:
        """
        Return a list of modified versions of `self` that
        represent how this `AddTransition` can be changed
        so that it is valid after a transition has been added
        """

        # If doesn't intersect, skip
        if (self.end_time <= old_unoccupied_window["start_time"]) or (
            old_unoccupied_window["end_time"] <= self.start_time
        ):
            return [self]

        out: List[Self] = []
        # Otherwise, it was contained in old_unoccupied_window
        assert old_unoccupied_window["start_time"] <= self.start_time
        assert self.end_time <= old_unoccupied_window["end_time"]

        # Consider moving them to before or after delivery
        for _, unoccupied_window in new_unoccupied_windows:
            delivery_duration, (left_padding, right_padding) = (
                self.get_duration_and_padding(
                    unoccupied_window["from_terminal"],
                    unoccupied_window["to_terminal"],
                    get_driving_time,
                )
            )

            # Add information about carried cargo, so that we can
            # restrict our search to relevant cargo intervals
            unoccupied_window = unoccupied_window.copy()
            unoccupied_window["cargo"] = self.cargo

            # When can the actual delivery occur
            # (disregarding other commitments of this truck)
            possible_start_intervals = (
                # TODO: maybe it is faster to limit_time than intersect_on_column
                # So maybe remove being able to call intersect_on_column with pd.Series
                direct_delivery_start_intervals.intersect_on_column(
                    unoccupied_window,
                    self_col="cargo",
                    other_col="cargo",
                    self_cols_to_keep=[],
                    other_cols_to_keep=[],
                )
            )

            # We want to give some time for the truck to drive to
            # other_change.from_terminal and drive
            # from other_change.to_terminal
            # So consider left_padding and right_padding
            possible_start_intervals = possible_start_intervals.limit_time(
                start_time=(unoccupied_window["start_time"] + left_padding),
                end_time=(
                    unoccupied_window["end_time"]
                    - right_padding
                    - delivery_duration
                ),
            )

            # Try moving it as early as possible
            if (time := possible_start_intervals.earliest()) is not None:
                out.append(self.reschedule_start(time))
            # Try moving as late as possible
            if (time := possible_start_intervals.latest()) is not None:
                out.append(self.reschedule_start(time))

        return out


class RemoveTransitions(TruckScheduleChange):
    """
    A class representing deletion of all transitions intersecting a time
    interval. It does not remove intervals which only share an endpoint with it
    """

    start_time: pd.Timestamp
    """
    Timestamp of when truck leaves
    """

    end_time: pd.Timestamp
    """
    Timestamp of when truck arrives
    """

    def __init__(self, start_time: pd.Timestamp, end_time: pd.Timestamp):
        self.start_time = start_time
        self.end_time = end_time


# class RescheduleTransition(TruckScheduleChange):
#     """
#     A class representing changing the time of a transition
#     """
#
#     pass
