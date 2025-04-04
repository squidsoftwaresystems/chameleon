from typing import List

import pandas as pd
import pytest

import src.api.SquidAPI as API
from src.metaheuristic.sa import sa_solve
from src.metaheuristic.schedule import (
    make_schedule_generator,
    make_schedule_generator_from_api,
)


def create_schedule_data():
    # Converts list of hours to pandas datetime index
    def to_time(hours: List[float]) -> pd.DatetimeIndex:
        # convert to seconds
        seconds = [int(hour * 60) for hour in hours]

        return pd.to_datetime(seconds, origin="unix", unit="m")

    # Converts list of hours to pandas TimedeltaIndex
    def to_timedelta(hours: List[float]) -> pd.TimedeltaIndex:
        # convert to seconds
        seconds = [int(hour * 60) for hour in hours]

        return pd.to_timedelta(seconds, unit="m")

    def to_str(values):
        return [str(val) for val in values]

    terminals = pd.DataFrame(
        {
            "opening_time": to_time([7, 8, 9]),
            "closing_time": to_time([17, 18, 19]),
        },
        index=["0", "1", "2"],
    )

    trucks = pd.DataFrame(
        {"starting_terminal": to_str([0, 0, 0, 1, 1, 2])},
        index=to_str(range(6)),
    )

    # fmt: off
    transports = pd.DataFrame(
        {
            "cargo": to_str(range(6)),
            "from_terminal":       to_str([0,  0,  1,  1,   2,   2]),
            "to_terminal":         to_str([1,  2,  0,  2,   0,   1]),
            "pickup_open_time":   to_time([6, 10, 10, 15,   8,  15]),
            "pickup_close_time":  to_time([8, 14, 14, 19,  19,  20]),
            "dropoff_open_time":  to_time([8, 12, 10, 20,   9,  15]),
            "dropoff_close_time": to_time([9, 14, 12, 22,  10,  17]),
            "driving_time":  to_timedelta([1,  1,  1,  2, 0.5, 2.5]),
        }
    )

    planning_period = to_time([5, 20])

    # fmt: on
    return (terminals, trucks, transports, planning_period)


def test_get_neighbours():
    (terminals, trucks, transports, planning_period) = create_schedule_data()

    schedule_generator = make_schedule_generator(
        terminals, trucks, transports, planning_period
    )
    schedule = schedule_generator.empty_schedule()

    # I don't know how to test this without inspecting it by hand,
    # or testing that the schedule is valid.
    # TODO: automatically check that the schedule is valid
    for _ in range(100):
        schedule = schedule_generator.get_schedule_neighbour(schedule, 100)


def run_sa_with_seed(seed):
    (terminals, trucks, transports, planning_period) = create_schedule_data()
    schedule_generator = make_schedule_generator(
        terminals, trucks, transports, planning_period
    )
    schedule_generator.seed(seed)

    schedule = schedule_generator.empty_schedule()
    best_schedule, best_score = sa_solve(
        initial_solution=schedule,
        schedule_generator=schedule_generator,
        seed=seed,
    )
    print(best_score)

    # TODO: think of a way to test that its output has not
    # significantly degraded


def test_loading_api_data():
    planning_period = (
        pd.Timestamp("2025-02-19T00"),
        pd.Timestamp("2025-02-20T00"),
    )
    schedule_generator = make_schedule_generator_from_api(
        API(), planning_period
    )
    schedule = schedule_generator.empty_schedule()

    # Test that this data doesn't produce errors when looking for neighbours
    for _ in range(100):
        schedule = schedule_generator.get_schedule_neighbour(schedule, 100)


def test_simulated_annealing():
    """Tests that simulated annealing works without errors"""
    for i in range(20):
        run_sa_with_seed(i)
