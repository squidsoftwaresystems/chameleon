from typing import List

import pandas as pd
import pytest

import src.api.SquidAPI as API
from src.metaheuristic.sa import sa_solve
from src.metaheuristic.schedule import (
    cached_make_schedule_data_from_api,
    make_schedule_generator,
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

    def get_driving_times(terminal_ids):
        assert terminal_ids == ["0", "1", "2"]
        return {
            "0": to_timedelta([0, 1, 1]),
            "1": to_timedelta([1, 0, 2]),
            "2": to_timedelta([2.5, 2.5, 0]),
        }

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
            "dropoff_open_time":  to_time([8, 12, 10, 20,  11,  15]),
            "dropoff_close_time": to_time([9, 14, 12, 22,  12,  17]),
        }
    )

    planning_period = to_time([3, 24])

    # fmt: on
    return (terminals, trucks, transports, planning_period, get_driving_times)


def test_get_neighbours():
    (terminals, trucks, transports, planning_period, get_driving_time) = (
        create_schedule_data()
    )

    schedule_generator = make_schedule_generator(
        terminals, trucks, transports, planning_period, get_driving_time
    )
    schedule = schedule_generator.empty_schedule()

    # I don't know how to test this without inspecting it by hand,
    # or testing that the schedule is valid.
    # TODO: automatically check that the schedule is valid
    for _ in range(100):
        schedule = schedule_generator.get_schedule_neighbour(schedule, 100)


def run_sa_with_seed(
    data, seed, num_iterations, print_score=True, print_schedule=False
):
    schedule_generator = make_schedule_generator(*data)
    schedule_generator.seed(seed)

    schedule = schedule_generator.empty_schedule()
    best_schedule, best_score = sa_solve(
        initial_solution=schedule,
        schedule_generator=schedule_generator,
        seed=seed,
        num_iterations=num_iterations,
    )

    if print_score:
        print(best_score)
    if print_schedule:
        print(best_schedule.repr(schedule_generator))

    # TODO: think of a way to test that its output has not
    # significantly degraded


def test_loading_api_data():
    planning_period = (
        pd.Timestamp("2025-03-24T00"),
        pd.Timestamp("2025-03-25T00"),
    )

    data = cached_make_schedule_data_from_api(API(), planning_period)
    schedule_generator = make_schedule_generator(*data)
    schedule = schedule_generator.empty_schedule()

    # Test that this data doesn't produce errors when looking for neighbours
    for _ in range(100):
        schedule = schedule_generator.get_schedule_neighbour(schedule, 100)


def test_simulated_annealing():
    """Tests that simulated annealing works without errors"""
    for i in range(20):
        data = create_schedule_data()
        run_sa_with_seed(data, i, num_iterations=10000)


def test_simulated_annealing_on_api():
    """Tests that simulated annealing works without errors"""
    planning_period = (
        pd.Timestamp("2025-03-24T00"),
        pd.Timestamp("2025-03-25T00"),
    )
    for i in range(20):
        data = cached_make_schedule_data_from_api(API(), planning_period)
        run_sa_with_seed(data, i, num_iterations=30000)
