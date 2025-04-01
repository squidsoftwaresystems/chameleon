from typing import List

import pandas as pd
import pytest

from src.metaheuristic.sa import sa_solve
from src.metaheuristic.schedule import make_schedule_generator


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

    terminals = pd.DataFrame(
        {
            "opening_time": to_time([7, 8, 9]),
            "closing_time": to_time([17, 18, 19]),
        }
    )

    trucks = pd.DataFrame({"starting_terminal": [0, 0, 0, 1, 1, 2]})

    # fmt: off
    transports = pd.DataFrame(
        {
            "cargo": range(6),
            "from_terminal":              [0,  0,  1,  1,   2,   2],
            "to_terminal":                [1,  2,  0,  2,   0,   1],
            "pickup_open_time":   to_time([6, 10, 10, 15,   8,  15]),
            "pickup_close_time":  to_time([8, 14, 14, 19,  19,  20]),
            "dropoff_open_time":  to_time([8, 12, 10, 20,   9,  15]),
            "dropoff_close_time": to_time([9, 14, 12, 22,  10,  17]),
            "driving_time": to_timedelta([1,  1,  1,  2, 0.5, 2.5]),
        }
    )
    # fmt: on
    return (terminals, trucks, transports)


def test_get_neighbours():
    (terminals, trucks, transports) = create_schedule_data()
    schedule_generator = make_schedule_generator(terminals, trucks, transports)
    schedule = schedule_generator.empty_schedule()

    # I don't know how to test this without inspecting it by hand,
    # or testing that the schedule is valid.
    # TODO: automatically check that the schedule is valid
    for _ in range(100):
        schedule = schedule_generator.get_schedule_neighbour(schedule, 100)


def test_simulated_annealing():
    (terminals, trucks, transports) = create_schedule_data()
    schedule_generator = make_schedule_generator(terminals, trucks, transports)
    schedule_generator.seed(4)

    schedule = schedule_generator.empty_schedule()
    best_schedule, best_score = sa_solve(
        initial_solution=schedule,
        schedule_generator=schedule_generator,
        seed=0,
    )

    # TODO: think of a way to test that its output has not
    # significantly degraded
