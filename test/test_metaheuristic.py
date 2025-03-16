from typing import List

import pandas as pd
import pytest

from src.metaheuristic.neighbours import get_neighbours
from src.metaheuristic.schedule import Schedule


def test_get_neighbours():
    def verify_neighbourhood_constraints(schedule: Schedule):
        nbs = get_neighbours(schedule)
        for nb in nbs:
            assert True

    schedule1 = Schedule()
    verify_neighbourhood_constraints(schedule1)
    schedule2 = Schedule()
    verify_neighbourhood_constraints(schedule2)
    schedule3 = Schedule()
    verify_neighbourhood_constraints(schedule3)

    raise NotImplementedError


def test_schedule_creation():
    # Converts list of hours to pandas datetime index
    def to_time(hours: List[float]) -> pd.DatetimeIndex:
        # convert to seconds
        seconds = [int(hour * 60 * 60) for hour in hours]

        return pd.to_datetime(seconds, origin="unix", unit="s")

    # Converts list of hours to pandas TimedeltaIndex
    def to_timedelta(hours: List[float]) -> pd.TimedeltaIndex:
        # convert to seconds
        seconds = [int(hour * 60 * 60) for hour in hours]

        return pd.to_timedelta(seconds, unit="s")

    terminals = pd.DataFrame(
        {"opening_time": to_time([7, 8, 9]), "closing_time": to_time([17, 18, 19])}
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
            "dropoff_close": to_timedelta([1,  1,  1,  2, 0.5, 2.5]),
        }
    )
    # fmt: on

    schedule = Schedule(terminals, trucks, transports)
