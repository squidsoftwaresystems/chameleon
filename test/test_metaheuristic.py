import random
from typing import List

import pandas as pd
import pytest

from src.metaheuristic.schedule import Schedule


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
    schedule = Schedule(terminals, trucks, transports)
    random.seed(0)

    for _ in range(10):
        schedule.get_random_neighbour()


def test_schedule_creation():
    (terminals, trucks, transports) = create_schedule_data()
    schedule = Schedule(terminals, trucks, transports)
