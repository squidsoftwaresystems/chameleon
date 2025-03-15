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
