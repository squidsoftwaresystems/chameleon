from .neighbours import get_neighbours
from .schedule import Schedule


def ga_solve(problem):
    schedule: Schedule = None
    get_neighbours(schedule)
