from .neighbours import get_neighbours
from .schedule import Schedule


def sa_solve(problem):
    schedule: Schedule = Schedule()
    get_neighbours(schedule)
    pass
