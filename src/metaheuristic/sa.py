import math
import random
import sys

from chameleon_rust import Schedule, ScheduleGenerator


class RunningAverage:
    N: int
    avg: float

    def __init__(self):
        self.N = 0
        self.avg = 0

    def update(self, new_value: float):
        self.avg = self.N / (self.N + 1) * self.avg + new_value / (self.N + 1)
        self.N += 1

    def get_avg(self) -> float:
        return self.avg


def sa_solve(
    initial_solution: Schedule,
    schedule_generator: ScheduleGenerator,
    initial_temperature: float = 10.0,
    final_temperature: float = 1e-3,
    alpha: float = 0.99,
    max_iterations: int = 10000,
    num_tries: int = 100,
) -> Schedule:
    """
    This simulated annealing algorithm optimises a given objective function

    @param the function to maximise.
    @param initial_solution initial guess for the solution
    @param schedule_generator algorithm for generating neighbouring schedules
    @param starting 'temperature' for the annealing process
    @param final 'temperature' for the annealing process
    @param 'cooling rate' (0<alpha<1). Temperature is multiplied by alpha each iteration
    @param maximum number of iterations to perform before terminating

    @returns a schedule
    """
    current_solution: Schedule = initial_solution
    current_score: float = initial_solution.score()

    average_delta_magnitude = RunningAverage()

    best_solution = current_solution
    best_score = current_score

    temperature = initial_temperature

    iteration = 0

    while temperature > final_temperature and iteration < max_iterations:
        new_solution = schedule_generator.get_schedule_neighbour(
            current_solution, num_tries
        )  # generate a new candidate solution

        new_score = new_solution.score()

        delta = new_score - current_score  # calculate 'energy difference'

        average_delta_magnitude.update(abs(delta))

        # decide whether to accept the new solution
        if delta > 0:  # if new solution is better, always accept
            current_solution = new_solution
            current_score = new_score
        else:  # accept with a probability depending on the temperature
            try:
                # use the average score to put the current score into context
                ratio = new_score / average_delta_magnitude.get_avg()
                acceptance_probability = math.exp(-ratio / temperature)
            except OverflowError:
                acceptance_probability = sys.float_info.max
            if random.random() < acceptance_probability:
                current_solution = new_solution
                current_score = new_score

        if current_score > best_score:  # track the best solution found
            best_solution = current_solution
            best_score = current_score

        # 'cool down'
        temperature *= alpha
        iteration += 1

    return best_solution, best_score
