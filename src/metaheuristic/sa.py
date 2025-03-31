import math
import random
import sys
from math import exp, log

from chameleon_rust import Schedule, ScheduleGenerator


def sa_solve(
    initial_solution: Schedule,
    schedule_generator: ScheduleGenerator,
    initial_temperature: float = 10.0,
    final_temperature: float = 1e-3,
    num_iterations: int = 10000,
    num_tries_per_action: int = 10,
    restart_probability=0.001,
) -> Schedule:
    """
    This simulated annealing algorithm optimises a given objective function

    @param initial_solution initial guess for the solution
    @param schedule_generator algorithm for generating neighbouring schedules
    @param initial_temperature starting 'temperature' for the annealing process
    @param final_temperature final 'temperature' for the annealing process
    @param num_iterations number of iterations to perform before terminating
    @param num_tries_per_action a parameter for generation of neighbours
    @param restart_probability probability of going back to a best_solution

    @returns a schedule and its score
    """
    current_solution: Schedule = initial_solution
    current_score: float = initial_solution.score()

    best_solution = current_solution
    best_score = current_score

    temperature = initial_temperature

    iteration = 0

    # Calculate alpha, the cooling rate, so that after `num_iterations` iterations,
    # the temperature becomes `final_temperature`
    alpha = exp(
        (log(final_temperature) - log(initial_temperature)) / num_iterations
    )

    while temperature > final_temperature and iteration < num_iterations:
        # Allow randomly restarting to best known state
        if random.random() <= restart_probability:
            current_solution = best_solution
            current_score = best_score

        new_solution = schedule_generator.get_schedule_neighbour(
            current_solution, num_tries_per_action
        )  # generate a new candidate solution

        new_score = new_solution.score()

        delta = new_score - current_score  # calculate 'energy difference'

        # decide whether to accept the new solution
        if delta > 0:  # if new solution is better, always accept
            current_solution = new_solution
            current_score = new_score
        else:  # accept with a probability depending on the temperature
            try:
                acceptance_probability = math.exp(-new_score / temperature)
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
