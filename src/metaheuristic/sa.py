import math
import random
import sys
from math import exp, log

import numpy as np
import numpy.typing as npt

from chameleon_rust import Schedule, ScheduleGenerator
from src.metaheuristic.schedule import get_scores_calculator


def __deltas_to_probability(deltas: npt.NDArray, temperature: float) -> float:
    (deliveries_delta, free_trucks_delta, driving_time_delta) = deltas

    # We are mainly optimising for delivered cargo,
    # so encourage the switch
    combined_delta = 3 * deliveries_delta + 0.05 * free_trucks_delta

    # Minimising truck time is secondary to maximising number of deliveries
    if deliveries_delta <= 0:
        combined_delta += driving_time_delta

    try:
        return math.exp(combined_delta / temperature)
    except OverflowError:
        return sys.float_info.max


def __is_better(deltas: npt.NDArray) -> bool:
    """
    Is `schedule1` better than `schedule2`, where
    `deltas` = `schedule1_score - schedule2_score`
    """
    (deliveries_delta, free_trucks_delta, driving_time_delta) = deltas
    if deliveries_delta > 0:
        return True
    elif deliveries_delta == 0 and driving_time_delta > 0:
        return True
    else:
        return (
            3 * deliveries_delta + 0.5 * free_trucks_delta + driving_time_delta
            > 0
        )


def sa_solve(
    initial_solution: Schedule,
    schedule_generator: ScheduleGenerator,
    initial_temperature: float = 10.0,
    final_temperature: float = 1e-3,
    num_iterations: int = 10000,
    num_tries_per_action: int = 10,
    restart_probability=0.001,
    seed=0,
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
    @param seed: seed for the rng

    @returns a schedule and its score
    """
    random.seed(seed)
    get_scores = get_scores_calculator(schedule_generator)

    current_solution: Schedule = initial_solution
    current_scores: npt.NDArray = get_scores(current_solution)

    best_solution = current_solution
    best_scores = current_scores

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
            current_scores = best_scores

        new_solution = schedule_generator.get_schedule_neighbour(
            current_solution, num_tries_per_action
        )  # generate a new candidate solution

        new_scores = get_scores(new_solution)

        deltas: npt.NDArray = (
            new_scores - current_scores
        )  # calculate 'energy difference'

        # decide whether to accept the new solution
        if __is_better(deltas):  # if new solution is better, always accept
            current_solution = new_solution
            current_scores = new_scores
        else:  # accept with a probability depending on the temperature
            acceptance_probability = __deltas_to_probability(
                deltas, temperature
            )
            if random.random() < acceptance_probability:
                current_solution = new_solution
                current_scores = new_scores

        if __is_better(
            new_scores - best_scores
        ):  # track the best solution found
            best_solution = current_solution
            best_scores = current_scores

        # 'cool down'
        temperature *= alpha
        iteration += 1

    return best_solution, best_scores
