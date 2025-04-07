from typing import Tuple, List

from chameleon_rust import Schedule, ScheduleGenerator

def ts_solve(
    initial_solution: Schedule,
    schedule_generator: ScheduleGenerator,
    tabu_list_size: int = 50,
    max_iterations: int = 10000,
    num_tries: int = 100,
    candidate_neighbours: int = 10
) -> Tuple[Schedule, float]:
    """
    This tabu search algorithm optimizes a schedule based on an objective function.
    
    @param initial_solution: The initial guess for the schedule.
    @param schedule_generator: A generator to produce neighboring schedules.
    @param tabu_list_size: Maximum number of recent solutions to store to avoid cycles.
    @param max_iterations: Maximum number of iterations to perform.
    @param num_tries: Number of candidate neighbors generated per iteration.
    
    @returns a tuple (best_solution, best_score)
    """
    current_solution: Schedule = initial_solution
    current_score: float = initial_solution.score()
    
    best_solution = current_solution
    best_score = current_score
    
    # Tabu list stores recently visited solutions.
    tabu_list: List["Schedule"] = []
    
    iteration = 0
    while iteration < max_iterations:
        candidates = []
        # Generate a set of candidate neighbor schedules.
        for _ in range(candidate_neighbours):
            neighbor = schedule_generator.get_schedule_neighbour(current_solution, num_tries)
            candidates.append(neighbor)
        
        # Sort candidates based on their score
        candidates.sort(key=lambda s: s.score(), reverse=True)
        
        selected = None
        # Look for the best candidate that is either not in the tabu list or meets the aspiration criterion.
        for candidate in candidates:
            # Aspiration: if candidate is better than the best known solution, accept it even if it is tabu.
            if candidate not in tabu_list or candidate.score() > best_score:
                selected = candidate
                break
        
        # If no candidate was found (all are tabu), then just select the best candidate.
        if selected is None:
            selected = candidates[0]
        
        # Update the current solution.
        current_solution = selected
        current_score = current_solution.score()
        
        # Update the overall best solution if improvement is found.
        if current_score > best_score:
            best_solution = current_solution
            best_score = current_score
        
        # Add the current solution to the tabu list.
        tabu_list.append(current_solution)
        if len(tabu_list) > tabu_list_size:
            tabu_list.pop(0)  # Remove the oldest entry to maintain fixed size.
        
        iteration += 1

    return best_solution, best_score
