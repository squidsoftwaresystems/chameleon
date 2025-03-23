from .schedule import Schedule
import math
import random
import sys

def someFunction(x,y):
        #a simple function that the sa algorithm will be optimising for testing purposes
        return (x^2)*(y^3)

#this simulated annealing algorithm optimises a given objective function - not sure how to adapt this to take in a schedule...
def simulated_annealing( #simulated annealing algorithm
        objective_function,         #the function to maximise. ((float,float,float...)->(float))
        initial_solution,           #inital guess for the solution (float, float)
        step_size=3,               #maximum perturbation to apply when generating a new neighbour (float)
        initial_temperature=10.0,   #starting 'temperature' for the annealing process (float)
        final_temperature=1e-3,     #final 'temperature' for the annealing process (float)
        alpha=0.99,                  #'cooling rate' (0<alpha<1). temperature is multiplied by alpha each iteration (float)
        max_iterations=10000,        #maximum number of iterations to perform before terminating
        bounds=[(-10,10),(-10,10)]                 #range for candidate solutions ((float, float), (float, float))
):                                  #returns: best_solution (float, float, float...), best_value (float)
    current_solution = list(initial_solution)
    current_value = objective_function(*current_solution)

    best_solution = current_solution[:]
    best_value = current_value

    temperature = initial_temperature

    iteration = 0

    while temperature > final_temperature and iteration < max_iterations:
        new_solution = current_solution[:]                          #generate a new candidate solution
        idx = random.randint(0, len(new_solution) - 1)              #randomly choose an index to perturb
        new_solution[idx] += random.uniform(-step_size, step_size)  #perturb the chosen index by a random value in [-step_size, step_size]

        if bounds:  #if bounds are specified, clamp the solution
            for i in range(len(new_solution)):
                low, high = bounds[i]
                new_solution[i] = max(low, min(high, new_solution[i]))

        new_value = objective_function(*new_solution)

        delta = new_value - current_value   #calculate 'energy difference'

        #decide whether to accept the new solution
        if delta > 0:   #if new solution is better, always accept
            current_solution = new_solution
            current_value = new_value
        else:   #accept with a probability depending on the temperature
            try:
                acceptance_probability = math.exp(-delta / temperature)
            except OverflowError:
                acceptance_probability = sys.float_info.max
            if random.random() < acceptance_probability:
                current_solution = new_solution
                current_value = new_value

        if current_value > best_value: #track the best solution found
            best_solution = current_solution[:]
            best_value = current_value

        temperature *= alpha    #'cool down'
        iteration += 1

    return best_solution, best_value

def sa_solve(problem):
    schedule: Schedule = Schedule()
    #run simulated annealing on problem
