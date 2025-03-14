from schedule import Schedule


def get_neighbours(schedule: Schedule):
    # Add a route
    # When adding a route, consider (some subset of) trucks in-flight to see if some truck in flight can do this delivery instead of a truck already at the terminal
    # Might have to add "snapping points" in that case

    # Remove a route
    # NOTE: if schedule A has neighbour B, then B has neighbour A
    pass
