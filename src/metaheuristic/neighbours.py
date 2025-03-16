from typing import List

from .schedule import Schedule


def get_neighbours(schedule: Schedule) -> List[Schedule]:
    # Add a route
    # Allow moving empty truck to different terminal and picking up cargo there
    # When adding a route, consider (some subset of) trucks in-flight to see if some truck in flight can do this delivery instead of a truck already at the terminal
    # Might have to add "snapping points" in that case

    # Keep "cached neighbours" among terminals/trucks affected

    # Remove a route
    # NOTE: Try to make it so that if schedule A has neighbour B, then B has neighbour A
    # If not possible, instead allow a chain B->C->A to get back to original schedule

    # Maybe try moving a route within "snapping" positions for this truck

    return []
