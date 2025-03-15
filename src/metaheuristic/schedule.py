from abc import ABC
from typing import List

import pandas as pd


class Event(ABC):
    """
    Timestamp (and additional information) of an event at a terminal
    """
    
    time = None


class TerminalOpeningEvent(Event):
    """
    Event of terminal starting work in some day
    """
    pass


class TerminalClosingEvent(Event):
    """
    Event of terminal finishing work in some day
    """
    pass


class CargoPickupOpenEvent(Event):
    """
    Event signalling beginning of cargo pickup slot
    """
    pass


class CargoPickupCloseEvent(Event):
    """
    Event signalling ending of cargo pickup slot
    """
    pass


class CargoDropoffOpenEvent(Event):
    """
    Event signalling beginning of cargo dropoff slot
    """
    pass


class CargoDropoffCloseEvent(Event):
    """
    Event signalling ending of cargo dropoff slot
    """
    pass


class TruckUnloadedEvent(Event):
    """
    Event signalling that a truck has arrived to this terminal and unloaded its cargo
    """

    # TODO: add code which distinguishes between trucks of same type
    # truck = None

    origin_terminal = None
    "Index of terminal that the truck came from"

    unload_time: int
    """
    Length of time in minutes it took to unload cargo from truck.
    Time of event = time when truck arrived + unload_time
    """


class TruckReadyToLoadEvent(Event):
    """
    Event signalling that a truck has loaded its cargo and has left for a delivery
    """

    # TODO: add code which distinguishes between trucks of same type
    # truck = None

    destination_terminal = None
    "Index of terminal that the truck is going to"

    load_time: int
    """
    Length of time in minutes it took to load cargo onto truck.
    Time of event = time when truck left - load_time
    """


class Schedule:
    """
    A class representing a timetable for deliveries which
    doesn't violate hard constraints
    """

    terminal_events: List[Event]

    # TODO: store cargo s.t. it is easy to retrieve available cargo in
    # terminal based on time or cargo on truck based on time,
    # and easy to change/remove/add cargo also check for cargo weight, size
    terminal_cargo_df: pd.DataFrame
    """
    A dataframe containing information about cargo in terminals.
    Each entry contains time interval over which cargo is in the terminal and the terminal
    """

    truck_cargo_df: pd.DataFrame
    """
    A dataframe containing information about cargo in trucks.
    Each entry contains time interval over which cargo is in the truck and the truck
    """

    def get_number_of_deliveries(self):
        """
        Returns number of deliveries in the schedule

        :returns: the number of cargo items delivered under this schedule
        :rtype: int
        """