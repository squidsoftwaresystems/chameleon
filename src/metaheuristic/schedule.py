class Event:
    time = None


class TerminalOpeningEvent(Event):
    pass


class TerminalClosingEvent(Event):
    pass


class CargoPickupOpenEvent(Event):
    pass


class CargoPickupCloseEvent(Event):
    pass


class CargoDropoffOpenEvent(Event):
    pass


class CargoDropoffCloseEvent(Event):
    pass


class TruckUnloadedEvent(Event):
    truck = None
    origin_terminal = None
    # NOTE: time when truck arrived = time - unload_time
    unload_time = None


class TruckReadyToLoadEvent(Event):
    truck = None
    # NOTE: time when truck leaves = time + load_time
    destination_terminal = None
    load_time = None


class Schedule:
    terminal_events = []
    # TODO: store cargo s.t. it is easy to retrieve available cargo in terminal based on time or cargo on truck based on time,
    # and easy to change/remove/add cargo also check for cargo weight, size

    def get_number_of_deliveries(self):
        pass
