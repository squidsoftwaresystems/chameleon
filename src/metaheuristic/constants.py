from enum import IntEnum

# NOTE: use this instead of None for trucks or cargo to avoid
# having to store None as NaN in pandas
# (and so converting ids to floats)
INVALID_ID = -1


class FixedEvent(IntEnum):
    """
    Events that our plans can't change
    """

    TERMINAL_OPEN = 0
    TERMINAL_CLOSE = 1
    PICKUP_OPEN = 2
    PICKUP_CLOSE = 3
    DROPOFF_OPEN = 4
    DROPOFF_CLOSE = 5


class TruckEvent(IntEnum):
    DELIVERY_END = 0
    DELIVERY_START = 1
