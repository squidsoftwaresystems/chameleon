from enum import IntEnum

# NOTE: use this instead of None for trucks or cargo to avoid
# having to store None as NaN in pandas
# (and so converting ids to floats)
INVALID_ID = -1


class TruckEvent(IntEnum):
    DELIVERY_END = 0
    DELIVERY_START = 1
