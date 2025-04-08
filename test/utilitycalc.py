import datetime as dt
from src.api import SquidAPI
from copy import copy
import logging

log = logging.getLogger("log_metaheuristic")
logging.basicConfig(level=logging.DEBUG)
api = SquidAPI()

CONFIG = {
    "max_penalty": 1000,
    "weekend_scale": 0.8,
    "weekday_scale": 0.3,
    "error_check": 3,
}

def travel_length(start, end):
    res = api.getCodeDist([start, end])
    return dt.timedelta(seconds=res["duration"])

class Order:
    # Instantiate a new Order object with the given attributes
    def __init__(self, name, pickup_location, delivery_location, pickup_start, pickup_end, delivery_start, delivery_end, weight, adr):
        self.name = name
        self.pickup_location = pickup_location
        self.delivery_location = delivery_location
        self.pickup_start = pickup_start
        self.pickup_end = pickup_end
        self.delivery_start = delivery_start
        self.delivery_end = delivery_end
        self.weight = weight
        self.adr = adr
        # not yet taken or delivered, as it is a new order
        self.taken = False
        self.delivered = False


    def utility(self, datetime: dt.datetime):
        # Get the score for the delivery given the target delivery time - lower is better
        def penalty(current_datetime: dt.datetime, target_datetime: dt.datetime):
            if current_datetime > target_datetime:
                # Deadline has passed, so penalize by the maximum
                return CONFIG["max_penalty"]
            # Number of full or partial days until the target date
            n_days = (target_datetime - current_datetime).days + 1
            current_day = current_datetime.weekday()
            # Count how many weekends until the target date
            n_weekends = (n_days//7)*2 + (max(n_days % 7, 1) if current_day == 6 else max(current_day + n_days%7 - 6, 2))
            return CONFIG["max_penalty"]*pow(CONFIG["weekend_scale"], n_weekends)*pow(CONFIG["weekday_scale"], n_days - n_weekends)

        # Score based on pickup time
        if not self.taken:
            score = penalty(datetime, self.pickup_end)
            log.debug(f"{self.name} | Not taken today: score {score}")
        # Score based on delivery time
        elif not self.delivered:
            score = penalty(datetime, self.delivery_end)
            log.debug(f"{self.name} | Not delivered today: score {score}")
        # Score 0, as it has already been delivered
        else:
            score = 0
            log.debug(f"{self.name} | Delivered today: score 0")
        return score

# Stop datatype, used to store information about the stops chosen by the algorithm
class Stop:
    def __init__(self, order: Order, datetime: dt.datetime, stop_type: str):
        self.order = order
        self.datetime = datetime
        self.stop_type = stop_type

class Truck:
    # Instantiate a new Truck object with the given parameters
    def __init__(self, name, teu, weight, adr, start_time, start_location):
        self.name = name
        self.max_teu = teu
        self.teu_open = copy(self.max_teu)
        self.max_weight = weight
        self.weight_open = copy(self.max_weight)
        self.adr = adr
        # List of stops serviced by the truck - will be filled during the utility() function.
        self.stops = []

        # Total time from start time to last delivery
        self.work_length = dt.timedelta(0)
        # Total time spent waiting between deliveries
        self.idle_length = dt.timedelta(0)
        # Time at which the truck stopped last - used for calculating the above
        self.last_stop_datetime = start_time
        # Truck's current location
        self.location = start_location

        # Current deliveries being held by the truck
        self.inventory = []

    def pickup(self, order: Order, datetime: dt.datetime):
        # Error checking for TEU, weight, and ADR
        if self.teu_open == 0 and CONFIG["error_check"] >= 2:
            log.error(f"{self.name} || Capacity error - not enough TEU open")
        if self.weight_open < order.weight and CONFIG["error_check"] >= 2:
            log.error(f"{self.name} || Capacity error - not enough weight open")
        if not self.adr and order.adr and CONFIG["error_check"] >= 2:
            log.error(f"{self.name} || ADR error")

        # Calculate delivery time and check that it is valid
        delta = datetime - self.last_stop_datetime
        leg_length = travel_length(self.location, order.pickup_location)
        log.debug(f"{self.location} -> {order.pickup_location} in {delta.seconds//60}m")
        if leg_length.total_seconds() > delta.total_seconds() and CONFIG["error_check"] >= 1:
            log.error(f"{self.name} || Travel time error - pickup")
            print(f"Error: {leg_length.total_seconds() // 60}min (travel time) > {delta.total_seconds() // 60}min (delivery time)")

        self.inventory.append(order)
        self.teu_open -= 1
        self.weight_open -= order.weight

        self.work_length += delta
        self.idle_length += (delta - leg_length) * self.max_teu + (delta * max(self.teu_open, 0))
        self.location = order.pickup_location
        self.last_stop_datetime = datetime
        return True

    def deliver(self, order: Order, datetime: dt.datetime):
        if not order in self.inventory and CONFIG["error_check"]:
            log.error(f"{self.name} || Delivery error - order not present in inventory")

        # Calculate delivery time and check that it is valid.
        delta = datetime - self.last_stop_datetime
        leg_length = travel_length(self.location, order.delivery_location)
        log.debug(f"{self.location} -> {order.delivery_location} in {delta.seconds // 60}m")
        if leg_length > delta and CONFIG["error_check"]:
            log.error(f"{self.name} || Travel time error - delivery")
            print(f"Error: {leg_length.seconds // 60}min (travel time) > {delta.seconds // 60}min (delivery time)")

        self.inventory.remove(order)
        self.teu_open += 1
        self.weight_open += order.weight

        self.work_length += delta
        self.idle_length += (delta - leg_length) * self.max_teu + (delta * max(self.teu_open - 1, 0))
        self.location = order.delivery_location
        self.last_stop_datetime = datetime
        return True

    def utility(self):
        self.stops.sort(key=lambda stop: stop.datetime)
        for stop in self.stops:
            if stop.stop_type == "pickup":
                self.pickup(stop.order, stop.datetime)
            else:
                self.deliver(stop.order, stop.datetime)

        if self.work_length.total_seconds() // 60 > 540 and CONFIG["error_check"] >= 2:
            log.error(f"{self.name} || Work time error - {self.work_length.total_seconds()//60 - 540} extra minutes")

        penalty_length = self.idle_length + (dt.timedelta(minutes=540) - self.work_length) * (self.max_teu/2)
        log.info(f"{self.name} | Idle score: {penalty_length.total_seconds() // 60}")
        return penalty_length.total_seconds() // 60

def add_stop(truck: Truck, stop: Stop):
    truck.stops.append(stop)
    if stop.stop_type == "pickup":
        stop.order.taken = True
    else:
        stop.order.delivered = True

def utility(order_list, truck_list, stop_list):
    for truck, stop in stop_list:
        add_stop(truck, stop)
    total_utility = 0
    for truck in truck_list:
        total_utility += truck.utility()
    for order in order_list:
        total_utility += order.utility(dt.datetime(2025, 3, 25, tzinfo=dt.timezone.utc))
    log.info(f"Total score: {total_utility}")
    return total_utility


