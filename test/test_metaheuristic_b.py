import datetime as dt

from src.api import SquidAPI
import pandas as pd

from test_metaheuristic import run_simulated_annealing_on_api
from utilitycalc import Order, Truck, Stop, utility
import logging
log = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def to_datetime(time):
    hr = int(str(time)[:2])
    return dt.datetime(2025, 3, 24, hr, tzinfo=dt.timezone.utc)

# Get list of locations - used by get_trucks and get_routes to assign locations to trucks and endpoints of routes
def get_locations(api: SquidAPI):
    raw_locations = api.getLocations()
    num_locations = raw_locations.shape[0]
    return raw_locations

# Get list of trucks, formatted how utilitycalc wants them to be structured
def get_trucks(api: SquidAPI, locations: pd.DataFrame, start_times=True):
    raw_trucks = api.getTrucks()
    raw_truck_starts = api.getTruckStarts().rename(
        columns={
            "location_id": "starting_terminal"
        }
    )
    # Joins both on index by default
    raw_truck_data: pd.DataFrame = raw_trucks.join(raw_truck_starts, how="inner")
    # raw_truck_data = raw_truck_data[raw_truck_data["loading_capacity"] != 0]

    truck_list = []
    for row_id, row in raw_truck_data.iterrows():
        st = to_datetime(row["start_time"]) if start_times else dt.datetime(2025, 3, 24, tzinfo=dt.timezone.utc)
        truck = Truck(row_id, 2 if row["lzv"] == False else 3, row["loading_capacity"], row["adr"], st, locations.loc[[row["starting_terminal"]]]["code"].values[0])
        truck_list.append(truck)
    return truck_list

def get_routes(api: SquidAPI, locations: pd.DataFrame):
    raw_bookings = api.getBookings()
    min_timestamp = pd.Timestamp(pd.to_datetime(0, origin="unix", utc=True))
    max_timestamp = pd.Timestamp.max.tz_localize("UTC")
    raw_bookings.fillna(
        {
            "cargo_opening": min_timestamp,
            "cargo_closing": max_timestamp,
            "first_pickup": min_timestamp,
            "last_pickup": max_timestamp,
        },
        inplace=True,
    )

    # Convert to pd.Timestamp
    column_names = [
        "cargo_opening",
        "cargo_closing",
        "first_pickup",
        "last_pickup",
    ]
    # Convert to UTC, timezone-naive time (this is later converted to aware)
    raw_bookings[column_names] = raw_bookings[column_names].map(pd.to_datetime)

    assert (min_timestamp <= raw_bookings[column_names]).all().all()

    # Remove invalid rows
    raw_bookings = raw_bookings[
        (raw_bookings["cargo_opening"] < raw_bookings["cargo_closing"])
        & (raw_bookings["first_pickup"] < raw_bookings["last_pickup"])
        ]

    # Change the format of bookings and only add ones that
    # have corresponding routes.
    # For now, for the sake of simplicity,
    # we consider the task to be going from the very first
    # waypoint straight to the very last one.
    orders = []
    for booking_id, booking in raw_bookings.iterrows():
        routes = api.getRoutesForBooking(booking_id)
        if routes.empty:
            continue
        # TODO: ignore the deliveries to non-port areas

        # We will have issues if we are asked to deliver from a location to itself
        assert routes.shape[0] > 1

        if pd.isna(booking["container_id"]):
            continue

        # Add new order in the desired format
        orders.append(Order(booking["container_id"], locations.loc[[routes.iloc[-1]["location_id"]]]["code"].values[0],  locations.loc[[routes.iloc[0]["location_id"]]]["code"].values[0],
                            to_datetime(booking["first_pickup"]), to_datetime(booking["last_pickup"]),
                            to_datetime(booking["cargo_opening"]), to_datetime(booking["cargo_closing"]),
                            0, False))

    return orders

def test_meta():
    # Initialize API and log
    logging.basicConfig(filename="testing.log", level=logging.DEBUG)
    api = SquidAPI()

    # Get locations, trucks, and orders
    locations = get_locations(api)
    trucks = get_trucks(api, locations, start_times=False)
    orders = get_routes(api, locations)

    # Get stops chosen by the algorithm, and convert them into the correct format
    _stops = run_simulated_annealing_on_api(seed=2718)
    stops = []
    for stop in _stops:
        order = list(filter(lambda x: x.name == stop[3], orders))[0]
        truck = list(filter(lambda x: x.name == stop[0], trucks))[0]
        time = dt.datetime.fromtimestamp(stop[1], dt.timezone.utc)
        stage = "pickup" if stop[4] else "dropoff"
        stops.append([truck, Stop(order, time, stage)])

    # Calculate and print the score attained by the algorithm (lower is better)
    print(utility(orders, trucks, stops))

if __name__ == '__main__':
    logging.basicConfig(filename="testing.log", level=logging.DEBUG)