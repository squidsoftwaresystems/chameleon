import requests
import pandas as pd
import os
from dotenv import load_dotenv
from typing import Optional, List, Dict, Union
from datetime import date, datetime

load_dotenv()


class SquidAPI:
    """
    API client for interacting with Squid Software's logistics API.
    Handles data fetching, caching, and provides access methods for logistics entities.
    """

    def __init__(self, day: Optional[date] = None):
        # Initialize API credentials and endpoints
        self.__API_KEY = os.getenv("API-KEY")
        if self.__API_KEY is None:
            raise ValueError("API-KEY not found in environment variables")

        self.__API_URL = "https://api.squid.software"
        self.__OSRM_URL = "https://osrm.squid.software"

        # Base path for cached data file storage
        self.__BASEDIR = "./data"
        if not os.path.exists(self.__BASEDIR):
            os.mkdir(self.__BASEDIR)

        # Define paths for cached data files
        self.__LOCATIONPATH = f"{self.__BASEDIR}/locations.csv"
        self.__TRUCKPATH = f"{self.__BASEDIR}/trucks.csv"
        self.__DRIVERPATH = f"{self.__BASEDIR}/drivers.csv"
        self.__SCHEDULEPATH = f"{self.__BASEDIR}/schedules.csv"
        self.__CHASSISPATH = f"{self.__BASEDIR}/chassis.csv"
        self.__BOOKINGSPATH = f"{self.__BASEDIR}/bookings.csv"
        self.__TRANSPORTSPATH = f"{self.__BASEDIR}/transports.csv"
        self.__ROUTESPATH = f"{self.__BASEDIR}/routes.csv"
        self.__TRUCKSTARTSPATH = f"{self.__BASEDIR}/truck_starts.csv"

        # Load or fetch all required data
        self.__fetchLocations()
        self.__fetchTrucks()
        self.__fetchTruckStarts()
        self.__fetchDrivers()
        self.__fetchChassis()
        self.__fetchBookings(day)

    def __api_call(self, endpoint: str, args: Optional[str] = ""):
        """Make a single API request to the specified endpoint"""
        return requests.get(
            f"{self.__API_URL}/{endpoint}?{args}", headers={"Api-Key": self.__API_KEY}
        ).json()

    def __paginated_api_call(self, endpoint: Optional[str], args: Optional[str] = ""):
        """Handle pagination to retrieve complete datasets from the API"""
        all_results = []
        offset = 0
        page_size = 1000

        while True:
            pagination_args = f"offset={offset}"
            if args:
                pagination_args = f"{args}&{pagination_args}"

            results = self.__api_call(endpoint, pagination_args)
            if not results:  # No more results
                break

            all_results.extend(results)
            if len(results) < page_size:  # Last page
                break

            offset += page_size

        return all_results

    def __osrm_call(self, endpoint: str, args: Optional[str] = ""):
        """Make a single API request to the OSRM endpoint"""
        return requests.get(
            f"{self.__OSRM_URL}/{endpoint}?{args}",
            headers={"Api-Key": f"Bearer {self.__API_KEY}"},
        ).json()

    def __fetchLocations(self):
        """Fetch or load location data from cache"""

        # if the data has already been fetched, load it
        if os.path.exists(self.__LOCATIONPATH):
            self.locations = pd.read_csv(self.__LOCATIONPATH)
            self.locations.set_index("id", inplace=True)
        # fetch the data from the API and save it
        else:
            res = self.__paginated_api_call(
                "locations", "exclude_from_planning=false&deleted=false"
            )
            self.locations = pd.DataFrame(res)
            self.locations.set_index("id", inplace=True)
            self.locations.drop(
                labels=[
                    "address",
                    "city",
                    "deleted",
                    "created",
                    "exclude_from_planning",
                    "industry_area",
                    "email",
                    "phone",
                    "remarks",
                    "website",
                    "zip",
                    "updated",
                ],
                axis=1,
                inplace=True,
            )
            self.locations.to_csv(self.__LOCATIONPATH)

    def __fetchTrucks(self):
        """Fetch or load truck data from cache"""

        # if the data has already been fetched, load it
        if os.path.exists(self.__TRUCKPATH):
            self.trucks = pd.read_csv(self.__TRUCKPATH)
            self.trucks.set_index("id", inplace=True)
        # fetch the data from the API and save it
        else:
            res = self.__paginated_api_call(
                "trucks", "deleted=false&road_certified=true"
            )
            self.trucks = pd.DataFrame(res)
            self.trucks.set_index("id", inplace=True)
            self.trucks.drop(
                labels=[
                    "deleted",
                    "created",
                    "remarks",
                    "type",
                    "road_certified",
                    "fuel_consumption",
                    "fuel_type",
                ],
                axis=1,
                inplace=True,
            )
            self.trucks["number"] = self.trucks["number"].astype(int)
            self.trucks["capacity"] = self.trucks["loading_capacity"] - 5000
            self.trucks.to_csv(self.__TRUCKPATH)

    def __fetchTruckStarts(self):
        """
        Load cached truck start locations and times.
        Note that this data is not available via the API,
        so it has to be provided manually.
        """

        # Assume the data does not need to be fetched, it is already saved.
        if os.path.exists(self.__TRUCKSTARTSPATH):
            self.truck_starts = pd.read_csv(self.__TRUCKSTARTSPATH)
            self.truck_starts.set_index("id", inplace=True)
        else:
            # Temporary method to provide an initial setup. This data is not
            # accessible via the API, so has to be provided manually.
            raise ValueError(
                "Truck starts data not found. Please fetch the data first."
            )

    def __processSchedules(self, schedules: List[Dict]):
        """Transform raw schedule data into a usable DataFrame structure"""
        # For each schedule, remove deleted ones, created and updated ids
        # and flatten the inner repeat (if not deleted) object to just the
        # repeat_number, repeat_type, start, end.
        # Output into a dataframe.

        for schedule in schedules:
            # Remove deleted schedules
            if schedule["deleted"]:
                continue

            # Flatten repeat object
            if not schedule["repeat"]["deleted"]:
                schedule["repeat_number"] = schedule["repeat"]["repeat_number"]
                schedule["repeat_type"] = schedule["repeat"]["repeat_type"]
                schedule["repeat_start"] = schedule["repeat"]["start"]
                schedule["repeat_end"] = schedule["repeat"]["end"]
            del schedule["repeat"]

        # Convert to dataframe
        self.schedules = pd.DataFrame(schedules)
        self.schedules.set_index("id", inplace=True)
        self.schedules.drop(
            labels=["created", "updated", "deleted", "description"],
            axis=1,
            inplace=True,
        )
        self.schedules.to_csv(self.__SCHEDULEPATH)

    def __fetchDrivers(self):
        """Fetch or load driver data and their schedules from cache"""

        # if the data has already been fetched, load it
        if os.path.exists(self.__DRIVERPATH):
            self.drivers = pd.read_csv(self.__DRIVERPATH)
            self.drivers.set_index("id", inplace=True)
            return
        # fetch the data from the API and save it
        res = self.__paginated_api_call("drivers", "deleted=false")
        schedules = []

        # Process data to replace nested objects with IDs
        for driver in res:
            if driver.get("truck"):
                # Replace truck object with just the truck ID
                truck_id = driver["truck"].get("id")
                driver["truck"] = truck_id
            if driver.get("home"):
                # Replace home object with just the home ID
                home_id = driver["home"].get("id")
                driver["home"] = home_id
            if driver.get("employer"):
                # Replace employer object with just the employer ID
                employer_id = driver["employer"].get("id")
                driver["employer"] = employer_id

            if driver.get("schedule"):
                # Iterate through the default and deviation schedules, add them to the schedules list
                # with the driver ID and schedule type
                for schedule in driver["schedule"]["default"]:
                    schedule["driver"] = driver["id"]
                    schedule["type"] = "default"
                    schedules.append(schedule)

                for schedule in driver["schedule"]["deviations"]:
                    schedule["driver"] = driver["id"]
                    schedule["type"] = "deviation"
                    schedules.append(schedule)

                # Remove schedules from the driver
                del driver["schedule"]

        self.__processSchedules(schedules)

        self.drivers = pd.DataFrame(res)
        self.drivers.set_index("id", inplace=True)
        self.drivers.drop(
            labels=["deleted", "created"],
            axis=1,
            inplace=True,
        )
        self.drivers.to_csv(self.__DRIVERPATH)

    def __fetchChassis(self):
        """Fetch or load chassis data from cache"""

        # if the data has already been fetched, load it
        if os.path.exists(self.__CHASSISPATH):
            self.chassis = pd.read_csv(self.__CHASSISPATH)
            self.chassis.set_index("id", inplace=True)
        # fetch the data from the API and save it
        else:
            res = self.__paginated_api_call(
                "chassis", "deleted=false&road_certified=true"
            )
            self.chassis = pd.DataFrame(res)
            self.chassis.set_index("id", inplace=True)
            self.chassis.drop(
                labels=[
                    "deleted",
                    "created",
                    "updated",
                    "description",
                    "road_certified",
                ],
                axis=1,
                inplace=True,
            )
            self.chassis.to_csv(self.__CHASSISPATH)

    def __fetchBookings(self, day: Optional[date] = None):
        """
        Fetch or load booking data and related transport/route information.
        Optionally filter by date of first pickup.
        """
        # Check if the data has already been fetched
        if (
            os.path.exists(self.__BOOKINGSPATH)
            and os.path.exists(self.__TRANSPORTSPATH)
            and os.path.exists(self.__ROUTESPATH)
        ):
            self.bookings = pd.read_csv(self.__BOOKINGSPATH)
            self.bookings.set_index("id", inplace=True)
            self.transports = pd.read_csv(self.__TRANSPORTSPATH)
            self.transports.set_index("id", inplace=True)
            self.routes = pd.read_csv(self.__ROUTESPATH)
            self.routes.set_index("id", inplace=True)
            return

        # Fetch the data from the API
        # If day is set, will only fetch bookings which have a
        # first pickup on or after that date
        if day:
            # Convert date to string format for API query
            query_date = day.strftime("%Y-%m-%d")
            res = self.__paginated_api_call(
                "bookings",
                f"deleted=false&first_pickup>{query_date}T00:00:00",
            )
        else:
            res = self.__paginated_api_call("bookings", "deleted=false")

        all_bookings = []
        all_transports = []
        all_routes = []

        for booking in res:
            # Process container information
            if booking.get("container"):
                booking["container_id"] = booking["container"].get("id")
                booking["container_number"] = booking["container"].get("number")
            else:
                booking["container_id"] = None
                booking["container_number"] = None
            # Remove container object from booking
            del booking["container"]

            # Process delivery location information
            if booking.get("delivery_location"):
                booking["delivery_location_id"] = booking["delivery_location"].get("id")
            else:
                booking["delivery_location_id"] = None
            # Remove delivery location object from booking
            del booking["delivery_location"]

            # Flatten import_status fields
            if booking.get("import_status"):
                for key, value in booking["import_status"].items():
                    booking[f"import_{key}"] = value
            else:
                booking["import_status"] = None
            # Remove import_status object from booking
            del booking["import_status"]

            # Flatten export_status fields
            if booking.get("export_status"):
                for key, value in booking["export_status"].items():
                    booking[f"export_{key}"] = value
            else:
                booking["export_status"] = None
            # Remove export_status object from booking
            del booking["export_status"]

            # Process transports
            if booking.get("transports"):
                for transport in booking["transports"]:
                    # Skip deleted transports
                    if transport.get("deleted", True):
                        continue

                    # Rename booking to booking_id
                    transport["booking_id"] = transport.pop("booking")

                    # Process planner information
                    if transport.get("planner"):
                        transport["planner_id"] = transport["planner"].get("id")
                    del transport["planner"]

                    # Process routes
                    if transport.get("route"):
                        for route in transport["route"]:
                            # Skip deleted routes
                            if route.get("deleted", True):
                                continue

                            # Add transport reference
                            route["transport_id"] = transport["id"]

                            # Process location information
                            if route.get("location"):
                                route["location_id"] = route["location"].get("id")
                            del route["location"]

                            all_routes.append(route)

                    # Remove route array from transport
                    del transport["route"]

                    all_transports.append(transport)

            # Remove transports array from booking
            del booking["transports"]

            all_bookings.append(booking)

        # Create dataframes and drop unwanted fields
        if all_bookings:
            self.bookings = pd.DataFrame(all_bookings)
            self.bookings.set_index("id", inplace=True)
            self.bookings.drop(
                columns=["deleted", "created", "updated"], errors="ignore", inplace=True
            )
            self.bookings["first_pickup"] = pd.to_datetime(
                self.bookings["first_pickup"], errors="coerce"
            )
            self.bookings["last_pickup"] = pd.to_datetime(
                self.bookings["last_pickup"], errors="coerce"
            )
            self.bookings["cargo_opening"] = pd.to_datetime(
                self.bookings["cargo_opening"], errors="coerce"
            )
            self.bookings["cargo_closing"] = pd.to_datetime(
                self.bookings["cargo_closing"], errors="coerce"
            )
            self.bookings["delivery_datetime"] = pd.to_datetime(
                self.bookings["delivery_datetime"], errors="coerce"
            )
            self.bookings.to_csv(self.__BOOKINGSPATH)
        else:
            self.bookings = pd.DataFrame()

        if all_transports:
            self.transports = pd.DataFrame(all_transports)
            self.transports.set_index("id", inplace=True)
            self.transports.drop(
                columns=["deleted", "created", "updated"], errors="ignore", inplace=True
            )
            self.transports["start"] = pd.to_datetime(
                self.transports["start"], errors="coerce"
            )
            self.transports["end"] = pd.to_datetime(
                self.transports["end"], errors="coerce"
            )
            self.transports.to_csv(self.__TRANSPORTSPATH)
        else:
            self.transports = pd.DataFrame()

        if all_routes:
            self.routes = pd.DataFrame(all_routes)
            self.routes.set_index("id", inplace=True)
            self.routes.drop(
                columns=[
                    "deleted",
                    "created",
                    "updated",
                    "planned_arrival",
                    "planned_departure",
                    "planned_driving_duration",
                    "planned_service_duration",
                ],
                errors="ignore",
                inplace=True,
            )
            self.routes["estimated_arrival"] = pd.to_datetime(
                self.routes["estimated_arrival"], errors="coerce"
            )
            self.routes["estimated_departure"] = pd.to_datetime(
                self.routes["estimated_departure"], errors="coerce"
            )
            self.routes["planned_arrival"] = pd.to_datetime(
                self.routes["planned_arrival"], errors="coerce"
            )
            self.routes["planned_departure"] = pd.to_datetime(
                self.routes["planned_departure"], errors="coerce"
            )
            self.routes.to_csv(self.__ROUTESPATH)
        else:
            self.routes = pd.DataFrame()

    # LOCATION GETTERS
    def getLocations(self, country: Optional[str] = None, type: Optional[str] = None):
        """Retrieve locations with optional filtering by country or type"""
        ret = self.locations
        if country is not None:
            ret = ret[ret.country == country]
        if type is not None:
            ret = ret[ret.type == type]

        return ret

    def getLocation(self, code: str):
        """Find a location by its unique code"""
        return self.locations.loc[self.locations["code"] == code].iloc[0]

    def getLocationById(self, id: str):
        """Find a location by ID"""
        return self.locations.loc[id]

    # TRUCK GETTERS
    def getTrucks(
        self,
        adr: Optional[bool] = None,
        lzv: Optional[bool] = None,
        capacity: Optional[int] = None,
        obu_germany: Optional[bool] = None,
        obu_belgium: Optional[bool] = None,
        euroorm: Optional[int] = None,
    ):
        """Retrieve trucks with multiple optional filtering parameters"""
        ret = self.trucks
        if adr is not None:
            ret = ret[ret.adr == adr]
        if lzv is not None:
            ret = ret[ret.lzv == lzv]
        if capacity is not None:
            ret = ret[ret.capacity >= capacity]
        if obu_germany is not None:
            ret = ret[ret.obu_germany == obu_germany]
        if obu_belgium is not None:
            ret = ret[ret.obu_belgium == obu_belgium]
        if euroorm is not None:
            ret = ret[ret.euroorm == euroorm]
        return ret

    def getTruck(self, license_plate: str):
        """Find a truck by its license plate"""
        return self.trucks.loc[self.trucks["license_plate"] == license_plate].iloc[0]

    def getTruckById(self, id: str):
        """Find a truck by ID"""
        return self.trucks.loc[id]

    # DRIVER GETTERS
    def getDrivers(
        self,
        available: Optional[bool] = None,
        adr: Optional[bool] = None,
        tank_adr: Optional[bool] = None,
        lzv: Optional[bool] = None,
    ):
        """Retrieve drivers with optional filtering by availability and qualifications"""
        ret = self.drivers
        if available is not None:
            ret = ret[ret.available == available]
        if adr is not None:
            ret = ret[ret.adr == adr]
        if tank_adr != None:
            ret = ret[ret.liquid_adr == tank_adr]
        if lzv is not None:
            ret = ret[ret.lzv == lzv]
        return ret

    def getDriver(self, name: str):
        """Find a driver by name"""
        return self.drivers.loc[self.drivers["name"] == name].iloc[0]

    def getDriverById(self, id: str):
        """Find a driver by ID"""
        return self.drivers.loc[id]

    # GET TRUCK STARTS
    def getTruckStarts(self):
        """Retrieve all truck starts"""
        return self.truck_starts

    def getTruckStartById(self, id: str):
        """Find a truck start by ID"""
        return self.truck_starts.loc[id]

    def getTruckStartByNumber(self, number: int):
        """Find a truck start by truck number"""
        # First, get the id of the truck with that number, then find that id in the truck_starts
        # Remember that id is the index so is not accessible by loc
        truck = self.trucks.index[self.trucks["number"] == number]
        if len(truck) == 0:
            return None
        return self.truck_starts.loc[truck[0]]

    # SCHEDULE GETTERS
    def getSchedules(self):
        """Retrieve all schedules"""
        return self.schedules

    def getSchedulesForDriver(
        self,
        driver_id: str,
        type: Optional[str] = None,
        day: Optional[str] = None,
        date: Optional[str] = None,
    ):
        """Retrieve schedules for a specific driver with optional filters"""
        ret = self.schedules[self.schedules.driver == driver_id]
        if type is not None:
            ret = ret[ret.type == type]
        if day is not None:
            ret = ret[ret.day == day]
        if date is not None:
            # filter if date is between start and end
            ret = ret[(ret.start >= date) & (ret.end <= date)]
        return ret

    def getScheduleById(self, id: str):
        """Find a schedule by ID"""
        return self.schedules.loc[id]

    # CHASSIS GETTERS
    def getChassis(
        self,
        license_plate: Optional[str] = None,
        type: Optional[str] = None,
        owner_id: Optional[str] = None,
        location_id: Optional[str] = None,
        lzv: Optional[bool] = None,
        adr: Optional[bool] = None,
        available: Optional[bool] = None,
    ):
        """Retrieve chassis with multiple optional filtering parameters"""
        ret = self.chassis
        if license_plate is not None:
            ret = ret[ret.license_plate == license_plate]
        if type is not None:
            ret = ret[ret.type == type]
        if owner_id is not None:
            ret = ret[ret.owner_id == owner_id]
        if location_id is not None:
            ret = ret[ret.location_id == location_id]
        if lzv is not None:
            ret = ret[ret.lzv == lzv]
        if adr is not None:
            ret = ret[ret.adr == adr]
        if available is not None:
            ret = ret[ret.available == available]
        return ret

    def getChassisById(self, id: str):
        """Find a chassis by ID"""
        return self.chassis.loc[id]

    def getChassisByLicensePlate(self, license_plate: str):
        """Find a chassis by license plate"""
        return self.chassis.loc[self.chassis["license_plate"] == license_plate].iloc[0]

    # BOOKING GETTERS
    def getBookings(
        self,
        code: str = None,
        container_id: str = None,
        delivery_location_id: str = None,
    ):
        """Retrieve bookings with optional filtering parameters"""
        ret = self.bookings
        if code is not None:
            ret = ret[ret.code == code]
        if container_id is not None:
            ret = ret[ret.container_id == container_id]
        if delivery_location_id is not None:
            ret = ret[ret.delivery_location_id == delivery_location_id]
        return ret

    def getBookingById(self, id: str):
        """Find a booking by ID"""
        return self.bookings.loc[id]

    def getTransports(
        self,
        adr: Optional[bool] = None,
        area: Optional[str] = None,
        modality: Optional[str] = None,
    ):
        """Retrieve transports with optional filtering parameters"""
        ret = self.transports
        if adr is not None:
            ret = ret[ret.adr == adr]
        if area is not None:
            ret = ret[ret.area == area]
        if modality is not None:
            ret = ret[ret.modality == modality]
        return ret

    def getTransportByCode(self, code: str):
        """Find a transport by code"""
        return self.transports.loc[self.transports["code"] == code].iloc[0]

    def getTransportsForBooking(self, booking_id: str):
        """Retrieve all transports associated with a specific booking"""
        return self.transports.loc[self.transports.booking_id == booking_id]

    def getTransportById(self, id: str):
        """Find a transport by ID"""
        return self.transports.loc[id]

    def getRoutesForTransport(self, transport_id: str):
        """Retrieve all routes associated with a specific transport"""
        return self.routes.loc[self.routes.transport_id == transport_id]

    def getRouteById(self, id: str):
        """Find a route by ID"""
        return self.routes.loc[id]

    def getRoutesForBooking(self, booking_id: str) -> pd.DataFrame:
        """Retrieve all routes associated with a specific booking"""
        transports = self.getTransportsForBooking(booking_id)
        routes = []
        for transport in transports.itertuples():
            routes.append(self.getRoutesForTransport(transport.id))
        return pd.concat(routes) if routes else pd.DataFrame()

    def getBookingsByCargoWindow(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Find bookings with cargo windows within the specified time range"""
        return self.bookings[
            (self.bookings.cargo_opening >= start)
            & (self.bookings.cargo_closing <= end)
        ]

    def getBookingsByPickupWindow(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Find bookings with pickup windows within the specified time range"""
        return self.bookings[
            (self.bookings.pickup_opening >= start)
            & (self.bookings.pickup_closing <= end)
        ]

    def getBookingsByCargoDay(self, day: date) -> pd.DataFrame:
        """Find bookings with cargo operations scheduled on a specific day"""
        return self.bookings[
            (self.bookings.cargo_opening.dt.date == day)
            & (self.bookings.cargo_closing.dt.date == day)
        ]

    def getBookingsByPickupDay(self, day: datetime) -> pd.DataFrame:
        """Find bookings with pickups scheduled on a specific day"""
        return self.bookings[
            (self.bookings.pickup_opening.dt.date == day)
            & (self.bookings.pickup_closing.dt.date == day)
        ]

    def getBookingByLocation(self, location_id: str) -> pd.DataFrame:
        """Find bookings for a specific delivery location"""
        return self.bookings[self.bookings.delivery_location_id == location_id]

    def getTransportByPlanner(self, planner_id: str) -> pd.DataFrame:
        """Find transports assigned to a specific planner"""
        return self.transports[self.transports.planner_id == planner_id]

    def getRouteByLocation(self, location_id: str) -> pd.DataFrame:
        """Find routes that include a specific location"""
        return self.routes[self.routes.location_id == location_id]

    def getPortOpenTimes(self) -> pd.DataFrame:
        """Get all port opening and closing times"""
        return self.locations[self.locations.type == "port"].loc[
            :, ["open_from", "open_to"]
        ]

    def getTruckHomeLocations(self) -> pd.Series:
        """Get the home locations of all trucks"""
        return self.trucks[self.trucks.home.notna()]["home"]

    def getPortTransportDetails(self) -> pd.DataFrame:
        """Get all transports for ports"""

        return (
            # Merge transports with bookings using booking_id
            pd.merge(
                # Filter to only include port transports with a planner_id
                self.transports[
                    self.transports.planner_id.notna() & self.transports.area == "port"
                ].loc[:, ["planner_id", "booking_id"]],
                # Extract relevant columns from bookings
                self.bookings.loc[
                    :,
                    [
                        "first_pickup",
                        "last_pickup",
                        "cargo_opening",
                        "cargo_closing",
                        "delivery_datetime",
                    ],
                ],
                left_on="booking_id",
                right_index=True,
            )
        )

    def getCoordDist(
        self, coords: List[List[float]]
    ) -> Optional[Dict[str, Union[float, List[Dict[str, float]]]]]:
        """
        Get the distance between (long,lat) pairs using OSRM

        Returns the total distance and duration of the route, as well as the
        distance and duration of each leg of the journey
        """
        if len(coords) < 2:
            return None
        res = self.__osrm_call("route", f"coordinates={coords}")
        ret = {
            "distance": res["routes"][0]["distance"],
            "duration": res["routes"][0]["duration"],
            "legs": [],
        }
        for leg in res["routes"][0]["legs"]:
            ret["legs"].append(
                {"distance": leg["distance"], "duration": leg["duration"]}
            )
        return ret

    def getCodeDist(
        self, codes: List[str]
    ) -> Optional[Dict[str, Union[float, List[Dict[str, float]]]]]:
        """
        Get the distance between locations using their codes
        """
        coords = []
        for code in codes:
            loc = self.getLocation(code)
            if loc is None:
                return None
            coords.append([loc.longitude, loc.latitude])

        return self.getCoordDist(coords)

    def getTransportDist(
        self, id: str
    ) -> Optional[Dict[str, Union[float, List[Dict[str, float]]]]]:
        """
        Get the distance of a transport using its ID
        """
        coords = []
        route_stops = []
        # Get all routes for the transport
        for _, route in self.getRoutesForTransport(id).iterrows():
            route_stops.append([route["index"], route.location_id])

        # Sort routes by index
        route_stops = sorted(route_stops, key=lambda x: x[0])

        # Get the coordinates for each location in the route
        for _, loc_id in route_stops:
            loc = self.getLocationById(loc_id)
            if loc is None:
                return None
            coords.append([loc.longitude, loc.latitude])

        return self.getCoordDist(coords)

    def getCoordMatrix(
        self, coords: List[List[float]]
    ) -> Optional[Dict[str, List[List[float]]]]:
        """
        Get the distance matrix between (long,lat) pairs using OSRM
        """
        if len(coords) < 2:
            return None
        res = self.__osrm_call("table", f"coordinates={coords}")
        return {
            "distances": res["distances"],
            "durations": res["durations"],
        }

    def getCodeMatrix(self, codes: List[str]) -> Optional[Dict[str, List[List[float]]]]:
        """
        Get the distance matrix between locations using their codes
        """
        coords = []
        for code in codes:
            loc = self.getLocation(code)
            if loc is None:
                return None
            coords.append([loc.longitude, loc.latitude])

        return self.getCoordMatrix(coords)
