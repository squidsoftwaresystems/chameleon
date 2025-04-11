from typing import List, Dict, Tuple, Set, Optional
from src.multiagent import Graph
from src.multiagent import CoCoASolver
import pandas as pd
from src.api import SquidAPI

class Location:
    def __init__(self, city, country, opening_time, closing_time):
        self.city= city
        self.country = country
        self.opening_time = opening_time
        self.closing_time = closing_time
        
    def __repr__(self):
        return f"Location({self.city}, {self.country}, {self.opening_time}, {self.closing_time})"

def get_API_data():
  api = SquidAPI()

  # LOCATIONS
  # Get location data
  locationsDF = api.getLocations().reset_index().set_index('id', drop=False).rename(columns={'id': 'location_id'})
  locations = {
      row['code']: Location(row['code'], row['country'], row['open_from'], row['open_to']) #i created a class called Location (above) to match the requested output format, you can change this to just be a tuple/array
      for idx, row in locationsDF.iterrows()
  }

  # Final output should be:
  """locations = {
    "Rotterdam": Location("Rotterdam", "Netherlands", 8, 20),
    "Antwerp": Location("Antwerp", "Belgium", 7, 19),
    "Amsterdam": Location("Amsterdam", "Netherlands", 8, 18),
    "Hamburg": Location("Hamburg", "Germany", 7, 20),
    "Cologne": Location("Cologne", "Germany", 8, 18)
  }"""

  # TRUCKDRIVERS
  # Merge data
  trucksDF = api.getTrucks().reset_index().set_index('id', drop=False).rename(columns={'id': 'truck_id'})
  driversDF = api.getDrivers().reset_index().set_index('id', drop=False).rename(columns={'id': 'driver_id'})
  truckDriversDF = pd.merge(trucksDF, driversDF, left_on='truck_id', right_on='truck_id')

  # Select relevant columns
  truckdrivers = truckDriversDF[['truck_id', 'driver_id', 'truck_adr', 'driver_adr', 'truck_lzv', 'driver_lzv', 'loading_capacity', 'sleeping_cabin', 'obu_belgium', 'obu_germany']].values
  truckdrivers = truckdrivers.tolist()
  for item in truckdrivers:
    item[8] = item[8] and item[9]
    item[9] = {}
    item.append([])

  # Final output should be:
  # [[t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, capacity, sleeping cabin, OBU, preferences, rest periods]]
  
  # CONTAINERS
  # Merge data
  bookingsDF = api.getBookings().reset_index().set_index('id').rename(columns={'id': 'booking_id'})
  transportsDF = api.getTransports().reset_index().set_index('id', drop=False).rename(columns={'id': 'transport_id'})
  containersDF = pd.merge(bookingsDF, transportsDF, left_on='container_id', right_on='container_id')

  # Select relevant columns
  containers = []
  for id, row in containersDF.iterrows():
    bookingID = row['booking_id']
    routesDF = api.getRoutesForBooking(bookingID)
    routesWithLocs = pd.merge(routesDF, locationsDF, left_on = 'location_id', right_on='location_id')

    if routesWithLocs.empty:
      continue

    # We will have issues if we are asked to deliver from a location to itself
    assert routesWithLocs.shape[0] > 1

    if pd.isna(row['container_id']):
      continue

    containers.append(
      [
         row['container_id'], #'container_id':
         row['container_type'], #'container_type': 
         row['adr'], #'adr': 
         row['container_weight'], #'container_weight': 
         row['first_pickup'], #'first_pickup': 
         row['last_pickup'], #'last_pickup': 
         row['delivery_datetime'], #'delivery_datetime': 
         row['cargo_opening'], #'cargo_opening': 
         row['cargo_closing'], #'cargo_closing': 
         routesWithLocs.iloc[-1]['location_id'], #'pickup_location': 
         row['delivery_location_id'], #'delivery_location_id': 
         routesWithLocs.iloc[0]['estimated_driving_duration'], #'journey_duration': (do we add the estimated service time?)
         routesWithLocs.iloc[0]['estimated_distance'], #'journey_distance': 
         (routesWithLocs.iloc[-1]['country'], routesWithLocs.iloc[0]['country']), #'route_countries': (i provided this as a tuple with starting country and ending country)
         pd.to_datetime(row['delivery_datetime']).day #'pickup_day': (i interpreted this as the day part of the datetime)
      ]
    )

  # Final output should be:
  # [[c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing, pickup_location, delivery_location, journey_duration, journey_distance, route_countries, pickup_day]]

  return truckdrivers, containers, locations


get_API_data()


# ## -------------------------------------------------------------------


# ## Create test data including the new constraints
# def create_test_data():

#   # Create location data
#   locations = {
#     "Rotterdam": Location("Rotterdam", "Netherlands", 8, 20),
#     "Antwerp": Location("Antwerp", "Belgium", 7, 19),
#     "Amsterdam": Location("Amsterdam", "Netherlands", 8, 18),
#     "Hamburg": Location("Hamburg", "Germany", 7, 20),
#     "Cologne": Location("Cologne", "Germany", 8, 18)
#   }

#   # t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, capacity, sleeping cabin, OBU, preferences, rest periods
#   truckdrivers = [
#     [10, 11, True, False, True, True, 26500, True, True, {}, [(22, 6, 0), (22, 6, 1)]],   # Rest 10pm-6am
#     [20, 21, True, True, True, False, 26500, True, False, {}, [(23, 7, 0), (23, 7, 1)]],
#     [30, 31, False, True, False, True, 22000, False, True, {}, [(21, 5, 0), (21, 5, 1)]],
#     [40, 41, False, False, False, False, 18000, False, False, {}, [(22, 6, 0), (22, 6, 1)]],
#     [50, 51, False, False, False, False, 10, True, True, {}, [(23, 7, 0), (23, 7, 1)]]
#   ]

#   # c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing, pickup_location, delivery_location, journey_duration, journey_distance, route_countries, pickup_day
#   containers = [
#     [1, "20HC", True, 21000, 2, 5, 4, 9, 15, "Rotterdam", "Amsterdam", 3, 100, ["Netherlands"], 1],
#     [2, "20HC", False, 19000, 0, 3, 1, 4, 7, "Rotterdam", "Antwerp", 2, 80, ["Netherlands", "Belgium"], 1],
#     [3, "20HC", False, 20100, 10, 15, 13, 15, 18, "Antwerp", "Hamburg", 8, 500, ["Belgium", "Netherlands", "Germany"], 2],
#     [4, "20HC", False, 22000, 1, 8, 9, 9, 15, "Hamburg", "Cologne", 5, 400, ["Germany"], 2],
#     [5, "20HC", False, 17000, 8, 9, 9, 13, 15, "Amsterdam", "Rotterdam", 2, 100, ["Netherlands"], 3],
#     [6, "20HC", False, 21560, 13, 28, 25, 27, 28, "Cologne", "Antwerp", 6, 270, ["Germany", "Belgium"], 3],
#     [7, "20HC", True, 16, 9, 11, 12, 12, 14, "Rotterdam", "Hamburg", 9, 520, ["Netherlands", "Germany"], 4],   #16780
#     [8, "20HC", False, 21, 0, 4, 2, 6, 19, "Hamburg", "Rotterdam", 10, 520, ["Germany", "Netherlands"], 4], #21000
#     [9, "40HC", False, 19001, 1, 5, 4, 7, 9, "Amsterdam", "Cologne", 7, 300, ["Netherlands", "Germany"], 5],
#     [10, "40HC", False, 24000, 1, 6, 7, 8, 11, "Antwerp", "Rotterdam", 3, 100, ["Belgium", "Netherlands"], 5],
#     [11, "40HC", False, 26000, 1, 7, 6, 19, 25, "Rotterdam", "Hamburg", 12, 520, ["Netherlands", "Germany"], 6],
#     [12, "40HC", False, 25888, 6, 8, 9, 10, 11, "Hamburg", "Cologne", 5, 400, ["Germany"], 6],
#     [13, "40HC", False, 23008, 3, 7, 6, 9, 13, "Cologne", "Antwerp", 6, 270, ["Germany", "Belgium"], 7],
#     [14, "40HC", False, 27000, 5, 6, 8, 13, 17, "Antwerp", "Amsterdam", 4, 180, ["Belgium", "Netherlands"], 7],
#     [15, "40HC", True, 26, 2, 9, 8, 12, 14, "Amsterdam", "Hamburg", 9, 520, ["Netherlands", "Germany"], 8],   # 26000
#     [16, "40HC", False, 25500, 4, 8, 8, 10, 15, "Hamburg", "Rotterdam", 11, 520, ["Germany", "Netherlands"], 8]
#   ]

#   return truckdrivers, containers, locations


# ## -------------------------------------------------------------------


# ## Run a test with the enhanced logistics model
# def run_test():
#   truckdrivers, containers, locations = create_test_data()

#   print("Initialising logistics optimisation graph...")
#   g = Graph(truckdrivers, containers)

#   print(f"Generated graph with {len(g.nodes)} truck drivers and {len(containers)} containers")
#   print(f"Graph has {len(g.edges)} edges representing competing preferences")

#   """for n in g.nodes:
#     print(n)
#     print("---")
#     for p in n.prefs:
#       print(p, p[2].type)
#     print({m.id for m in n.neighbours})
#     for m in n.neighbours:
#       print(m.id)
#     print("====\n")"""

#   """for e in g.edges:
#     print(e[0].id, e[1].id)"""

#   cocoa = CoCoASolver(g)

#   print("\nSolving assignment problem...")
#   cocoa.solve()

#   """for n in g.nodes:
#     print(n)
#     print(n.currentState)
#     print(n.X)
#     print(n.neighbours)
#     print(n.neighbourStates)"""

#   print("\nResults:")
#   for n in g.nodes:
#     print(f"\nTruck Driver: {n}")
#     if n.assigned_containers:
#       print("Assigned containers:")
#       for container in n.assigned_containers:
#          print(f" - {container}")
#     else:
#       print(" - No containers assigned")

#   # Count how many containers were assigned
#   assigned_count = sum(len(node.assigned_containers) for node in g.nodes)
#   print(f"\nTotal assigned containers: {assigned_count}/{len(containers)}")


# if __name__ == "__main__":
#   run_test()
