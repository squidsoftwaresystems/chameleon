from src.multiagent import Graph
from src.multiagent import CoCoASolver
from src.multiagent import Location
import pandas as pd
from src.api import SquidAPI
from datetime import datetime
import sys


## -------------------------------------------------------------------

def get_API_data():
  api = SquidAPI()

  # LOCATIONS
  print("Collating locations...")
  # Get location data
  locationsDF = api.getLocations().reset_index().set_index('id', drop=False).rename(columns={'id': 'location_id'})
  locations = {
      row['code']: Location(row['name'], row['country'], row['open_from'], row['open_to']) 
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
  print("Collating trucks...")
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
  print("Collating containers...")
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

    # STRING TO DATETIME CONVERSIONS, (+ weeding out rows without delivery dates)

    #delivery_datetime (str) to days until delivery (int)
    if not pd.isna(row['delivery_datetime']):
      currentDT = datetime(2025, 3, 28, 0, 0) #a hard coded date/time
      deliveryDT = datetime.strptime(row['delivery_datetime'], '%Y-%m-%dT%H:%M:%S.000Z')
      daysUntilDelivery = (deliveryDT-currentDT).days
    else:
      daysUntilDelivery = sys.maxsize #default value - can be changed
    
    #first_pickup (str) to first_pickupDays
    if not pd.isna(row['first_pickup']):
      first_pickupDT = int(datetime.strptime(row['first_pickup'], '%Y-%m-%dT%H:%M:%S.000Z').timestamp()/86400)
    else:
      first_pickupDT = 0 #default value - can be changed

    #last_pickup (str) to last_pickupDT (datetime)
    if not pd.isna(row['last_pickup']):
      last_pickupDT = int(datetime.strptime(row['last_pickup'], '%Y-%m-%dT%H:%M:%S.000Z').timestamp()/86400)
    else:
      last_pickupDT = sys.maxsize #default value - can be changed

    #cargo_opening (str) to cargo_openingDT (datetime)
    if not pd.isna(row['cargo_opening']):
      cargo_openingDT = int(datetime.strptime(row['cargo_opening'], '%Y-%m-%dT%H:%M:%S.000Z').timestamp()/86400)
    else:
      cargo_openingDT = 0 #default value - can be changed

    #cargo_closing (str) to cargo_closingDT (datetime)
    if not pd.isna(row['cargo_closing']):
      cargo_closingDT = int(datetime.strptime(row['cargo_closing'], '%Y-%m-%dT%H:%M:%S.000Z').timestamp()/86400)
    else:
      cargo_closingDT = sys.maxsize #default value - can be changed

    #Create list of countries on route
    if not routesWithLocs.iloc[-1]['country'] == routesWithLocs.iloc[0]['country']:
      countries = [routesWithLocs.iloc[-1]['country'], routesWithLocs.iloc[0]['country']]
    else:
      countries = [routesWithLocs.iloc[0]['country']]

    containers.append(
      [
         row['container_id'], #'container_id':
         row['container_type'], #'container_type': 
         row['adr'], #'adr': 
         row['container_weight'], #'container_weight': 
         first_pickupDT, #'first_pickup': (int)
         last_pickupDT, #'last_pickup': (int)
         daysUntilDelivery, #'delivery_datetime': (int days until delivery)
         cargo_openingDT, #'cargo_opening': (int)
         cargo_closingDT, #'cargo_closing': (int)
         routesWithLocs.iloc[-1]['location_id'], #'pickup_location': 
         row['delivery_location_id'], #'delivery_location_id': 
         routesWithLocs.iloc[0]['estimated_driving_duration'], #'journey_duration': (do we add the estimated service time?)
         routesWithLocs.iloc[0]['estimated_distance'], #'journey_distance': 
         countries, #'route_countries':
         pd.to_datetime(row['delivery_datetime']).day #'pickup_day': (i interpreted this as the day part of the datetime, should this be day of the week?)
      ]
    )

  print("Done collating data!")
  # Final output should be:
  # [[c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing, pickup_location, delivery_location, journey_duration, journey_distance, route_countries, pickup_day]]

  return truckdrivers, containers, locations


## -------------------------------------------------------------------


## Run a test with the enhanced logistics model
def run_test():
  truckdrivers, containers, locations = get_API_data()

  print("Initialising logistics optimisation graph...")
  g = Graph(truckdrivers, containers)

  print(f"Generated graph with {len(g.nodes)} truck drivers and {len(containers)} containers")
  print(f"Graph has {len(g.edges)} edges representing competing preferences")

  cocoa = CoCoASolver(g)

  print("\nSolving assignment problem...")
  cocoa.solve()

  print("\nResults:")
  for n in g.nodes:
    print(f"\nTruck Driver: {n}")
    if n.assigned_containers:
      print("Assigned containers:")
      for container in n.assigned_containers:
         print(f" - {container}")
    else:
      print(" - No containers assigned")

  # Count how many containers were assigned
  assigned_count = sum(len(node.assigned_containers) for node in g.nodes)
  print(f"\nTotal assigned containers: {assigned_count}/{len(containers)}")


if __name__ == "__main__":
  run_test()
