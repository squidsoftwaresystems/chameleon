from src.multiagent import Container
from src.multiagent import TruckDriver


## -------------------------------------------------------------------


class Graph:

  ## Initialise graph with TruckDrivers as nodes, where neighbours share a preference for a container
  def __init__(self, truckdrivers, containers, locations=None):
    self.nodes = set()
    self.edges = set()
    self.locations = locations or {}

    ## Create Container objects from container data
    container_objs = set()
    for c in containers:

      # Extract basic container data
      c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing = c[:9]

      # Extract additional data if available
      pickup_loc_name = c[9] if len(c) > 9 else None
      delivery_loc_name = c[10] if len(c) > 10 else None
      journey_duration = c[11] if len(c) > 11 else None
      journey_distance = c[12] if len(c) > 12 else None
      route_countries = c[13] if len(c) > 13 else []
      pickup_day = c[14] if len(c) > 14 else None

      # Get location objects if names are provided
      pickup_location = self.locations.get(pickup_loc_name) if pickup_loc_name else None
      delivery_location = self.locations.get(delivery_loc_name) if delivery_loc_name else None

      containerObj = Container(c_id, c_type, c_adr, c_weight, first_pickup, last_pickup,
                              delivery_datetime, cargo_opening, cargo_closing, pickup_location, delivery_location, journey_duration, journey_distance, route_countries, pickup_day)

      container_objs.add(containerObj)

    ## Create TruckDrivers objects from truckdriver data
    for td in truckdrivers:

      # Extract basic truck driver data
      t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, loading_capacity = td[:7]

      # Extract additional data if available
      has_sleeping_cabin = td[7] if len(td) > 7 else False
      has_obu = td[8] if len(td) > 8 else False
      driver_preferences = td[9] if len(td) > 9 else {}

      tdObj = TruckDriver(t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, loading_capacity,
                          has_sleeping_cabin, has_obu, driver_preferences)

      # Add rest periods if provided
      if len(td) > 10 and td[10]:
        for rest_period in td[10]:
          tdObj.add_rest_period(rest_period[0], rest_period[1], rest_period[2])

      (self.nodes).add(tdObj)
      tdObj.choices(container_objs)


    # Build the neighbour graph
    for node in self.nodes:
      node.find_neighbours()
      for neighbour in node.neighbours:
        (self.edges).add((node, neighbour))
