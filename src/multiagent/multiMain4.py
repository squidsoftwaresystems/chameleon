from enum import Enum
import random
from dataclasses import dataclass
from typing import List, Dict, Tuple, Set, Optional


## -------------------------------------------------------------------


class State(Enum):
    IDLE = 1
    ACTIVE = 2
    HOLD = 3
    DONE = 4


## -------------------------------------------------------------------


@dataclass
class Location:
  name: str
  country: str
  opening_time: int  # Time in hours (0-23)
  closing_time: int  # Time in hours (0-23)

@dataclass
class RestPeriod:
  start_time: int  # Time in hours
  end_time: int    # Time in hours
  day: int         # Day number


## -------------------------------------------------------------------


class TruckDriver:

  ## Initialise TruckDriver object
  def __init__(self, t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, loading_capacity, has_sleeping_cabin=False, has_obu=False, driver_preferences=None):

    self.id = (t_id, d_id)
    self.adr = t_adr and d_adr
    self.lzv = t_lzv and d_lzv
    self.cap0 = loading_capacity
    self.cap = loading_capacity
    self.prefs = []                        # Container preferences

    # New attributes
    self.has_sleeping_cabin = has_sleeping_cabin
    self.has_obu = has_obu
    self.driver_preferences = driver_preferences or {}   # Dictionary for time windows, routes, etc.
    self.rest_periods = []          # List of RestPeriod objects

    # Track container assignments
    self.available_slots = self.calculate_available_slots()
    self.assigned_containers = []

    self.neighbours = set()          # Neighbouring TruckDrivers

    self.neighbourStates: dict[TruckDriver, Enum] = {}      # States
    self.context: dict[TruckDriver, Container] = {}         # Values
    self.currentState: Enum = State.IDLE                    # State
    self.uniquenessBound: int = 1
    self.X = None                  # Current container assignment


  ## Calculate available slots based on truck type
  def calculate_available_slots(self):
    if self.lzv:
      return {"20ft": 3, "40ft": 1}   # LZV can take 3x20ft or 1x40ft+1x20ft
    else:
      return {"20ft": 2, "40ft": 1}   # Non-LZV can take 2x20ft or 1x40ft


  ## Add a rest period for the driver
  def add_rest_period(self, start_time, end_time, day):
    self.rest_periods.append(RestPeriod(start_time, end_time, day))


  ## Determine if a journey requires overight stay
  def requires_overnight(self, journey_duration):
    return journey_duration > 10      # Assuming journeys > 10 hours requires overnight


  ## Check if journey conflicts with driver rest periods
  def conflicts_with_rest_periods(self, pickup_day, pickup_time, journey_duration):
    journey_end_day = pickup_day
    journey_end_time = pickup_time + journey_duration

    # Handle journey spanning multiple days
    while journey_end_time >= 24:
      journey_end_day += 1
      journey_end_time -= 24

    for rest in self.rest_periods:
      if rest.day == pickup_day and rest.start_time <= pickup_time < rest.end_time:
        return True     # Journey starts during a rest period

      if rest.day == journey_end_day and rest.start_time < journey_end_time <= rest.end_time:
        return True

      # Check if rest period falls within journey
      if (pickup_day < rest.day < journey_end_day) or \
         (pickup_day == rest.day and journey_end_day == rest.day and
          pickup_time <= rest.start_time and journey_end_time >= rest.end_time):
         return True

    return False


  ## Check if pickup and delivery can be done within port opening times
  def fits_port_times(self, pickup_loc, delivery_loc, pickup_day, pickup_time, journey_duration):
    if not pickup_loc or not delivery_loc:
      return True     # No location data available, assume it fits

    if not (pickup_loc.opening_time <= pickup_time < pickup_loc.closing_time):
      return False

    # Calculate delivery time
    delivery_time = (pickup_time + journey_duration) % 24
    #delivery_day = pickup_day + ((pickup_time + journey_duration) // 24)

    # Check if delivery is within port opening hours
    if not (delivery_loc.opening_time <= delivery_time < delivery_loc.closing_time):
      return False

    return True


  ## Check if the container type is compatible with available truck slots
  def check_slot_compatibility(self, container_type, container_weight):

    # Weight check
    if container_weight > self.cap:
      return False

    # Slot availability check
    if container_type == "40HC":
      return self.available_slots["40ft"] > 0
    elif container_type == "20HC":
      return self.available_slots["20ft"] > 0
    return False


  ## Check if container can be assigned to this truck
  def can_assign_container(self, container):
    # Check ADR compatibility
    if container.adr and not self.adr:
      return False

    # Check weight and slot compatibility
    return self.check_slot_compatibility(container.type, container.weight)


  ## UpdState Message
  def updState(self, agent, state, solver):
    # Update neighbour state
    self.neighbourStates.update({agent : state})

    idlActNeighbours = {k : v for k, v in agent.neighbourStates.items() if v != State.HOLD and v != State.DONE}

    if self.currentState == State.HOLD:
      if state == State.HOLD and len(idlActNeighbours) == 0:
        self.uniquenessBound += 1
        ### REPEAT ALGORITHM
        solver.solveNode(self)
      elif state == State.DONE:
        ### REPEAT ALGORITHM
        solver.solveNode(self)


  ## InqMsg Message
  def inqMsg(self, agent, cpa):
    theta = {}
    cpa_ids = [cid.id for cid in cpa.values()]
    for p in agent.prefs:         # For each assignment of the central node from its prefs, check
      maxUtility = 0              # maxUtility of all feasible assignments to the neighbouring nodes
      container = p[2]            # (return 0 if no feasible assignment)
      for q in self.prefs:
        q_container = q[2]
        if q_container.id != container.id and q_container.id not in cpa_ids:
        # container not assigned to another TruckDriver
          util = (self.utility(q_container))[0]
          if util > maxUtility:
            maxUtility = util
      theta.update({container : maxUtility})
    return self.costMsg(theta)       # returns maxUtility for this object, for each agent assignment


  ## CostMsg Message
  def costMsg(self, theta):                     # For clarity at the moment, but unecessary
    return theta


  ## SetVal Message
  def setVal(self, agent, value):
    self.context.update({agent : value})                    # value is a container


  ## Format TruckDriver as a string
  def __str__(self):
    return f"{self.id}\tADR: {self.adr}\tLZV: {self.lzv}\tCapacity: {self.cap0}\t\tRemaining Capacity: {self.cap}"

    #cabins = "With Sleeping Cabin" if self.has_sleeping_cabin else "No Sleeping Cabin"
    #obu = "With OBU" if self.has_obu else "No OBU"
    #return f"{self.id}\tADR: {self.adr}\tLZV: {self.lzv}\tCapacity: {self.cap}\t{cabins}\t{obu}"


  ## Top 10 container preferences for a TruckDriver
  def choices(self, containers):
    top = []

    for c in containers:
      # Check basic feasibility
      if self.can_assign_container(c):
        # Calculate utility if feasible
        util = self.utility(c)
        top.append(util)

    top.sort(key=lambda x: x[0], reverse=True)
    top = top[:10]

    for i in top:  ## Add TruckDriver to list of those bidding for a particular container
      i[2].biddingTDs.append(self)

    self.prefs = top


  ## Calculate utility function, returns utilities of all containers to a TruckDriver
  def utility(self, c):
    score = 0

    # ADR bonus
    if c.adr and self.adr:
      score += 1000           # ADR containers particularly valuable to ADR trucks

    # Existing time-based calculations
    days_until_due = c.delivery
    score += (max(0, 30 - days_until_due) * 10)

    pickup_window_size = c.pickup[1] - c.pickup[0]
    score += pickup_window_size * 5

    cargo_window_size = c.cargo[1] - c.cargo[0]
    score += cargo_window_size * 5

    score -= c.cargo[1]*5

    # Distance penalty - break ties if two urgent containers
    if c.journey_distance:
      distance_penalty = c.journey_distance * 0.1
      score -= distance_penalty

    # Overnight journey check (sleeping cabins and OBUs) - could use to narrow domain instead
    if c.journey_duration and self.requires_overnight(c.journey_duration):

      if not self.has_sleeping_cabin:
        score -= 300

      countries = c.route_countries
      if countries:   # Assume only in Netherlands if no route_countries
        if "Germany" in countries or "Belgium" in countries and not self.has_obu:
          score -= 500

    # LZV bonus for appropriate containers
    if c.type == "40HC" and self.lzv:
      score += 100

    # Check if journey conflicts with driver rest periods
    if c.pickup_day is not None and c.pickup[0] is not None and c.journey_duration is not None:
      if self.conflicts_with_rest_periods(c.pickup_day, c.pickup[0], c.journey_duration):
        score -= 200

    # Check port opening/closing times
    if c.pickup_location and c.delivery_location and c.pickup_day is not None \
       and c.pickup[0] is not None:
      if not self.fits_port_times(c.pickup_location, c.delivery_location,
                                  c.pickup_day, c.pickup[0], c.journey_duration):
        score -= 400

    return (score, c.id, c)


  ## Find neighbours by looking at containers in top 10 (or fewer) preferences and seeing which
  ## other TruckDrivers are bidding for it
  def find_neighbours(self):
    for c in self.prefs:
      for td in c[2].biddingTDs:
        if td.id != self.id:                # Compare ids, rather than objects (AMEND LATER?)
          (self.neighbours).add(td)


  ## Assign a container to the truck and update available slots
  def assign_container(self, container):
    container.assigned = True
    self.assigned_containers.append(container)
    if container.type == "40HC":
      self.available_slots["40ft"] -= 1
      # A 40ft container takes up space that could be used by two 20ft containers
      self.available_slots["20ft"] -= 2
    else:     # "20HC"
      self.available_slots["20ft"] -= 1
      if not self.lzv or (self.lzv and self.available_slots["20ft"] == 1):
        self.available_slots["40ft"] = 0
    self.cap -= container.weight

  ## Check if truck has any slots available
  def has_available_slots(self):
    return self.available_slots["40ft"] > 0 or self.available_slots["20ft"] > 0


## -------------------------------------------------------------------


class Container:

  ## Initialise Container object (includes an implicit journey)
  def __init__(self, c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing, pickup_location=None, delivery_location=None, journey_duration=None, journey_distance=None, route_countries=None, pickup_day=None):

    self.id = c_id
    self.type = c_type      # "20HC" or "40HC"
    self.adr = c_adr
    self.weight = c_weight
    self.pickup = (first_pickup, last_pickup)     # Time window for pickup
    self.pickup_day = pickup_day                  # Day of pickup
    self.delivery = delivery_datetime             # Days until delivery
    self.cargo = (cargo_opening, cargo_closing)   # Time window for cargo operations
    self.biddingTDs = []

    # New attributes
    self.pickup_location = pickup_location
    self.delivery_location = delivery_location
    self.journey_duration = journey_duration      # In hours
    self.journey_distance = journey_distance      # In km
    self.route_countries = route_countries or []  # List of countries on the route

    self.assigned = False

  def __str__(self):
    route = ", ".join(self.route_countries) if self.route_countries else "No route info"
    distance = f"{self.journey_distance} km" if self.journey_distance else "Unknown distance"
    duration = f"{self.journey_duration} hours" if self.journey_duration else "Unknown duration"
    return f"{self.id :<5}{'Type: ' :<6}{self.type :<8}{'Weight: ' :<8}{self.weight :<10}{'ADR: ' :<5}{self.adr :<6}{route :<30}{distance :<10}{duration :<10}"


## -------------------------------------------------------------------


class CoCoASolver:

  def __init__(self, graph):
    self.graph = graph
    self.agents = graph.nodes

    for agent in self.agents:
      agent.X = None                      # reset value to NONE and state to IDLE
      agent.assigned_containers = []
      agent.available_slots = agent.calculate_available_slots()

      # If TruckDriver has no neighbours, assign its top preferences directly (if it has them)
      if not agent.neighbours:            # prefs may be empty if weight/ADR etc. don't work at all
        self.assign_preferences_directly(agent)
        agent.currentState = State.DONE
      else:
        agent.neighbourStates = {}
        agent.context = {}
        agent.currentState = State.IDLE
        agent.uniquenessBound = 1


  ## Assign preferred containers directly to agent without neighbours
  def assign_preferences_directly(self, agent):
    for pref in sorted(agent.prefs, key=lambda x: x[0], reverse=True):
      container = pref[2]
      if not container.assigned:
        # and agent.can_assign_container(container):
        # prefs should already be filtered to avoid this
        agent.assign_container(container)
        agent.X = container                   # Set most recent assignment

        # Check if agent is full
        if not agent.has_available_slots():
          break


  def checkDone(self, agents):
    notDone = []
    for agent in agents:
      if agent.currentState != State.DONE:
        notDone.append(agent)
    if notDone:
      return(False, random.choice(notDone))
    else:
      return (True, None)


  def solve(self):
    # Initialise neighbour states
    for agent in self.agents:
      for n in agent.neighbours:
        n.neighbourStates.update({agent : agent.currentState})


    (done, toDo) = self.checkDone(self.agents)
    while not done:              # TruckDriver that is not in DONE state
      self.solveNode(toDo)
      (done, toDo) = self.checkDone(self.agents)


  ## Find an assignment for the given agent
  def solveNode(self, agent):
    assert(agent.currentState == State.IDLE or agent.currentState == State.HOLD)
    agent.currentState = State.ACTIVE

    # Notify neighbours of state change
    for n in agent.neighbours:
      n.updState(agent, State.ACTIVE, self)

    # Collect cost estimates from neighbours
    thetas = []
    for n in agent.neighbours:
      cost_map = n.inqMsg(agent, agent.context)
      thetas.append(cost_map)

    # Add agent's own utilities
    theta = {}
    for p in agent.prefs:
        theta.update({p[2]: p[0]})
    thetas.append(theta)

    """## If no unassigned containers available, mark as DONE
    if not theta:
      agent.currentState = State.DONE
      for n in agent.neighbours:
        n.updState(agent, State.DONE, self)
      return"""

    # Not sure this is possible
    """# If no containers available to assign, mark as DONE or HOLD
    if not any(theta for theta in thetas):
      # If agent has no containers assigned yet, put it on HOLD
      # This allows it to wait for other agents to make decisions
      if not agent.assigned_containers:
        agent.currentState = State.HOLD
        for n in agent.neighbours:
          n.updState(agent, State.HOLD, self)
      else:
        # If agent has at least one container, we can mark it as DONE
        agent.currentState = State.DONE
        for n in agent.neighbours:
          n.updState(agent, State.DONE, self)
      return"""

    # Sum utilities across all thetas
    # Could use original (same)
    sumThetas = {}
    for t in thetas:          # Each item in thetas list should have same keys exactly
      for k, v in t.items():
        if k not in sumThetas:
          sumThetas[k] = 0
        sumThetas[k] += v

    # Not sure about this
    """# If no containers to choose from, mark as DONE or HOLD
      if not sumThetas:
        if not agent.assigned_containers:
          agent.currentState = State.HOLD
        else:
          agent.currentState = State.DONE

        for n in agent.neighbours:
          n.updState(agent, agent.currentState, self)
        return"""

    # Filter sumThetas
    sumThetas = {k : v for k, v in sumThetas.items()
                 if agent.can_assign_container(k) and not k.assigned}

    # Get containers with maximum utility
    maxThetas = {k : v for k, v in sumThetas.items()
                 if v == max(sumThetas.values())}

    idlActNeighbours = {k: v for k, v in agent.neighbourStates.items()
                        if v == State.IDLE or v == State.ACTIVE}

    if maxThetas and (len(maxThetas) < agent.uniquenessBound or len(idlActNeighbours) == 0):

      # Against spirit:
      # Sort containers by ID for consistent tie-breaking
      #sorted_containers = sorted(maxThetas.keys(), key=lambda x: x.id)
      #x = sorted_containers[0]  # Take the container with the lowest ID

      x = random.choice(list(maxThetas.keys()))
      agent.assign_container(x)
      agent.X = x

      """for k in sumThetas.keys():
        print(" = ", k)
      for k in maxThetas.keys():
        print(" - ", k)"""

      # Update state based on whether there are available slots
      if not agent.has_available_slots():
        agent.currentState = State.DONE
      else:
        # If agent still has slots, keep it IDLE to get more assignments
        agent.currentState = State.IDLE
        self.solveNode(agent)

      for n in agent.neighbours:
        n.updState(agent, agent.currentState, self)
        n.setVal(agent, agent.X)

    elif not maxThetas:
      # If no containers to choose from, mark as DONE
      # Already filtered sumThetas, so if no maxThetas then no sumThetas
      agent.currentState = State.DONE
      for n in agent.neighbours:
        n.updState(agent, State.DONE, self)

    else:
      agent.currentState = State.HOLD
      for n in agent.neighbours:
        n.updState(agent, State.HOLD, self)




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


##==========================================================================


## Create test data including the new constraints
def create_test_data():

  # Create location data
  locations = {
    "Rotterdam": Location("Rotterdam", "Netherlands", 8, 20),
    "Antwerp": Location("Antwerp", "Belgium", 7, 19),
    "Amsterdam": Location("Amsterdam", "Netherlands", 8, 18),
    "Hamburg": Location("Hamburg", "Germany", 7, 20),
    "Cologne": Location("Cologne", "Germany", 8, 18)
  }

  # t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, capacity, sleeping cabin, OBU, preferences, rest periods
  truckdrivers = [
    [10, 11, True, False, True, True, 26500, True, True, {}, [(22, 6, 0), (22, 6, 1)]],   # Rest 10pm-6am
    [20, 21, True, True, True, False, 26500, True, False, {}, [(23, 7, 0), (23, 7, 1)]],
    [30, 31, False, True, False, True, 22000, False, True, {}, [(21, 5, 0), (21, 5, 1)]],
    [40, 41, False, False, False, False, 18000, False, False, {}, [(22, 6, 0), (22, 6, 1)]],
    [50, 51, False, False, False, False, 10, True, True, {}, [(23, 7, 0), (23, 7, 1)]]
  ]

  # c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing, pickup_location, delivery_location, journey_duration, journey_distance, route_countries, pickup_day
  containers = [
    [1, "20HC", True, 21000, 2, 5, 4, 9, 15, "Rotterdam", "Amsterdam", 3, 100, ["Netherlands"], 1],
    [2, "20HC", False, 19000, 0, 3, 1, 4, 7, "Rotterdam", "Antwerp", 2, 80, ["Netherlands", "Belgium"], 1],
    [3, "20HC", False, 20100, 10, 15, 13, 15, 18, "Antwerp", "Hamburg", 8, 500, ["Belgium", "Netherlands", "Germany"], 2],
    [4, "20HC", False, 22000, 1, 8, 9, 9, 15, "Hamburg", "Cologne", 5, 400, ["Germany"], 2],
    [5, "20HC", False, 17000, 8, 9, 9, 13, 15, "Amsterdam", "Rotterdam", 2, 100, ["Netherlands"], 3],
    [6, "20HC", False, 21560, 13, 28, 25, 27, 28, "Cologne", "Antwerp", 6, 270, ["Germany", "Belgium"], 3],
    [7, "20HC", True, 16, 9, 11, 12, 12, 14, "Rotterdam", "Hamburg", 9, 520, ["Netherlands", "Germany"], 4],   #16780
    [8, "20HC", False, 21, 0, 4, 2, 6, 19, "Hamburg", "Rotterdam", 10, 520, ["Germany", "Netherlands"], 4], #21000
    [9, "40HC", False, 19001, 1, 5, 4, 7, 9, "Amsterdam", "Cologne", 7, 300, ["Netherlands", "Germany"], 5],
    [10, "40HC", False, 24000, 1, 6, 7, 8, 11, "Antwerp", "Rotterdam", 3, 100, ["Belgium", "Netherlands"], 5],
    [11, "40HC", False, 26000, 1, 7, 6, 19, 25, "Rotterdam", "Hamburg", 12, 520, ["Netherlands", "Germany"], 6],
    [12, "40HC", False, 25888, 6, 8, 9, 10, 11, "Hamburg", "Cologne", 5, 400, ["Germany"], 6],
    [13, "40HC", False, 23008, 3, 7, 6, 9, 13, "Cologne", "Antwerp", 6, 270, ["Germany", "Belgium"], 7],
    [14, "40HC", False, 27000, 5, 6, 8, 13, 17, "Antwerp", "Amsterdam", 4, 180, ["Belgium", "Netherlands"], 7],
    [15, "40HC", True, 26, 2, 9, 8, 12, 14, "Amsterdam", "Hamburg", 9, 520, ["Netherlands", "Germany"], 8],   # 26000
    [16, "40HC", False, 25500, 4, 8, 8, 10, 15, "Hamburg", "Rotterdam", 11, 520, ["Germany", "Netherlands"], 8]
  ]

  return truckdrivers, containers, locations


## -------------------------------------------------------------------


## Run a test with the enhanced logistics model
def run_test():
  truckdrivers, containers, locations = create_test_data()

  print("Initialising logistics optimisation graph...")
  g = Graph(truckdrivers, containers)

  print(f"Generated graph with {len(g.nodes)} truck drivers and {len(containers)} containers")
  print(f"Graph has {len(g.edges)} edges representing competing preferences")

  """for n in g.nodes:
    print(n)
    print("---")
    for p in n.prefs:
      print(p, p[2].type)
    print({m.id for m in n.neighbours})
    for m in n.neighbours:
      print(m.id)
    print("====\n")"""

  """for e in g.edges:
    print(e[0].id, e[1].id)"""

  cocoa = CoCoASolver(g)

  print("\nSolving assignment problem...")
  cocoa.solve()

  """for n in g.nodes:
    print(n)
    print(n.currentState)
    print(n.X)
    print(n.neighbours)
    print(n.neighbourStates)"""

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
