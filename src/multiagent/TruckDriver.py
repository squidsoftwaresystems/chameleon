from enum import Enum
from dataclasses import dataclass

from Container import *

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
