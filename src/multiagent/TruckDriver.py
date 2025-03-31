from enum import Enum
from Container import *


## -------------------------------------------------------------------


class State(Enum):
    IDLE = 1
    ACTIVE = 2
    HOLD = 3
    DONE = 4


## -------------------------------------------------------------------


class TruckDriver:

  ## Initialise TruckDriver object
  def __init__(self, t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, loading_capacity):
    self.id = (t_id, d_id)
    self.adr = t_adr and d_adr
    self.lzv = t_lzv and d_lzv
    self.cap = loading_capacity
    self.prefs = []                                  ## container preferences

    self.neighbours = set()                          ## neighbouring TruckDrivers

    self.neighbourStates: dict[TruckDriver, Enum] = {}          # states of neighbours
    self.context: dict[TruckDriver, Container] = {}             # cpa values
    self.currentState: Enum = State.IDLE                        # agent state
    self.uniquenessBound: int = 1
    self.X = None                                               # final assignment


  ## UpdState Message
  def updState(self, agent, state, solver):
    self.neighbourStates.update({agent : state})
    idlActNeighbours = {k : v for k, v in agent.neighbourStates.items() if v != State.DONE or v != State.HOLD}
    #idlActNeighbours = {k : v for k, v in agent.neighbourStates.items() if v == State.IDLE or v == State.ACTIVE}
    if state == State.HOLD and self.currentState == State.HOLD and len(idlActNeighbours) == 0:
      self.uniquenessBound += 1
      ### REPEAT ALGORITHM
      solver.solveNode(self)
    elif state == State.DONE and self.currentState == State.HOLD:
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
  def costMsg(self, theta):                 # for clarity at the moment, but unecessary
    return theta


  ## SetVal Message
  def setVal(self, agent, value):
    self.context.update({agent : value})              # value is a container


  ## Format TruckDriver as a string
  def __str__(self):
    return f"{self.id}\tADR: {self.adr}\tLZV: {self.lzv}\tCapacity: {self.cap}"


  ## Top 10 container preferences for a TruckDriver
  def choices(self, containers):
    top = []
    for c in containers:
      util = self.utility(c)
      if util[0] != float('-inf'):
        top.append(util)

    top.sort(key=lambda x: x[0], reverse=True)
    top = top[:10]

    for i in top:         # add TruckDriver to bidders for a particular container
      i[2].biddingTDs.append(self)

    self.prefs = top


  ## Calculate utility function, returns utilities of all containers to a TruckDriver
  def utility(self, c):
    score = float('-inf')

    if c.weight <= self.cap:
      if not (c.adr and not self.adr):      # don't consider ADR container with non-ADR truck
        # Now the container is feasible, let's calculate the score
        score = 0

        if c.adr and self.adr:
          score += 1000                    # ADR containers particularly valuable to ADR trucks

        days_until_due = c.delivery
        score += (max(0, 30 - days_until_due) * 10)

        pickup_window_size = c.pickup[1] - c.pickup[0]
        score += pickup_window_size * 5

        cargo_window_size = c.cargo[1] - c.cargo[0]
        score += cargo_window_size * 5

        score -= c.cargo[1]*5

        # distance_penalty = distance * 0.1
        # score -= distance_penalty
        # if route_crosses_border and not truck.has_obu:
        #     score -= 500

        # if route_requires_overnight(container) and not truck.has_sleeping_cabin:
        #     score -= 300

        # if container.is_lzv and truck.lzv:
        #     score += 100

    return (score, c.id, c)


  ## Find neighbours by looking at containers in top 10 (or fewer) preferences and seeing which
  ## other TruckDrivers are bidding for it
  def find_neighbours(self):
    for c in self.prefs:
      for td in c[2].biddingTDs:
        if td.id != self.id:                # compare ids, rather than objects (AMEND LATER?)
          (self.neighbours).add(td)
