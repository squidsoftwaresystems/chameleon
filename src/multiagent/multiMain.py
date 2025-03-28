from enum import Enum
import random


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
    self.prefs = []                                  ## Container preferences

    self.neighbours = set()                          ## Neighbouring TruckDrivers

    self.neighbourStates: dict[TruckDriver, Enum] = {}          # states
    self.context: dict[TruckDriver, Container] = {}             # values
    self.currentState: Enum = State.IDLE                        # state
    self.uniquenessBound: int = 1
    self.X = None                                               # final assignment


  ## UpdState Message
  def updState(self, agent, state, solver):
    self.neighbourStates.update({agent : state})
    idlActNeighbours = {k : v for k, v in agent.neighbourStates.items() if v == State.IDLE or v == State.ACTIVE}
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
    for p in agent.prefs:         # For each assignment of the central node from its prefs, check
      maxUtility = 0              # maxUtility of all feasible assignments to the neighbouring nodes
      container = p[2]            # (return 0 if no feasible assignment)
      for q in self.prefs:
        q_container = q[2]
        if q_container.id != container.id and q_container.id not in [cid.id for cid in cpa.values()]:
        # container not assigned to another TruckDriver
          util = agent.utility(q_container)[0]
          if util > maxUtility:
            maxUtility = util
      theta.update({container : maxUtility})
      #print(p)
    return self.costMsg(theta)                    # returns maxUtility for this object, for each agent assignment




  ## CostMsg Message
  def costMsg(self, theta):                     # For clarity at the moment, but unecessary here
    return theta


  ## SetVal Message
  def setVal(self, agent, value):
    self.context.update({agent : value})                    # value is a container


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

      for i in top:  ## Add TruckDriver to list of those bidding for a particular container
        i[2].biddingTDs.append(self)

      self.prefs = top



  ## Calculate utility function, returns utilities of all containers to a TruckDriver
  def utility(self, c):
    score = float('-inf')

    if c.weight <= self.cap:
      if not (c.adr and not self.adr):      # Don't consider ADR container with non-ADR truck
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
        if td.id != self.id:                # Compare ids, rather than objects
          (self.neighbours).add(td)



## -------------------------------------------------------------------


class Container:

  ## Initialise Container object (includes an implicit journey)
  def __init__(self, c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing):
    self.id = c_id
    self.type = c_type
    self.adr = c_adr
    self.weight = c_weight
    self.pickup = (first_pickup, last_pickup)
    self.delivery = delivery_datetime
    self.cargo = (cargo_opening, cargo_closing)
    self.biddingTDs = []

  def __str__(self):
    return f"{self.id}\tType: {self.type}\tWeight: {self.weight}\tADR: {self.adr}"


## -------------------------------------------------------------------


class CoCoASolver:

  def __init__(self, graph):
    self.graph = graph
    self.agents = graph.nodes
    # agents = graph.nodes

    ## MUST CHECK LATER THAT ALL CONNECTED COMPONENTS HAVE BEEN COVERED - loop through agent states
    for agent in self.agents:

      agent.X = None                      # reset value to NONE and state to IDLE

      if not agent.neighbours:            # if the TruckDriver has no neighbours
        if agent.prefs:                   # assign it its top container preference if it has any
          agent.X = agent.prefs[0][2]     # prefs may be empty if weight/ADR etc. don't work at all
        agent.currentState = State.DONE

      else:
        agent.neighbourStates = {}
        agent.context = {}
        agent.currentState = State.IDLE
        agent.uniquenessBound = 1


  def checkDone(self, agents):
    for agent in agents:
      if agent.currentState != State.DONE:
        return (False, agent)
    return (True, None)


  def solve(self):
    done, toDo = self.checkDone(self.agents)        # Graph may have multiple connected components
    while not done:              # TruckDriver that is not in DONE state
      self.solveNode(toDo)
      #-- anything to add?
      done, toDo = self.checkDone(self.agents)


  def solveNode(self, agent):     # assume non-singleton, and at least one assignment exists

    assert(agent.currentState == State.IDLE or agent.currentState == State.HOLD)
    agent.currentState = State.ACTIVE

    for n in agent.neighbours:
      n.updState(agent, State.ACTIVE, self)

    thetas = []                      # List of dictionaries returned by CostMsg
    for n in agent.neighbours:
      cost_map = n.inqMsg(agent, agent.context)
      thetas.append(cost_map)

    sumThetas = {}
    print(agent)
    print(thetas)
    for k in (thetas[0]).keys():
      sum = 0
      for t in thetas:
        sum += t.get(k)
      sumThetas.update({k : sum})
    maxThetas = {k : v for k, v in sumThetas.items() if v == max(sumThetas.values())}

    idlActNeighbours = {k : v for k, v in agent.neighbourStates.items() if v == State.IDLE or v == State.ACTIVE}
    if len(maxThetas) < agent.uniquenessBound or idlActNeighbours == 0:
      agent.X = random.choice(maxThetas)
      agent.currentState = State.DONE
      for n in agent.neighbours:
        n.updState(agent, State.DONE, self)
        n.setVal(agent, agent.X)
    else:
      agent.currentState = State.HOLD
      for n in agent.neighbours:
        n.updState(agent, State.HOLD, self)


## -------------------------------------------------------------------


class Graph:

  ## Initialise graph with TruckDrivers as nodes, where neighbours share a preference for a container
  def __init__(self, truckdrivers, containers): ## N.B. Info about truck in corresponding driver API
    self.nodes = set()
    self.edges = set()
    ## Create Container objects from 2D array
    ## containers = [[c_id, c_type, c_adr, c_weight, first_pickup, last_pickup, delivery_datetime, cargo_opening, cargo_closing], ...]
    container_objs = set()
    for c in containers:
      container_objs.add(Container(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8]))
    ## Create TruckDrivers objects from 2D array and add each to nodes set
    ## truckdrivers = [[t_id, d_id, t_adr, d_adr, t_lzv, d_lzv, loading_capacity], ...]
    for td in truckdrivers:
      tdObj = TruckDriver(td[0], td[1], td[2], td[3], td[4], td[5], td[6])
      (self.nodes).add(tdObj)
      tdObj.choices(container_objs)
    for node in self.nodes:
      node.find_neighbours()
      for neighbour in node.neighbours:
        (self.edges).add((node, neighbour))


===================================================================

truckdrivers = [
  [10, 11, True, False, True, True, 26500],
  [20, 21, True, True, True, False, 26500],
  [30, 31, False, True, False, True, 22000],
  [40, 41, False, False, False, False, 18000],
  [50, 51, False, False, False, False, 10]
]

containers = [
  [1, "20HC", True, 21000, 2, 5, 4, 9, 15],
  [2, "20HC", False, 19000, 0, 3, 1, 4, 7],
  [3, "20HC", False, 20100, 10, 15, 13, 15, 18],
  [4, "20HC", False, 22000, 1, 8, 9, 9, 15],
  [5, "20HC", False, 17000, 8, 9, 9, 13, 15],
  [6, "20HC", False, 21560, 13, 28, 25, 27, 28],
  [7, "20HC", True, 16780, 9, 11, 12, 12, 14],
  [8, "20HC", False, 21000, 0, 4, 2, 6, 19],
  [9, "40HC", False, 19001, 1, 5, 4, 7, 9],
  [10, "40HC", False, 24000, 1, 6, 7, 8, 11],
  [11, "40HC", False, 26000, 1, 7, 6, 19, 25],
  [12, "40HC", False, 25888, 6, 8, 9, 10, 11],
  [13, "40HC", False, 23008, 3, 7, 6, 9, 13],
  [14, "40HC", False, 27000, 5, 6, 8, 13, 17],
  [15, "40HC", True, 26000, 2, 9, 8, 12, 14],
  [16, "40HC", False, 25500, 4, 8, 8, 10, 15]
]

g = Graph(truckdrivers, containers)

for n in g.nodes:
  print(n)
  print("---")
  for p in n.prefs:
    print(p)
  print({m.id for m in n.neighbours})
  #for m in n.neighbours:
  #  print(m.id)
  print("====\n")

for e in g.edges:
  print(e[0].id, e[1].id)

cocoa = CoCoASolver(g)

for n in g.nodes:
  print(n.currentState)
  print(n.X)
  print(n.neighbourStates)

#cocoa.solve()
