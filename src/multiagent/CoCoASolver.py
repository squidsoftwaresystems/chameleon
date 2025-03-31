import random
from Graph import *


## -------------------------------------------------------------------


class CoCoASolver:

  def __init__(self, graph):
    self.graph = graph
    self.agents = graph.nodes
    # agents = graph.nodes

    ## MUST CHECK LATER THAT ALL CONNECTED COMPONENTS HAVE BEEN COVERED - loop through agent states
    ## PERHAPS PUT IN solve SO THAT SAME OBJECT CAN BE RUN MULTIPLE TIMES
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
    notDone = []
    for agent in agents:
      if agent.currentState != State.DONE:
        notDone.append(agent)
    if notDone:
      return(False, random.choice(notDone))
    else:
      return (True, None)


  def solve(self):
    (done, toDo) = self.checkDone(self.agents)        # Graph may have multiple connected components

    for agent in self.agents:                       # Set neighbour states
      for n in agent.neighbours:
        n.neighbourStates.update({agent : agent.currentState})

    while not done:              # TruckDriver that is not in DONE state
    #for i in range(15):
      #print(toDo)
      self.solveNode(toDo)
      #-- anything to add?
      (done, toDo) = self.checkDone(self.agents)


  def solveNode(self, agent):     # assume non-singleton, and at least one assignment exists

    assert(agent.currentState == State.IDLE or agent.currentState == State.HOLD)
    agent.currentState = State.ACTIVE

    for n in agent.neighbours:
      n.updState(agent, State.ACTIVE, self)

    thetas = []                      # List of dictionaries returned by CostMsg
    for n in agent.neighbours:
      cost_map = n.inqMsg(agent, agent.context)
      thetas.append(cost_map)

    theta = {}                      # add utility of container assignment to central node
    for p in agent.prefs:
      theta.update({p[2] : p[0]})
    thetas.append(theta)

    sumThetas = {}
    for k in (thetas[0]).keys():    # k is a container
      sum = 0
      for t in thetas:
        sum += t.get(k)
      sumThetas.update({k : sum})

    maxThetas = {k : v for k, v in sumThetas.items() if v == max(sumThetas.values()) and k.assigned == False}

    idlActNeighbours = {k : v for k, v in agent.neighbourStates.items() if v == State.IDLE or v == State.ACTIVE}
    #print("id", agent.id, idlActNeighbours)
    if maxThetas and (len(maxThetas) < agent.uniquenessBound or len(idlActNeighbours) == 0):

      """print("-- ", agent)
      for k in maxThetas.keys():
        print("= ", k, agent.utility(k))
      print(thetas)
      print(sumThetas)
      print(maxThetas, "\n\n")"""

      x = random.choice(list(maxThetas.keys()))
      x.assigned = True
      agent.X = x
      agent.currentState = State.DONE
      for n in agent.neighbours:
        n.updState(agent, State.DONE, self)
        n.setVal(agent, agent.X)
    else:
      agent.currentState = State.HOLD
      for n in agent.neighbours:
        n.updState(agent, State.HOLD, self)
