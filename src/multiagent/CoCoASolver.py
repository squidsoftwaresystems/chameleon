import random
from src.multiagent import Graph


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
