from Container import *
from TruckDriver import *


## -------------------------------------------------------------------


class Graph:

  ## Initialise graph with TruckDrivers as nodes, where neighbours share a container preference
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
