from Graph import *
from CoCoASolver import *


## -------------------------------------------------------------------


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


## -------------------------------------------------------------------


"""for n in g.nodes:
  print(n)
  print("---")
  for p in n.prefs:
    print(p)
  print({m.id for m in n.neighbours})
  for m in n.neighbours:
    print(m.id)
  print("====\n")

for e in g.edges:
  print(e[0].id, e[1].id)"""


## -------------------------------------------------------------------


cocoa = CoCoASolver(g)

"""for n in g.nodes:
  print(n)
  print(n.currentState)
  print(n.X)
  print(n.neighbours)
  print(n.neighbourStates)"""


cocoa.solve()

print("===")

for n in g.nodes:
  print(n)
  print(n.currentState)
  print(n.X)
  print(n.neighbourStates)
