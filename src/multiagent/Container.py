

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
    self.biddingTDs = []                           # TruckDrivers who would like to take it
    self.assigned = False                          # has it already been assinged to a TruckDriver

  def __str__(self):
    return f"{self.id}\tType: {self.type}\tWeight: {self.weight}\tADR: {self.adr}"
