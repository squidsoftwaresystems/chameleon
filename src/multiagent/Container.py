

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
