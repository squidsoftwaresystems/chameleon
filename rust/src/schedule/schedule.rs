use std::collections::{BTreeMap, HashMap, HashSet};

use pyo3::{exceptions::PyTypeError, pyclass, pymethods, FromPyObject, PyResult};
use rand::{seq::IndexedRandom, Rng};

use super::intervals::*;

// TODO: convert these to struct Terminal(u64), etc
// to make it more fool-proof
// #[pyclass]
// #[derive(Clone, Copy, PartialEq, Eq, Hash)]
type Terminal = u64;
type Cargo = u64;
type Truck = u64;

/// A class representing information for a transition other than time
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct TransitionInfo {
    from: Terminal,
    to: Terminal,
    cargo: Cargo,
}

pub type Transition = IntervalWithData<TransitionInfo>;
pub type TransitionChain = IntervalWithDataChain<TransitionInfo>;

#[pyclass]
#[derive(FromPyObject)]
/// The representation of request for delivery that the rust code gets from python
pub struct TransportRequest {
    #[pyo3(get, set)]
    cargo: Cargo,
    #[pyo3(get, set)]
    from_terminal: Terminal,
    #[pyo3(get, set)]
    to_terminal: Terminal,
    #[pyo3(get, set)]
    pickup_open_time: Time,
    #[pyo3(get, set)]
    pickup_close_time: Time,
    #[pyo3(get, set)]
    dropoff_open_time: Time,
    #[pyo3(get, set)]
    dropoff_close_time: Time,
    #[pyo3(get, set)]
    direct_driving_time: TimeDelta,
}

#[pymethods]
impl TransportRequest {
    #[new]
    pub fn new(
        cargo: Cargo,
        from_terminal: Terminal,
        to_terminal: Terminal,
        pickup_open_time: Time,
        pickup_close_time: Time,
        dropoff_open_time: Time,
        dropoff_close_time: Time,
        direct_driving_time: TimeDelta,
    ) -> Self {
        Self {
            cargo,
            from_terminal,
            to_terminal,
            pickup_open_time,
            pickup_close_time,
            dropoff_open_time,
            dropoff_close_time,
            direct_driving_time,
        }
    }
}

#[derive(Debug)]
struct CargoDeliveryInformation {
    ///Times during which a pickup can occur and a truck has enough time to drive
    ///directly to destination and be on time for drop-off. Takes into account
    ///e.g. terminals closing overnight
    direct_delivery_start_times: IntervalChain,
    direct_driving_time: TimeDelta,

    /// Terminal where cargo can be picked up from
    from: Terminal,
    /// Terminal where cargo needs to be dropped off to
    to: Terminal,
}

type DrivingTimesMap = HashMap<(Terminal, Terminal), TimeDelta>;
type TransitionsByIntervalsMap = HashMap<(Terminal, Terminal, Interval), Vec<Transition>>;
type IntervalsByCargoMap = HashMap<Cargo, IntervalChain>;

#[pyclass]
#[derive(Clone)]
pub struct Schedule {
    truck_transitions: BTreeMap<Truck, TransitionChain>,

    #[pyo3(get, set)]
    planned_cargo: HashSet<Cargo>,
}

#[pymethods]
impl Schedule {
    /// Generates a textual representation of the schedule
    pub fn __repr__(&self) -> String {
        let mut out = String::new();
        for (truck, transitions) in self.truck_transitions.iter() {
            out.push_str(&format!("Truck {truck}:\n"));
            for transition in transitions.get_intervals().iter() {
                let transition_info = transition.get_additional_data();
                out.push_str(&format!(
                    "[{}, {}]: Cargo {}: {}->{}\n",
                    transition.get_start_time(),
                    transition.get_end_time(),
                    transition_info.cargo,
                    transition_info.from,
                    transition_info.to
                ));
            }
            out.push_str("\n\n");
        }
        out
    }

    /// Returns a score representing how good the Schedule is
    pub fn score(&self) -> f64 {
        // Get the number of deliveries
        let num_deliveries: usize = self
            .truck_transitions
            .values()
            .map(|transition| transition.get_intervals().len())
            .sum();

        num_deliveries as f64
    }
}

/// A map from (from_terminal, to_terminal) to cached driving times
struct DrivingTimesCache {
    // NOTE: assumes that driving from A to B might take a different time than
    // driving from B to A
    data: DrivingTimesMap,
}

impl DrivingTimesCache {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn from_map(map: DrivingTimesMap) -> Self {
        Self { data: map }
    }

    fn get_driving_time(&mut self, from: Terminal, to: Terminal) -> TimeDelta {
        if from == to {
            return 0;
        }

        // Get cached or recalculate cache
        self.data
            .entry((from, to))
            .or_insert_with(|| {
                // TODO: compute distance somehow
                10000000
            })
            .to_owned()
    }
}

/// Class with logic and data needed to create schedules
#[pyclass]
pub struct ScheduleGenerator {
    /// Intervals when the terminals are open
    terminal_open_intervals: HashMap<Terminal, IntervalChain>,

    /// A map from intervals to transitions which can be taken during those intervals
    transitions_by_intervals_cache: TransitionsByIntervalsMap,

    /// A map from (from_terminal, to_terminal) to cached driving times
    driving_times_cache: DrivingTimesCache,
    /// Times during which pickup can occur. Takes into account e.g. terminals
    /// closing overnight
    pickup_times: IntervalsByCargoMap,

    /// Times during which dropoff can occur. Takes into account e.g. terminals
    /// closing overnight
    dropoff_times: IntervalsByCargoMap,

    /// A map from cargo to information about delivering it
    cargo_delivery_info: HashMap<Cargo, CargoDeliveryInformation>,

    trucks: Vec<Truck>,

    /// Terminals where the trucks start at
    truck_terminals: HashMap<Truck, Terminal>,

    /// Time in which we are allowed to schedule trucks
    planning_period: Interval,
}

impl ScheduleGenerator {
    /// Returns a vector of transitions which can be done starting at terminal
    /// `from`, ending at `to`, and driving to transition start terminal, driving
    /// through the transition, and then driving to `to`. Also uses `planned_cargo`
    /// to only return cargo that still doesn't have delivery plans
    fn get_possible_transitions_for_window(
        &mut self,
        from: Terminal,
        to: Terminal,
        when: &Interval,
        planned_cargo: &HashSet<Cargo>,
    ) -> Vec<Transition> {
        let key = (from, to, when.clone());
        // Find cached value or recalculate
        let all_intervals = if let Some(entry) = self.transitions_by_intervals_cache.get(&key) {
            entry
        } else {
            &self.recalculate_possible_transitions_for_window(from, to, when)
        };

        // Filter out all cargo that has already been planned
        all_intervals
            .iter()
            .filter(|transition| !planned_cargo.contains(&transition.get_additional_data().cargo))
            // Copy all this into a Vec
            .cloned()
            .collect()
    }

    fn recalculate_possible_transitions_for_window(
        &mut self,
        from: Terminal,
        to: Terminal,
        when: &Interval,
    ) -> Vec<Transition> {
        let mut out = Vec::new();

        for (cargo, delivery_info) in self.cargo_delivery_info.iter() {
            // Driving from current terminal to start terminal of cargo
            let driving_time1 = self
                .driving_times_cache
                .get_driving_time(from, delivery_info.from);

            // Driving from terminal where cargo is dropped of to end terminal
            let driving_time2 = self
                .driving_times_cache
                .get_driving_time(delivery_info.to, to);

            let delivery_duration = delivery_info.direct_driving_time;

            // interval in which we can start the new delivery,
            // allowing for time beforehand to go to `delivery_info`.to
            // and time to go to `delivery_info`.from
            let padded_delivery_start_times =
                when.reschedule(driving_time1, -driving_time2 - delivery_duration);

            // If we have time to do that,
            if let Some(padded_delivery_start_times) = padded_delivery_start_times {
                // add the intervals when we have the time

                // Look at the intervals where we both can start the delivery
                // and have enough time to complete it
                let allowed_windows = delivery_info.direct_delivery_start_times.intersect(
                    &IntervalWithDataChain::from_interval(padded_delivery_start_times),
                );

                // For each of them, add a potential delivery
                for window in allowed_windows.get_intervals().iter() {
                    let transition_info = TransitionInfo {
                        from: delivery_info.from,
                        to: delivery_info.to,
                        cargo: *cargo,
                    };
                    let start_time = window.get_start_time();
                    let end_time = window.get_end_time();
                    // 1. As soon as we are free
                    out.push(
                        Transition::new(
                            start_time,
                            start_time.checked_add_signed(delivery_duration).unwrap(),
                            transition_info.clone(),
                        )
                        .unwrap(),
                    );

                    // 2. At the last possible moment
                    out.push(
                        Transition::new(
                            end_time.checked_add_signed(-delivery_duration).unwrap(),
                            end_time,
                            transition_info,
                        )
                        .unwrap(),
                    );
                }
            }
        }

        out
    }
}

/// Creates an interval [start_time, end_time] and returns an error
/// if invalid
fn interval_or_error(start_time: Time, end_time: Time) -> PyResult<Interval> {
    if let Some(interval) = Interval::new(start_time, end_time, ()) {
        Ok(interval)
    } else {
        Err(PyTypeError::new_err(format!(
            "Invalid interval starting at {start_time}, ending at {end_time}"
        )))
    }
}

#[pymethods]
impl ScheduleGenerator {
    #[new]
    /// Create a new schedule generator
    /// terminal_data is a dict sending a terminal id to (opening_time, closing_time)
    /// truck_data is a dict sending truck id to starting_terminal
    pub fn new(
        terminal_data: HashMap<Terminal, (Time, Time)>,
        truck_data: HashMap<Truck, Terminal>,
        transport_data: Vec<TransportRequest>,
        planning_period: (Time, Time),
    ) -> PyResult<Self> {
        // Calculate terminal_open_intervals
        let mut terminal_open_intervals = HashMap::new();
        for (terminal, (opening_time, closing_time)) in terminal_data.iter() {
            // If it is a valid interval, create
            let interval = interval_or_error(*opening_time, *closing_time)?;
            // TODO: make opening and closing times repeat day on day
            let intervals = IntervalChain::from_interval(interval);
            terminal_open_intervals.insert(*terminal, intervals);
        }

        let mut trucks = Vec::new();
        let mut truck_terminals = HashMap::new();

        for (truck, starting_terminal) in truck_data.into_iter() {
            trucks.push(truck);
            truck_terminals.insert(truck, starting_terminal);
        }

        // Calculate pickup and dropoff times
        let mut pickup_times = HashMap::new();
        let mut dropoff_times = HashMap::new();

        let mut cargo_delivery_info = HashMap::new();

        let mut driving_times_map: DrivingTimesMap = HashMap::new();

        for transport_request in transport_data.iter() {
            let cargo = transport_request.cargo;
            // Pickup intervals that don't consider terminal opening times
            let raw_pickup_intervals = IntervalChain::from_interval(interval_or_error(
                transport_request.pickup_open_time,
                transport_request.pickup_close_time,
            )?);
            let raw_dropoff_intervals = IntervalChain::from_interval(interval_or_error(
                transport_request.dropoff_open_time,
                transport_request.dropoff_close_time,
            )?);

            let from_terminal_open_intervals = terminal_open_intervals
                .get(&transport_request.from_terminal)
                .unwrap();

            let to_terminal_open_intervals = terminal_open_intervals
                .get(&transport_request.to_terminal)
                .unwrap();

            let pickup_intervals = from_terminal_open_intervals.intersect(&raw_pickup_intervals);
            let dropoff_intervals = to_terminal_open_intervals.intersect(&raw_dropoff_intervals);

            // Calculate direct_delivery_start_times: times when we can
            // start a delivery on a direct route and complete it successfully
            // To do that, shift dropoff times by time it takes to drive to get
            // times when it is possible to leave
            let driving_time: TimeDelta = transport_request.direct_driving_time;
            let shifted_dropoff_intervals = IntervalChain::from_intervals(
                dropoff_intervals
                    .get_intervals()
                    .iter()
                    .map(|interval| interval.reschedule(-driving_time, -driving_time).unwrap())
                    .collect(),
            );
            let direct_delivery_start_times =
                pickup_intervals.intersect(&shifted_dropoff_intervals);

            pickup_times.insert(cargo, pickup_intervals);
            dropoff_times.insert(cargo, dropoff_intervals);

            // Record driving times on direct routes
            driving_times_map.insert(
                (
                    transport_request.from_terminal,
                    transport_request.to_terminal,
                ),
                transport_request.direct_driving_time,
            );

            // Update delivery info
            let delivery_info = CargoDeliveryInformation {
                direct_driving_time: transport_request.direct_driving_time,
                from: transport_request.from_terminal,
                to: transport_request.to_terminal,
                direct_delivery_start_times,
            };
            cargo_delivery_info.insert(cargo, delivery_info);
        }

        Ok(Self {
            terminal_open_intervals,
            transitions_by_intervals_cache: HashMap::new(),
            driving_times_cache: DrivingTimesCache::from_map(driving_times_map),
            pickup_times,
            dropoff_times,
            cargo_delivery_info,
            trucks,
            truck_terminals,
            planning_period: interval_or_error(planning_period.0, planning_period.1)?,
        })
    }

    pub fn empty_schedule(&self) -> Schedule {
        let mut truck_transitions: BTreeMap<Truck, TransitionChain> = BTreeMap::new();
        for truck in self.trucks.iter() {
            truck_transitions.insert(*truck, TransitionChain::new());
        }

        Schedule {
            truck_transitions,
            planned_cargo: HashSet::new(),
        }
    }

    /// Gets a random neighbour for a schedule.
    /// Note that the neighbours might not be sampled uniformly.
    /// Try to generate a truck `num_tries` times before
    /// giving up. This helps protect against e.g. schedules that have no
    /// neighbours
    pub fn get_schedule_neighbour(
        &mut self,
        schedule: &Schedule,
        num_tries: usize,
    ) -> Option<Schedule> {
        let mut out = schedule.clone();
        let mut rng = rand::rng();

        for _ in 0..num_tries {
            // Pick a truck at random
            let truck = self.trucks.choose(&mut rng)?;

            let transitions: &mut TransitionChain = out.truck_transitions.get_mut(truck).unwrap();

            // We require the schedule to be within the planning interval
            if !transitions.contained_in(&self.planning_period) {
                return None;
            }

            // Randomly decide whether we want to add or remove a transition
            // Prioritise adding transitions because we want to explore more of those
            // options, and also because adding a transition might fail, but removing is a lot less likely to fail
            // TODO: count how many relevant values in gaps there are and use that
            if rng.random_range(0..3) == 0 {
                // Remove some transition
                // If empty, fail
                if transitions.get_intervals().is_empty() {
                    continue;
                }

                // Else, remove one of them
                let index_to_remove = rng.random_range(0..transitions.get_intervals().len());
                let removed_transition = transitions.remove(index_to_remove);
                out.planned_cargo
                    .remove(&removed_transition.get_additional_data().cargo);

                return Some(out);
            } else {
                // Add some transaction
                let gaps = transitions.gaps(&self.planning_period);
                let gap = gaps.get_intervals().choose(&mut rng);

                // if the gap is empty, fail
                if let Some(gap) = gap {
                    let (prev_transition_data, next_transition_data) = gap.get_additional_data();
                    // Take terminal from previous transition or
                    // initial terminal if this is the first gap
                    let from_terminal = if let Some(prev_transition_data) = prev_transition_data {
                        prev_transition_data.to
                    } else {
                        *self.truck_terminals.get(truck).unwrap()
                    };

                    // Take terminal from next transition or
                    // initial terminal if this is the last gap
                    // TODO: allow flexibility for last terminal
                    let to_terminal = if let Some(next_transition_data) = next_transition_data {
                        next_transition_data.from
                    } else {
                        *self.truck_terminals.get(truck).unwrap()
                    };

                    let possible_new_transitions = self.get_possible_transitions_for_window(
                        from_terminal,
                        to_terminal,
                        &gap.remove_additional_data(),
                        &out.planned_cargo,
                    );

                    // If no transitions here, fail
                    if let Some(new_transition) = possible_new_transitions.choose(&mut rng) {
                        // Add the transition

                        assert!(transitions.try_add(new_transition.clone()));
                        out.planned_cargo
                            .insert(new_transition.get_additional_data().cargo);

                        return Some(out);
                    }
                }
            }
        }
        None
    }
}
