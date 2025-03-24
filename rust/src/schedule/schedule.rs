use std::collections::{BTreeMap, HashMap, HashSet};

use rand::{seq::IndexedRandom, Rng};

use super::intervals::*;

pub type Terminal = u64;
pub type Cargo = u64;
pub type Truck = u64;

/// A class representing information for a transition other than time
#[derive(PartialEq, Eq, Clone)]
struct TransitionInfo {
    from: Terminal,
    to: Terminal,
    cargo: Cargo,
}

pub type Transition = IntervalWithData<TransitionInfo>;
pub type TransitionChain = IntervalWithDataChain<TransitionInfo>;

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

#[derive(Clone)]
struct Schedule {
    truck_transitions: HashMap<Truck, TransitionChain>,
    planned_cargo: HashSet<Cargo>,
}

/// A map from (from_terminal, to_terminal) to cached driving times
struct DrivingTimesCache {
    data: DrivingTimesMap,
}

impl DrivingTimesCache {
    fn get_driving_time(&mut self, from: Terminal, to: Terminal) -> TimeDelta {
        if from == to {
            return 0;
        }

        // Get cached or recalculate cache
        self.data
            .entry((from, to))
            .or_insert_with(|| {
                // TODO: compute distance somehow
                unimplemented!()
            })
            .to_owned()
    }
}

/// Class with logic and data needed to create schedules
struct ScheduleGenerator {
    /// Intervals when the terminals are open
    terminal_open_intervals: HashMap<Terminal, IntervalChain>,

    /// A map from intervals to transitions which can be taken during those intervals
    transitions_by_intervals: TransitionsByIntervalsMap,

    /// A map from (from_terminal, to_terminal) to cached driving times
    driving_times_cache: DrivingTimesCache,
    /// Times during which pickup can occur. Takes into account e.g. terminals
    /// closing overnight
    pickup_times: IntervalsByCargoMap,

    /// Times during which dropoff can occur. Takes into account e.g. terminals
    /// closing overnight
    dropoff_times: IntervalsByCargoMap,

    /// A map from cargo to information about delivering it
    direct_info: HashMap<Cargo, CargoDeliveryInformation>,

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
        let all_intervals = if let Some(entry) = self.transitions_by_intervals.get(&key) {
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

        for (cargo, delivery_info) in self.direct_info.iter() {
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
                        &schedule.planned_cargo,
                    );
                }
            }
        }
        None
    }
}
