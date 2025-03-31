use std::collections::{BTreeMap, BTreeSet, HashMap};

use pyo3::{exceptions::PyTypeError, pyclass, pymethods, FromPyObject, PyResult};
use rand::{rngs::ThreadRng, seq::IteratorRandom, Rng};

use super::intervals::*;

//NOTE: this prevents recognising them as the same type, and e.g.
// assigning a truck to a cargo by mistake
#[pyclass]
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Debug)]
pub struct Terminal(u64);

#[pymethods]
impl Terminal {
    #[new]
    pub fn new(terminal: u64) -> Self {
        Terminal(terminal)
    }
}

#[pyclass]
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Debug)]
pub struct Cargo(u64);

#[pymethods]
impl Cargo {
    #[new]
    pub fn new(cargo: u64) -> Self {
        Cargo(cargo)
    }
}

#[pyclass]
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Debug)]
pub struct Truck(u64);

#[pymethods]
impl Truck {
    #[new]
    pub fn new(truck: u64) -> Self {
        Truck(truck)
    }
}

#[pyclass]
#[derive(FromPyObject)]
/// The representation of request for delivery that the rust code gets from python
pub struct Booking {
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
impl Booking {
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
struct BookingInformation {
    /// Terminal where cargo can be picked up from
    from: Terminal,
    /// Terminal where cargo needs to be dropped off to
    to: Terminal,
}

type DrivingTimesMap = HashMap<(Terminal, Terminal), TimeDelta>;
type IntervalsByCargoMap = HashMap<Cargo, IntervalChain>;

/// An operation that the truck needs to carry out
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Checkpoint {
    time: Time,
    // Needs to be at this terminal
    terminal: Terminal,
    pickup_cargo: BTreeSet<Cargo>,
    dropoff_cargo: BTreeSet<Cargo>,
}

#[pyclass]
#[derive(Clone)]
pub struct Schedule {
    truck_checkpoints: BTreeMap<Truck, Vec<Checkpoint>>,

    #[pyo3(get, set)]
    /// Map from cargo that was scheduled to truck taking it
    scheduled_cargo_truck: BTreeMap<Cargo, Truck>,
}

impl Schedule {
    fn get_checkpoint_mut(
        &mut self,
        truck: &Truck,
        checkpoint_index: usize,
    ) -> Option<&mut Checkpoint> {
        self.truck_checkpoints
            .get_mut(truck)?
            .get_mut(checkpoint_index)
    }

    /// Finds latest checkpoint strictly before `time` for truck
    /// and earliest checkpoint strictly after `time`
    fn get_surrounding_checkpoints(
        &self,
        truck: Truck,
        time: Time,
    ) -> (Option<&Checkpoint>, Option<&Checkpoint>) {
        let checkpoints = self.truck_checkpoints.get(&truck).unwrap();

        let prev = checkpoints
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.time < time);
        let next = checkpoints.iter().find(|checkpoint| checkpoint.time > time);

        (prev, next)
    }
}

#[pymethods]
impl Schedule {
    /// Generates a textual representation of the schedule
    pub fn __repr__(&self) -> String {
        let mut out = String::new();
        for (truck, checkpoints) in self.truck_checkpoints.iter() {
            out.push_str(&format!("Truck {truck:?}:\n"));
            for checkpoint in checkpoints.iter() {
                out.push_str(&format!(
                    "Time: {}, Terminal {:?}: Pick up {:?}, drop off {:?}\n",
                    checkpoint.time,
                    checkpoint.terminal,
                    // Display as vector
                    checkpoint.pickup_cargo.iter().collect::<Vec<_>>(),
                    // Display as vector
                    checkpoint.dropoff_cargo.iter().collect::<Vec<_>>(),
                ));
            }
            out.push_str("\n\n");
        }
        out
    }

    /// Returns a score representing how good the Schedule is
    pub fn score(&self) -> f64 {
        // Get the number of deliveries
        let num_deliveries: usize = self.scheduled_cargo_truck.len();

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
    fn from_map(map: DrivingTimesMap) -> Self {
        Self { data: map }
    }

    fn get_driving_time(&mut self, from: Terminal, to: Terminal) -> TimeDelta {
        if from == to {
            return 0;
        }

        // Get cached or recalculate cache
        let out = self
            .data
            .entry((from, to))
            .or_insert_with(|| {
                // TODO: compute distance somehow
                unimplemented!()
            })
            .to_owned();
        assert!(out >= 0);
        out
    }
}

/// Class with logic and data needed to create schedules
#[pyclass]
pub struct ScheduleGenerator {
    /// A map from (from_terminal, to_terminal) to cached driving times
    driving_times_cache: DrivingTimesCache,

    // A map from (start_terminal, end_terminal) to collection of cargo
    // that can be delivered from start_terminal to end_terminal
    cargo_by_terminals: HashMap<(Terminal, Terminal), BTreeSet<Cargo>>,

    /// Times during which pickup can occur. Takes into account e.g. terminals
    /// closing overnight
    pickup_times: IntervalsByCargoMap,

    /// Times during which dropoff can occur. Takes into account e.g. terminals
    /// closing overnight
    dropoff_times: IntervalsByCargoMap,

    /// A map from cargo to information about delivering it
    cargo_booking_info: HashMap<Cargo, BookingInformation>,

    terminals: BTreeSet<Terminal>,

    trucks: BTreeSet<Truck>,

    /// Terminals when and where the trucks start at
    truck_starting_data: HashMap<Truck, (Time, Terminal)>,

    /// Time in which we are allowed to schedule trucks
    planning_period: Interval,
}

impl ScheduleGenerator {
    /// Given a checkpoint before, checkpoint after and a terminal
    /// for a new checkpoint between them, output the interval in which
    /// the new checkpoint can be placed so that we have enough time to
    /// drive between them.
    fn get_driving_time_constraints(
        &mut self,
        truck: Truck,
        prev_checkpoint: Option<&Checkpoint>,
        next_checkpoint: Option<&Checkpoint>,
        new_terminal: Terminal,
    ) -> Option<Interval> {
        let (time_before, time_after, terminal_before, terminal_after) =
            self.get_surrounding_terminals_and_times(truck, prev_checkpoint, next_checkpoint);

        let driving_time1 = self
            .driving_times_cache
            .get_driving_time(terminal_before, new_terminal);

        // If the end terminal is not enforced, we can just stay where we are
        let driving_time2 = if let Some(end_terminal) = terminal_after {
            self.driving_times_cache
                .get_driving_time(new_terminal, end_terminal)
        } else {
            0
        };

        let earliest_checkpoint_time = time_before.checked_add_signed(driving_time1).unwrap();
        let latest_checkpoint_time = time_after.checked_add_signed(-driving_time2).unwrap();

        Interval::new(earliest_checkpoint_time, latest_checkpoint_time, ())
    }

    /// Given a previous and next checkpoints, find
    /// what terminals and times those correspond to. Handles cases when
    /// there is no checkpoint, and suggests correct terminals
    fn get_surrounding_terminals_and_times(
        &self,
        truck: Truck,
        prev_checkpoint: Option<&Checkpoint>,
        next_checkpoint: Option<&Checkpoint>,
    ) -> (Time, Time, Terminal, Option<Terminal>) {
        let (prev_time, prev_terminal) = if let Some(prev) = prev_checkpoint {
            (prev.time, prev.terminal)
        } else {
            // Before first interval
            (
                self.planning_period.get_start_time(),
                self.truck_starting_data.get(&truck).unwrap().1,
            )
        };

        let (next_time, next_terminal) = if let Some(next) = next_checkpoint {
            (next.time, Some(next.terminal))
        } else {
            (self.planning_period.get_end_time(), None)
        };
        (prev_time, next_time, prev_terminal, next_terminal)
    }

    /// Return (`truck`, index in `checkpoints`) for a random checkpoint
    fn get_random_checkpoint<'a>(
        &mut self,
        schedule: &'a Schedule,
        rng: &mut ThreadRng,
    ) -> Option<(&'a Checkpoint, Truck, usize)> {
        // Pick a random checkpoint, uniformly, across trucks
        // TODO: keep track of this number to make it faster
        let total_num_checkpoints = schedule
            .truck_checkpoints
            .iter()
            .map(|(_truck, checkpoints)| checkpoints.len())
            .sum();

        if total_num_checkpoints == 0 {
            return None;
        }

        let checkpoint_index = rng.random_range(0..total_num_checkpoints);
        let mut num_checkpoints_considered = 0;
        // Find a truck, weighted by number of checkpoints in it
        let (chosen_truck, chosen_index) = schedule
            .truck_checkpoints
            .iter()
            .find_map(|(truck, checkpoints)| {
                if checkpoint_index - num_checkpoints_considered < checkpoints.len() {
                    return Some((truck, checkpoint_index - num_checkpoints_considered));
                } else {
                    num_checkpoints_considered += checkpoints.len();
                    return None;
                }
            })
            .unwrap();

        let checkpoint = schedule
            .truck_checkpoints
            .get(&chosen_truck)
            .unwrap()
            .get(chosen_index)
            .unwrap();
        Some((checkpoint, *chosen_truck, chosen_index))
    }

    /// Try to add a random direct delivery; return new schedule if succeeded
    fn add_random_checkpoint(
        &mut self,
        schedule: &Schedule,
        rng: &mut ThreadRng,
    ) -> Option<Schedule> {
        // TODO: pick so that empty trucks have a higher chance of being picked
        let truck = *self.trucks.iter().choose(rng)?;

        // We want to pick an interval between checkpoints to which we will add a new checkpoint
        // Pick a time uniformly at random and pick the interval containing that time,
        // so that large intervals are more likely to be chosen, breaking up large intervals.
        let planning_start_time = self.planning_period.get_start_time();
        let planning_end_time = self.planning_period.get_end_time();
        let time_to_identify_gap = (planning_start_time..planning_end_time).choose(rng)?;
        let (prev_checkpoint, next_checkpoint) =
            schedule.get_surrounding_checkpoints(truck, time_to_identify_gap);
        let (_, _, start_terminal, end_terminal) =
            self.get_surrounding_terminals_and_times(truck, prev_checkpoint, next_checkpoint);

        // TODO: pick a time and a terminal based on whether we can pick up or drop off cargo at the time
        // For now, we just picked at random and stick with it

        // disallow picking same terminal as the one before or after, since we want to associate
        // gaps between checkpoints with driving
        let new_terminal = *self
            .terminals
            .iter()
            .filter(|terminal| **terminal != start_terminal && Some(**terminal) != end_terminal)
            .choose(rng)?;

        let allowed_time_interval = self.get_driving_time_constraints(
            truck,
            prev_checkpoint,
            next_checkpoint,
            new_terminal,
        )?;

        // Otherwise, schedule a checkpoint in this time, if we can
        let new_time = allowed_time_interval.random_time(rng);

        let mut out = schedule.clone();
        let new_deliveries = out.truck_checkpoints.get_mut(&truck).unwrap();

        // Insert in place of first element after it,
        // or if all elements are before it, insert it at the end
        let new_checkpoint_index = new_deliveries
            .iter()
            .position(|checkpoint| checkpoint.time > new_time)
            .unwrap_or(new_deliveries.len());
        new_deliveries.insert(
            new_checkpoint_index,
            Checkpoint {
                time: new_time,
                terminal: new_terminal,
                pickup_cargo: BTreeSet::new(),
                dropoff_cargo: BTreeSet::new(),
            },
        );

        // Make sure that the times are still in strictly ascending order of time
        // https://stackoverflow.com/questions/51272571/how-do-i-check-if-a-slice-is-sorted
        assert!(new_deliveries
            .windows(2)
            .all(|checkpoints| checkpoints[0].time < checkpoints[1].time));

        return Some(out);
    }

    /// Pick a random checkpoint and remove it
    fn remove_random_checkpoint(
        &mut self,
        schedule: &Schedule,
        rng: &mut ThreadRng,
    ) -> Option<Schedule> {
        let (checkpoint, chosen_truck, chosen_index) = self.get_random_checkpoint(schedule, rng)?;
        // To avoid easily undoing progress, only allow removing checkpoint if there is no cargo
        // pickup or dropoff in it

        // TODO: maybe it is faster to list all checkpoints without pickups or dropoffs and
        // then pick randomly among them
        if !checkpoint.pickup_cargo.is_empty() || !checkpoint.dropoff_cargo.is_empty() {
            return None;
        }

        // TODO: make the clones cheaper
        let mut out = schedule.clone();

        // Remove the cargo
        out.truck_checkpoints
            .get_mut(&chosen_truck)
            .unwrap()
            .remove(chosen_index);

        return Some(out);
    }

    // fn remove_random_dropoff(
    //     &mut self,
    //     schedule: &Schedule,
    //     rng: &mut ThreadRng,
    // ) -> Option<Schedule> {
    //     // Only consider cargo that has been delivered
    //     let (cargo, cargo_state) = schedule
    //         .scheduled_cargo_state
    //         .iter()
    //         .filter(|(_, cargo_state)| cargo_state.is_delivered())
    //         .choose(rng)?;
    //     let mut out = schedule.clone();
    //
    //     // Remove all references to this cargo in truck
    //     let truck = cargo_state.get_truck();
    //
    //     out.scheduled_cargo_state
    //         .insert(*cargo, ScheduledCargoState::PickedUp(truck));
    //     out.truck_checkpoints
    //         .get_mut(&truck)
    //         .unwrap()
    //         .iter_mut()
    //         .for_each(|checkpoint| {
    //             checkpoint.dropoff_cargo.remove(cargo);
    //         });
    //
    //     Some(out)
    // }

    /// Remove pickup and dropoff for a piece of cargo
    fn remove_random_delivery(
        &mut self,
        schedule: &Schedule,
        rng: &mut ThreadRng,
    ) -> Option<Schedule> {
        let (cargo, truck) = schedule.scheduled_cargo_truck.iter().choose(rng)?;
        let mut out = schedule.clone();

        out.scheduled_cargo_truck.remove(cargo);
        // Remove all references to this cargo in truck
        out.truck_checkpoints
            .get_mut(&truck)
            .unwrap()
            .iter_mut()
            .for_each(|checkpoint| {
                checkpoint.pickup_cargo.remove(cargo);
                checkpoint.dropoff_cargo.remove(cargo);
            });

        Some(out)
    }

    /// Given an  old checkpoint and new pickup and dropoff for it,
    /// finds a random time it can be rescheduled to. Keeps the relative
    /// order of all checkpoints the same
    fn find_random_reschedule_time(
        &mut self,
        schedule: &Schedule,
        truck: &Truck,
        old_checkpoint_index: usize,
        new_pickup: &BTreeSet<Cargo>,
        new_dropoff: &BTreeSet<Cargo>,
        rng: &mut ThreadRng,
    ) -> Option<Time> {
        let old_checkpoint = schedule
            .truck_checkpoints
            .get(truck)
            .unwrap()
            .get(old_checkpoint_index)
            .unwrap();
        let pickup_restriction_intervals = new_pickup
            .iter()
            .map(|cargo| self.pickup_times.get(cargo).unwrap())
            .intersect_all();
        let dropoff_restriction_intervals = new_dropoff
            .iter()
            .map(|cargo| self.dropoff_times.get(cargo).unwrap())
            .intersect_all();

        let (checkpoint_before, checkpoint_after) =
            schedule.get_surrounding_checkpoints(*truck, old_checkpoint.time);

        let driving_restriction_intervals =
            IntervalWithDataChain::from_interval(self.get_driving_time_constraints(
                *truck,
                checkpoint_before,
                checkpoint_after,
                old_checkpoint.terminal,
            )?);

        let allowed_intervals = [
            pickup_restriction_intervals,
            dropoff_restriction_intervals,
            driving_restriction_intervals,
        ]
        .iter()
        .intersect_all();

        let new_interval = allowed_intervals.get_intervals().iter().choose(rng)?;
        let new_time = (new_interval.get_start_time()..new_interval.get_end_time()).choose(rng)?;

        // TODO: implement this instead
        // // Pick a time in the allowed intervals uniformly,
        // // so that the sub-interval that is larger (and so offers more flexibility)
        // // is more likely to be picked
        //
        // // This is a measure of "how much we are away from the start",
        // // only measuring the time contained in the intervals. For example,
        // // if for intervals [1, 3), [10, 4), this value is 5, then
        // // we have "moved past" the 2 timesteps in the first interval,
        // // and are on the 3rd time step in the second interval
        // // We will then convert this to actual time.
        // let new_time_index = (0..allowed_intervals.total_length()).choose(rng);
        Some(new_time)
    }

    /// Add a random cargo pickup-dropoff pair to two checkpoints.
    /// If necessary, move checkpoints to allow this to be done
    fn add_random_delivery(
        &mut self,
        schedule: &Schedule,
        rng: &mut ThreadRng,
    ) -> Option<Schedule> {
        // Pick a random truck, see what cargo it can deliver based on what terminals
        // it is visiting
        let (truck, checkpoints) = schedule.truck_checkpoints.iter().choose(rng)?;

        // See what undelivered cargo can be delivered between these terminals

        // TODO: limit the gap between (from, to) as a heuristic: it is unlikely
        // that a truck will pick up a cargo, drive for a very long time,
        // then drop it off

        // A map from unscheduled cargo which can be taken by this truck
        // to a collection of (pickup_checkpoint, dropoff_checkpoint)
        let mut available_cargo_checkpoints = BTreeMap::new();
        for (start_checkpoint_index, start_checkpoint) in checkpoints.iter().enumerate() {
            // Look at all terminals after this
            for end_checkpoint_index in (start_checkpoint_index + 1)..checkpoints.len() {
                let end_checkpoint = checkpoints.get(end_checkpoint_index).unwrap();
                let start_terminal = start_checkpoint.terminal;
                let end_terminal = end_checkpoint.terminal;

                // If we found some,
                if let Some(cargo_collection) =
                    self.cargo_by_terminals.get(&(start_terminal, end_terminal))
                {
                    // Record all cargo that hasn't been scheduled yet
                    for cargo in cargo_collection.iter() {
                        if !schedule.scheduled_cargo_truck.contains_key(&cargo) {
                            available_cargo_checkpoints
                                .entry(*cargo)
                                .or_insert(BTreeSet::new())
                                .insert((
                                    start_checkpoint,
                                    end_checkpoint,
                                    start_checkpoint_index,
                                    end_checkpoint_index,
                                ));
                        }
                    }
                }
            }
        }

        // Pick random cargo and a random pair of checkpoints to deliver between
        let (chosen_cargo, chosen_checkpoint_pairs) =
            available_cargo_checkpoints.iter().choose(rng)?;
        // TODO: if the same start_checkpoint/end_checkpoint appears multiple times,
        // then the shortest delivery is always optimal, so disregard others.
        // E.g. if the truck goes A->B->C->A->B, and we want to deliver A->B,
        // it is always better to drive A->B than A->B->C->A->B
        // We will want to implement this in the future
        let (start_checkpoint, end_checkpoint, start_checkpoint_index, end_checkpoint_index) =
            chosen_checkpoint_pairs.iter().choose(rng).unwrap();

        let chosen_cargo = *chosen_cargo;
        let start_checkpoint_index = *start_checkpoint_index;
        let end_checkpoint_index = *end_checkpoint_index;

        // Find the intervals when these checkpoints can be moved to
        // Consider restrictions due to being able to pick up all items,
        // drop off all items and drive to and from checkpoint
        // TODO: it might make sense to cache this

        // TODO: add an operation that randomly reschedules some checkpoint

        // Create copies and operate on them
        let mut new_start_checkpoint_pickup = start_checkpoint.pickup_cargo.clone();
        new_start_checkpoint_pickup.insert(chosen_cargo);

        let mut new_end_checkpoint_dropoff = end_checkpoint.dropoff_cargo.clone();
        new_end_checkpoint_dropoff.insert(chosen_cargo);

        let mut out = schedule.clone();

        // NOTE: reschedule them one-by-one. If we reschedule them at the same time and
        // the end checkpoint is directly after the start checkpoint,
        // the end checkpoint might be rescheduled to before the new start
        // checkpoint time
        let new_start_checkpoint_time = self.find_random_reschedule_time(
            &out,
            truck,
            start_checkpoint_index,
            &new_start_checkpoint_pickup,
            &start_checkpoint.dropoff_cargo,
            rng,
        )?;
        let new_start_checkpoint = out
            .get_checkpoint_mut(truck, start_checkpoint_index)
            .unwrap();
        new_start_checkpoint.pickup_cargo.insert(chosen_cargo);
        new_start_checkpoint.time = new_start_checkpoint_time;

        let new_end_checkpoint_time = self.find_random_reschedule_time(
            &out,
            truck,
            end_checkpoint_index,
            &end_checkpoint.pickup_cargo,
            &new_end_checkpoint_dropoff,
            rng,
        )?;
        let new_end_checkpoint = out.get_checkpoint_mut(truck, end_checkpoint_index).unwrap();
        new_end_checkpoint.dropoff_cargo.insert(chosen_cargo);
        new_end_checkpoint.time = new_end_checkpoint_time;

        // Make sure that the times are still in strictly ascending order of time
        // https://stackoverflow.com/questions/51272571/how-do-i-check-if-a-slice-is-sorted
        assert!(out
            .truck_checkpoints
            .get(truck)
            .unwrap()
            .windows(2)
            .all(|checkpoints| checkpoints[0].time < checkpoints[1].time));

        out.scheduled_cargo_truck.insert(chosen_cargo, *truck);

        return Some(out);
    }
    //
    // /// Add a random cargo dropoff to a checkpoint
    // fn add_random_dropoff(&mut self, schedule: &Schedule, rng: &mut ThreadRng) -> Option<Schedule> {
    //     // Find a random cargo that has been picked up, but not dropped off
    //     let (chosen_cargo, chosen_cargo_state) = schedule
    //         .scheduled_cargo_state
    //         .iter()
    //         .filter(|(_, state)| state.is_picked_up_not_delivered())
    //         .choose(rng)?;
    //     let chosen_truck = chosen_cargo_state.get_truck();
    //
    //     let booking_info = self.cargo_booking_info.get(chosen_cargo).unwrap();
    //     let to_terminal = booking_info.to;
    //     // BUG: Not yet respecting allowed dropoff and pickup times
    //     // TODO: instead of simply adding a dropoff, move the checkpoint to allow for a new dropoff
    //     // to happen
    //
    //     let mut out = schedule.clone();
    //
    //     // Find first index after this truck picks up the cargo during which we can drop off
    //     let dropoff_checkpoint = out
    //         .truck_checkpoints
    //         .get_mut(&chosen_truck)
    //         .unwrap()
    //         .iter_mut()
    //         .skip_while(|checkpoint| !checkpoint.pickup_cargo.contains(chosen_cargo))
    //         .find(|checkpoint| checkpoint.terminal == to_terminal)?;
    //
    //     dropoff_checkpoint.dropoff_cargo.insert(*chosen_cargo);
    //     out.scheduled_cargo_state
    //         .insert(*chosen_cargo, ScheduledCargoState::Delivered(chosen_truck));
    //
    //     // Make sure that the dropoff occurs after pickup
    //     let deliveries = out.truck_checkpoints.get(&chosen_truck).unwrap();
    //     let pickup_index = deliveries
    //         .iter()
    //         .position(|checkpoint| checkpoint.pickup_cargo.contains(chosen_cargo));
    //     let dropoff_index = deliveries
    //         .iter()
    //         .position(|checkpoint| checkpoint.dropoff_cargo.contains(chosen_cargo));
    //     assert!(pickup_index < dropoff_index);
    //
    //     return Some(out);
    // }
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
        booking_data: Vec<Booking>,
        planning_period: (Time, Time),
    ) -> PyResult<Self> {
        // Calculate terminal_open_intervals
        let mut terminal_open_intervals = HashMap::new();
        for (terminal, (opening_time, closing_time)) in terminal_data.iter() {
            // If it is a valid interval, create
            let interval = interval_or_error(*opening_time, *closing_time)?;
            // TODO: make opening and closing times repeat day on day
            // TODO: if you do that, be sure to set the starting point to be sane (and
            // not e.g. 0 unix time) to avoid considering really old time intervals
            let intervals = IntervalChain::from_interval(interval);
            terminal_open_intervals.insert(*terminal, intervals);
        }

        let mut trucks = BTreeSet::new();
        let mut truck_starting_data = HashMap::new();

        for (truck, starting_terminal) in truck_data.into_iter() {
            trucks.insert(truck);
            // TODO: in the future, find the time when a driver can start working
            // in some other way
            let start_time = terminal_open_intervals
                .get(&starting_terminal)
                .unwrap()
                .get_intervals()
                .first()
                .unwrap()
                .get_start_time();
            truck_starting_data.insert(truck, (start_time, starting_terminal));
        }

        // Calculate pickup and dropoff times
        let mut pickup_times = HashMap::new();
        let mut dropoff_times = HashMap::new();

        let mut cargo_booking_info = HashMap::new();
        let mut cargo_by_terminals = HashMap::new();

        let mut driving_times_map: DrivingTimesMap = HashMap::new();

        for booking in booking_data.iter() {
            let cargo = booking.cargo;
            // Pickup intervals that don't consider terminal opening times
            let raw_pickup_intervals = IntervalChain::from_interval(interval_or_error(
                booking.pickup_open_time,
                booking.pickup_close_time,
            )?);
            let raw_dropoff_intervals = IntervalChain::from_interval(interval_or_error(
                booking.dropoff_open_time,
                booking.dropoff_close_time,
            )?);

            let from_terminal_open_intervals =
                terminal_open_intervals.get(&booking.from_terminal).unwrap();

            let to_terminal_open_intervals =
                terminal_open_intervals.get(&booking.to_terminal).unwrap();

            let pickup_intervals = from_terminal_open_intervals.intersect(&raw_pickup_intervals);
            let dropoff_intervals = to_terminal_open_intervals.intersect(&raw_dropoff_intervals);

            pickup_times.insert(cargo, pickup_intervals);
            dropoff_times.insert(cargo, dropoff_intervals);

            // Record driving times on direct routes
            driving_times_map.insert(
                (booking.from_terminal, booking.to_terminal),
                booking.direct_driving_time,
            );
            // TODO: might need to remove this assumption that time from
            // `from` to `to` is the same as from `to` to `from`
            driving_times_map.insert(
                (booking.to_terminal, booking.from_terminal),
                booking.direct_driving_time,
            );

            // Update delivery info
            let booking_info = BookingInformation {
                from: booking.from_terminal,
                to: booking.to_terminal,
            };
            cargo_by_terminals
                .entry((booking_info.from, booking_info.to))
                .or_insert(BTreeSet::new())
                .insert(cargo);
            cargo_booking_info.insert(cargo, booking_info);
        }

        Ok(Self {
            driving_times_cache: DrivingTimesCache::from_map(driving_times_map),
            cargo_by_terminals,
            pickup_times,
            dropoff_times,
            cargo_booking_info,
            terminals: terminal_data.keys().cloned().collect(),
            trucks,
            truck_starting_data,
            planning_period: interval_or_error(planning_period.0, planning_period.1)?,
        })
    }

    /// Creates an empty schedule
    pub fn empty_schedule(&self) -> Schedule {
        Schedule {
            // Create empty checkpoints for each truck
            truck_checkpoints: self.trucks.iter().map(|truck| (*truck, vec![])).collect(),
            scheduled_cargo_truck: BTreeMap::new(),
        }
    }

    /// Gets a random neighbour for a schedule.
    /// Note that the neighbours might not be sampled uniformly.
    /// Pick an action type and try to execute it randomly up to
    /// `num_tries_per_action` times. If this fails, pick another action type and repeat.
    /// This helps to keep frequency of selecting each action type similar to what is expected,
    /// despite some action types failing more often than others
    pub fn get_schedule_neighbour(
        &mut self,
        schedule: &Schedule,
        num_tries_per_action: usize,
    ) -> Schedule {
        let mut rng = rand::rng();

        loop {
            // Randomly decide what we want to do
            // Prioritise adding and updating checkpoints because we want to explore more of those
            // options, and also because adding a checkpoint might fail, but removing is a lot less likely to fail
            let action_index = rng.random_range(0..4);

            // Try executing this action type a few times
            for _ in 0..num_tries_per_action {
                let new_schedule = match action_index {
                    0..1 => self.remove_random_checkpoint(schedule, &mut rng),
                    1..2 => self.add_random_checkpoint(schedule, &mut rng),
                    2..3 => self.remove_random_delivery(schedule, &mut rng),
                    3..4 => self.add_random_delivery(schedule, &mut rng),
                    _ => unreachable!(),
                };
                if let Some(new_schedule) = new_schedule {
                    return new_schedule;
                }
            }
        }
    }
}
