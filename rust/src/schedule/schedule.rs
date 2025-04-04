use std::{
    cmp::max,
    collections::{BTreeMap, BTreeSet, HashMap},
};

use pyo3::{exceptions::PyTypeError, pyclass, pymethods, FromPyObject, PyResult};
use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;

use super::{counter_mapper::CounterMapper, intervals::*};

type TerminalID = String;
type CargoID = String;
type TruckID = String;

// NOTE: this prevents recognising them as the same type, and e.g.
// assigning a truck to a cargo by mistake
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Debug)]
pub struct Terminal(usize);

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Debug)]
pub struct Cargo(usize);

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Debug)]
pub struct Truck(usize);

#[pyclass]
#[derive(FromPyObject)]
/// The representation of request for delivery that the rust code gets from python
pub struct Booking {
    #[pyo3(get, set)]
    cargo: CargoID,
    #[pyo3(get, set)]
    from_terminal: TerminalID,
    #[pyo3(get, set)]
    to_terminal: TerminalID,
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
        cargo: CargoID,
        from_terminal: TerminalID,
        to_terminal: TerminalID,
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
    /// The list of checkpoints for each truck.
    /// An invariant we are maintaining is that the times of checkpoints
    /// in each Vec are in a strictly ascending order and no two consecutive
    /// checkpoints have the same terminal. This includes the implicit
    /// first checkpoint representing the first terminal
    truck_checkpoints: BTreeMap<Truck, Vec<Checkpoint>>,

    /// Map from cargo that was scheduled to truck taking it
    scheduled_cargo_truck: BTreeMap<Cargo, Truck>,

    /// Total length of time this truck is driving under this schedule
    truck_driving_times: BTreeMap<Truck, TimeDelta>,
}

impl Schedule {
    fn get_checkpoint_mut(
        &mut self,
        truck: Truck,
        checkpoint_index: usize,
    ) -> Option<&mut Checkpoint> {
        self.truck_checkpoints
            .get_mut(&truck)?
            .get_mut(checkpoint_index)
    }

    /// Given a checkpoint, finds the checkpoints directly before and after it
    fn get_prev_and_next_checkpoints(
        &self,
        truck: Truck,
        checkpoint: &Checkpoint,
    ) -> (Option<&Checkpoint>, Option<&Checkpoint>) {
        let checkpoints = self.truck_checkpoints.get(&truck).unwrap();

        let time = checkpoint.time;

        // NOTE: this inequality is strict since we don't expect
        // 2 checkpoints to have the same time
        let prev = checkpoints
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.time < time);
        let next = checkpoints.iter().find(|checkpoint| checkpoint.time > time);

        if let Some(prev) = prev {
            assert!(prev.time < time);
        }

        if let Some(next) = next {
            assert!(time < next.time);
        }

        (prev, next)
    }

    /// Given a time, finds the gap between two neighbouring checkpoints
    /// containing this time
    /// In other words, given a time, identify an interval
    /// [prev_checkpoint, next_checkpoint) of consecutive checkpoints so that the
    /// time is in [prev_checkpoint.time, next_checkpoint.time). This also
    /// works for implicit checkpoints, such as the ones representing the start and end of day.
    fn get_checkpoints_around_gap(
        &self,
        truck: Truck,
        time: Time,
    ) -> (Option<&Checkpoint>, Option<&Checkpoint>) {
        let checkpoints = self.truck_checkpoints.get(&truck).unwrap();

        // NOTE: this inequality is weak so that we capture the half-open
        // interval [prev_checkpoint.time, next_checkpoint.time)
        let prev = checkpoints
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.time <= time);
        let next = checkpoints.iter().find(|checkpoint| checkpoint.time > time);

        if let Some(prev) = prev {
            assert!(prev.time <= time);
        }

        if let Some(next) = next {
            assert!(time < next.time);
        }

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

    rng: Xoshiro256PlusPlus,
}

impl ScheduleGenerator {
    fn assert_truck_checkpoints_invariant(&self, schedule: &Schedule, truck: Truck) {
        let checkpoints = schedule.truck_checkpoints.get(&truck).unwrap();
        // Make sure that we don't have 2 checkpoints in the same terminal
        // together
        assert!(checkpoints
            .windows(2)
            .all(|checkpoints| checkpoints[0].terminal != checkpoints[1].terminal));

        // Also check the starting terminal
        if let Some(first_checkpoint) = checkpoints.first() {
            assert!(first_checkpoint.terminal != self.truck_starting_data.get(&truck).unwrap().1);
        }

        // Make sure that the times are still in strictly ascending order of time
        // https://stackoverflow.com/questions/51272571/how-do-i-check-if-a-slice-is-sorted
        assert!(checkpoints
            .windows(2)
            .all(|checkpoints| checkpoints[0].time < checkpoints[1].time));
    }

    /// Get driving time between `from` and `to`.
    /// If `from` is None, assume it is the starting terminal
    /// If `to` is None, assume that there is no restriction
    /// on what `to` is, and so we can stay at `from` for 0 driving time
    fn get_driving_time(
        &mut self,
        from: Option<Terminal>,
        to: Option<Terminal>,
        truck: Truck,
    ) -> TimeDelta {
        let from = from.unwrap_or_else(|| self.truck_starting_data.get(&truck).unwrap().1);
        if let Some(to) = to {
            self.driving_times_cache.get_driving_time(from, to)
        } else {
            0
        }
    }

    /// Find the interval between `prev_checkpoint.time` and `next_checkpoint.time`
    /// containing the times during which we can put a checkpoint in `new_terminal`
    /// and have time to drive from `prev_checkpoint.terminal` to `new_terminal` and
    /// from `new_terminal` to `next_checkpoint.terminal`
    fn get_driving_time_constraints(
        &mut self,
        truck: Truck,
        prev_checkpoint: Option<&Checkpoint>,
        next_checkpoint: Option<&Checkpoint>,
        new_terminal: Terminal,
    ) -> Option<Interval> {
        let prev_terminal = prev_checkpoint.map(|checkpoint| checkpoint.terminal);
        let next_terminal = next_checkpoint.map(|checkpoint| checkpoint.terminal);

        // TODO: add proper upper bound on time
        let prev_time = prev_checkpoint
            .map(|checkpoint| checkpoint.time)
            .unwrap_or(self.planning_period.get_start_time());
        let next_time = next_checkpoint
            .map(|checkpoint| checkpoint.time)
            .unwrap_or(self.planning_period.get_end_time());

        let driving_time1 = self.get_driving_time(prev_terminal, Some(new_terminal), truck);
        let driving_time2 = self.get_driving_time(Some(new_terminal), next_terminal, truck);

        let earliest_checkpoint_time = prev_time.checked_add_signed(driving_time1).unwrap();
        let latest_checkpoint_time = next_time.checked_add_signed(-driving_time2).unwrap();

        Interval::new(earliest_checkpoint_time, latest_checkpoint_time, ())
    }

    /// Given a previous and next checkpoints, find
    /// what terminals those correspond to. Handles cases when
    /// there is no checkpoint, and suggests correct terminals
    fn get_gap_terminals(
        &self,
        truck: Truck,
        prev_checkpoint: Option<&Checkpoint>,
        next_checkpoint: Option<&Checkpoint>,
    ) -> (Terminal, Option<Terminal>) {
        let prev_terminal = if let Some(prev) = prev_checkpoint {
            prev.terminal
        } else {
            // Before first interval
            self.truck_starting_data.get(&truck).unwrap().1
        };

        let next_terminal = if let Some(next) = next_checkpoint {
            Some(next.terminal)
        } else {
            None
        };
        (prev_terminal, next_terminal)
    }

    /// Return (`truck`, index in `checkpoints`) for a random checkpoint
    fn get_random_checkpoint<'a>(
        &mut self,
        schedule: &'a Schedule,
    ) -> Option<(&'a Checkpoint, Truck, usize)> {
        let rng = &mut self.rng;
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
    fn add_random_checkpoint(&mut self, schedule: &Schedule) -> Option<Schedule> {
        // TODO: pick so that empty trucks have a higher chance of being picked
        let truck = *self.trucks.iter().choose(&mut self.rng)?;

        // We want to pick an interval between checkpoints to which we will add a new checkpoint
        // Pick a time uniformly at random and pick the interval containing that time,
        // so that large intervals are more likely to be chosen, breaking up large intervals.
        let planning_start_time = self.planning_period.get_start_time();
        let planning_end_time = self.planning_period.get_end_time();
        let time_to_identify_gap =
            (planning_start_time..planning_end_time).choose(&mut self.rng)?;
        let (prev_checkpoint, next_checkpoint) =
            schedule.get_checkpoints_around_gap(truck, time_to_identify_gap);
        let (prev_terminal, next_terminal) =
            self.get_gap_terminals(truck, prev_checkpoint, next_checkpoint);

        // TODO: pick a time and a terminal based on whether we can pick up or drop off cargo at the time
        // For now, we just picked at random and stick with it

        // disallow picking same terminal as the one before or after, since we want to associate
        // gaps between checkpoints with driving
        let new_terminal = *self
            .terminals
            .iter()
            .filter(|terminal| **terminal != prev_terminal && Some(**terminal) != next_terminal)
            .choose(&mut self.rng)?;

        let allowed_time_interval = self.get_driving_time_constraints(
            truck,
            prev_checkpoint,
            next_checkpoint,
            new_terminal,
        )?;

        // Otherwise, schedule a checkpoint in this time, if we can
        let new_time = allowed_time_interval.random_time(&mut self.rng);

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

        self.assert_truck_checkpoints_invariant(&out, truck);

        // Increase the cached driving time
        // We are replacing driving A->C with driving A->B->C
        let mut driving_time = *out.truck_driving_times.get(&truck).unwrap();
        let prev_terminal = Some(prev_terminal);
        let terminal = Some(new_terminal);

        let time_a_to_c = self.get_driving_time(prev_terminal, next_terminal, truck);
        let time_a_to_b = self.get_driving_time(prev_terminal, terminal, truck);
        let time_b_to_c = self.get_driving_time(terminal, next_terminal, truck);

        driving_time -= time_a_to_c;
        driving_time += time_a_to_b + time_b_to_c;
        assert!(driving_time >= 0);
        out.truck_driving_times.insert(truck, driving_time);

        return Some(out);
    }

    /// Pick a random checkpoint and remove it
    fn remove_random_checkpoint(&mut self, schedule: &Schedule) -> Option<Schedule> {
        let (checkpoint, chosen_truck, chosen_index) = self.get_random_checkpoint(schedule)?;
        // To avoid easily undoing progress, only allow removing checkpoint if there is no cargo
        // pickup or dropoff in it

        // TODO: maybe it is faster to list all checkpoints without pickups or dropoffs and
        // then pick randomly among them
        if !checkpoint.pickup_cargo.is_empty() || !checkpoint.dropoff_cargo.is_empty() {
            return None;
        }

        // TODO: make the clones cheaper
        let mut out = schedule.clone();

        // Check that removing this checkpoint won't leave us
        // with 2 consecutive checkpoints with the same terminals
        let (prev_checkpoint, next_checkpoint) =
            schedule.get_prev_and_next_checkpoints(chosen_truck, checkpoint);
        let (prev_terminal, next_terminal) =
            self.get_gap_terminals(chosen_truck, prev_checkpoint, next_checkpoint);
        if Some(prev_terminal) == next_terminal {
            return None;
        }

        // Remove the checkpoint
        out.truck_checkpoints
            .get_mut(&chosen_truck)
            .unwrap()
            .remove(chosen_index);

        self.assert_truck_checkpoints_invariant(&out, chosen_truck);

        // Reduce the cached driving time
        // We are replacing driving A->B->C with driving A->C
        let mut driving_time = *out.truck_driving_times.get(&chosen_truck).unwrap();
        let (prev_checkpoint, next_checkpoint) =
            schedule.get_prev_and_next_checkpoints(chosen_truck, checkpoint);
        let prev_terminal = prev_checkpoint.map(|c| c.terminal);
        let terminal = Some(checkpoint.terminal);
        let next_terminal = next_checkpoint.map(|c| c.terminal);

        let time_a_to_c = self.get_driving_time(prev_terminal, next_terminal, chosen_truck);
        let time_a_to_b = self.get_driving_time(prev_terminal, terminal, chosen_truck);
        let time_b_to_c = self.get_driving_time(terminal, next_terminal, chosen_truck);

        driving_time += time_a_to_c;
        driving_time -= time_a_to_b + time_b_to_c;
        assert!(driving_time >= 0);
        out.truck_driving_times.insert(chosen_truck, driving_time);

        return Some(out);
    }

    /// Remove pickup and dropoff for a piece of cargo
    fn remove_random_delivery(&mut self, schedule: &Schedule) -> Option<Schedule> {
        let rng = &mut self.rng;
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
        truck: Truck,
        old_checkpoint_index: usize,
        new_pickup: &BTreeSet<Cargo>,
        new_dropoff: &BTreeSet<Cargo>,
    ) -> Option<Time> {
        let old_checkpoint = schedule
            .truck_checkpoints
            .get(&truck)
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
            schedule.get_prev_and_next_checkpoints(truck, old_checkpoint);

        let driving_restriction_intervals =
            IntervalWithDataChain::from_interval(self.get_driving_time_constraints(
                truck,
                checkpoint_before,
                checkpoint_after,
                old_checkpoint.terminal,
            )?);

        let allowed_intervals = [
            pickup_restriction_intervals,
            dropoff_restriction_intervals,
            driving_restriction_intervals,
            IntervalWithDataChain::from_interval(self.planning_period.clone()),
        ]
        .iter()
        .intersect_all();

        let new_interval = allowed_intervals
            .get_intervals()
            .iter()
            .choose(&mut self.rng)?;
        let new_time =
            (new_interval.get_start_time()..new_interval.get_end_time()).choose(&mut self.rng)?;

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
    fn add_random_delivery(&mut self, schedule: &Schedule) -> Option<Schedule> {
        let rng = &mut self.rng;
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
            *truck,
            start_checkpoint_index,
            &new_start_checkpoint_pickup,
            &start_checkpoint.dropoff_cargo,
        )?;
        let new_start_checkpoint = out
            .get_checkpoint_mut(*truck, start_checkpoint_index)
            .unwrap();
        new_start_checkpoint.pickup_cargo.insert(chosen_cargo);
        new_start_checkpoint.time = new_start_checkpoint_time;

        let new_end_checkpoint_time = self.find_random_reschedule_time(
            &out,
            *truck,
            end_checkpoint_index,
            &end_checkpoint.pickup_cargo,
            &new_end_checkpoint_dropoff,
        )?;
        let new_end_checkpoint = out
            .get_checkpoint_mut(*truck, end_checkpoint_index)
            .unwrap();
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
        terminal_data: HashMap<TerminalID, (Time, Time)>,
        truck_data: HashMap<TruckID, TerminalID>,
        booking_data: Vec<Booking>,
        planning_period: (Time, Time),
    ) -> PyResult<Self> {
        // We want to map between the internally-used
        // integer ids and the externally-used String ids.
        // This is done because it is easier to deal with
        // integers and ownership, while Strings would make
        // maintenance a bit more tricky
        let mut terminal_mapper = CounterMapper::new();
        let mut cargo_mapper = CounterMapper::new();
        let mut truck_mapper = CounterMapper::new();

        let planning_period = interval_or_error(planning_period.0, planning_period.1)?;
        let planning_period_as_interval_chain =
            IntervalChain::from_interval(planning_period.clone());

        // Calculate terminal_open_intervals
        let mut terminal_open_intervals = HashMap::new();
        for (terminal_id, (opening_time, closing_time)) in terminal_data.iter() {
            let terminal = Terminal(terminal_mapper.add_or_find(terminal_id));
            // If it is a valid interval, create
            let interval = interval_or_error(*opening_time, *closing_time)?;
            // TODO: make opening and closing times repeat day on day
            // TODO: if you do that, be sure to set the starting point to be sane (and
            // not e.g. 0 unix time) to avoid considering really old time intervals
            let intervals = IntervalChain::from_interval(interval);
            terminal_open_intervals.insert(terminal, intervals);
        }

        let mut trucks = BTreeSet::new();
        let mut truck_starting_data = HashMap::new();

        for (truck_id, starting_terminal_id) in truck_data.into_iter() {
            let truck = Truck(truck_mapper.add_or_find(&truck_id));
            let starting_terminal = Terminal(terminal_mapper.add_or_find(&starting_terminal_id));

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

        let mut terminals = BTreeSet::new();

        // Calculate pickup and dropoff times
        let mut pickup_times = HashMap::new();
        let mut dropoff_times = HashMap::new();

        let mut cargo_booking_info = HashMap::new();
        let mut cargo_by_terminals = HashMap::new();

        let mut driving_times_map: DrivingTimesMap = HashMap::new();

        for booking in booking_data.iter() {
            // Remove irrelevant bookings
            // Note that this also includes the bookings that are too far in the future -
            // we are not anticipating anything after the planning period ends.
            // We want to run this algorithm with a relatively large look-ahead,
            // so that all relevant bookings are within the planning_period. In
            // this case, if our plan near the end of the period is suboptimal
            // because we didn't anticipate bookings after the end of
            // planning_period, that is not an issue: any plans for that time
            // become stale as the situation changes

            // TODO: we still might want to consider this in order to e.g.
            // handle scheduling not-urgent containers more frequently

            // To do that, first shrink the intervals, and then remove the empty ones

            let from_terminal = Terminal(terminal_mapper.add_or_find(&booking.from_terminal));
            let to_terminal = Terminal(terminal_mapper.add_or_find(&booking.to_terminal));

            let pickup_intervals = [
                terminal_open_intervals.get(&from_terminal).unwrap().clone(),
                IntervalChain::from_interval(interval_or_error(
                    booking.pickup_open_time,
                    booking.pickup_close_time,
                )?),
                planning_period_as_interval_chain.clone(),
            ]
            .iter()
            .intersect_all();

            let dropoff_intervals = [
                terminal_open_intervals.get(&to_terminal).unwrap().clone(),
                IntervalChain::from_interval(interval_or_error(
                    booking.dropoff_open_time,
                    booking.dropoff_close_time,
                )?),
                planning_period_as_interval_chain.clone(),
            ]
            .iter()
            .intersect_all();

            // Remove the deliveries we can't do
            if pickup_intervals.is_empty() || dropoff_intervals.is_empty() {
                continue;
            }

            // Only add terminals which are referenced in a relevant booking
            terminals.insert(from_terminal);
            terminals.insert(to_terminal);

            let cargo = Cargo(cargo_mapper.add_or_find(&booking.cargo));
            pickup_times.insert(cargo, pickup_intervals);
            dropoff_times.insert(cargo, dropoff_intervals);

            // Record driving times on direct routes
            driving_times_map.insert((from_terminal, to_terminal), booking.direct_driving_time);

            // Update delivery info
            let booking_info = BookingInformation {
                from: from_terminal,
                to: to_terminal,
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
            terminals,
            trucks,
            truck_starting_data,
            planning_period,
            rng: Xoshiro256PlusPlus::seed_from_u64(0),
        })
    }

    /// Creates an empty schedule
    pub fn empty_schedule(&self) -> Schedule {
        Schedule {
            // Create empty checkpoints for each truck
            truck_checkpoints: self.trucks.iter().map(|truck| (*truck, vec![])).collect(),
            scheduled_cargo_truck: BTreeMap::new(),
            // Each truck drives 0 distance by default, simply staying where it is
            truck_driving_times: self.trucks.iter().map(|truck| (*truck, 0)).collect(),
        }
    }

    /// Reseeds internal RNG
    pub fn seed(&mut self, seed: u64) {
        self.rng = Xoshiro256PlusPlus::seed_from_u64(seed);
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
        loop {
            // Randomly decide what we want to do
            // Prioritise adding and updating checkpoints because we want to explore more of those
            // options, and also because adding a checkpoint might fail, but removing is a lot less likely to fail
            let action_index = self.rng.random_range(0..4);

            // Try executing this action type a few times
            for _ in 0..num_tries_per_action {
                let new_schedule = match action_index {
                    0..1 => self.remove_random_checkpoint(schedule),
                    1..2 => self.add_random_checkpoint(schedule),
                    2..3 => self.remove_random_delivery(schedule),
                    3..4 => self.add_random_delivery(schedule),
                    _ => unreachable!(),
                };
                if let Some(new_schedule) = new_schedule {
                    return new_schedule;
                }
            }
        }
    }

    /// Returns a score representing how good the Schedule is
    /// The score is a vector of numbers in interval [0, 1] and each
    /// represent a different criterion by which the solution can be judged.
    /// Higher score is better
    pub fn scores(&mut self, schedule: &Schedule) -> Vec<f64> {
        // Maximise the number of deliveries
        let num_deliveries: usize = schedule.scheduled_cargo_truck.len();
        // Minimise the number of trucks required
        let num_free_trucks: usize = schedule
            .truck_checkpoints
            .values()
            .filter(|checkpoints| checkpoints.is_empty())
            .count();

        // Sum of minimal driving times needed to deliver each piece of cargo that
        // has been delivered;
        // this is a very simplistic lower bound
        let min_driving_time: TimeDelta = schedule
            .scheduled_cargo_truck
            .keys()
            .map(|cargo| {
                let booking_info = self.cargo_booking_info.get(cargo).unwrap();
                self.driving_times_cache
                    .get_driving_time(booking_info.from, booking_info.to)
            })
            .sum();

        // Total driving time
        let total_driving_time: TimeDelta = schedule.truck_driving_times.values().copied().sum();

        // Proportion of deliveries made
        let deliveries_proportion =
            (num_deliveries as f64) / (self.cargo_booking_info.len() as f64);

        // Proportion of trucks that are free
        let free_trucks_proportion = (num_free_trucks as f64) / (self.trucks.len() as f64);

        // The smaller the total driving time, the closer this is to 1
        // Prevent division by 0
        let driving_time_score = (min_driving_time as f64) / (max(total_driving_time, 1) as f64);

        vec![
            deliveries_proportion,
            free_trucks_proportion,
            driving_time_score,
        ]
    }
}
