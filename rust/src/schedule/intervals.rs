use std::cmp::max;
use std::cmp::min;

use rand::seq::IteratorRandom;
use rand_xoshiro::Xoshiro256PlusPlus;

use super::common_types::NonNegativeTimeDelta;
use super::common_types::Time;

pub type Interval = IntervalWithData<()>;
pub type IntervalChain = IntervalWithDataChain<()>;

#[derive(PartialEq, Eq, Clone, Debug, PartialOrd, Ord)]
/// A non-empty interval of time
pub struct IntervalWithData<T>
where
    T: Eq,
{
    start_time: Time,
    end_time: Time,
    additional_data: T,
}

impl<T: Clone + Eq> IntervalWithData<T> {
    /// Try to create an interval, return None if its length is non-positive
    pub fn new(start_time: Time, end_time: Time, additional_data: T) -> Option<Self> {
        if start_time >= end_time {
            None
        } else {
            Some(Self {
                start_time,
                end_time,
                additional_data,
            })
        }
    }

    pub fn get_start_time(&self) -> Time {
        self.start_time
    }

    pub fn get_end_time(&self) -> Time {
        self.end_time
    }

    pub fn get_duration(&self) -> NonNegativeTimeDelta {
        (self.end_time - self.start_time).try_into().unwrap()
    }

    pub fn get_additional_data(&self) -> &T {
        &self.additional_data
    }

    pub fn get_additional_data_mut(&mut self) -> &mut T {
        &mut self.additional_data
    }

    pub fn random_time(&self, rng: &mut Xoshiro256PlusPlus) -> Time {
        // the interval can't be empty
        (self.start_time..self.end_time).choose(rng).unwrap()
    }

    pub fn map_data<U: Eq>(&self, new_data: U) -> IntervalWithData<U> {
        IntervalWithData {
            start_time: self.start_time,
            end_time: self.end_time,
            additional_data: new_data,
        }
    }

    pub fn remove_additional_data(&self) -> Interval {
        self.map_data(())
    }
}

/// A list of non-overlapping intervals in an increasing order
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntervalWithDataChain<T>
where
    T: Eq,
{
    intervals: Vec<IntervalWithData<T>>,
}

impl<T: Clone + Eq> IntervalWithDataChain<T> {
    /// Create an empty interval chain
    pub fn new() -> Self {
        IntervalWithDataChain { intervals: vec![] }
    }

    pub fn from_interval(interval: IntervalWithData<T>) -> Self {
        IntervalWithDataChain {
            intervals: vec![interval],
        }
    }

    pub fn from_intervals(intervals: Vec<IntervalWithData<T>>) -> Self {
        IntervalWithDataChain { intervals }
    }

    /// Create an IntervalChain that is the intersection of two IntervalChains,
    /// that is sub-intervals occurring in both. Keeps additional information of `self`
    pub fn intersect<U: Eq>(&self, other: &IntervalWithDataChain<U>) -> IntervalWithDataChain<T> {
        // Lock-step with `other`, adding intervals if they intersect
        let mut out = IntervalWithDataChain::new();

        // Take iterators
        let mut self_it = self.intervals.iter();
        let mut other_it = other.intervals.iter();

        // While we have intervals left over in both
        // https://stackoverflow.com/questions/71814858/using-while-let-with-two-variables-simultaneously#71814902
        while let Some((self_interval, other_interval)) = self_it.next().zip(other_it.next()) {
            // Add the intersection if they intersect
            if other_interval.end_time > self_interval.start_time
                && self_interval.end_time > other_interval.start_time
            {
                out.intervals.push(IntervalWithData {
                    start_time: max(self_interval.start_time, other_interval.start_time),
                    end_time: min(self_interval.end_time, other_interval.end_time),
                    additional_data: self_interval.additional_data.clone(),
                });
            }
        }
        return out;
    }

    /// Checks whether all the intervals in this chain are contained in `other`
    pub fn contained_in<U: Eq>(&self, other: &IntervalWithData<U>) -> bool {
        if self.intervals.is_empty() {
            return true;
        } else {
            return other.start_time <= self.intervals.first().unwrap().start_time
                && self.intervals.first().unwrap().end_time <= other.end_time;
        }
    }

    /// Calculates `other \ self` as another interval chain;
    /// in other words, finds the gaps between intervals with `other`
    /// signifying the endpoints. Ignores everything outside of `other` endpoints
    /// The additional_data of output contains copies of additional_data of self.intervals
    /// before and after (if any)
    pub fn gaps<U: Eq>(
        &self,
        other: &IntervalWithData<U>,
    ) -> IntervalWithDataChain<(Option<T>, Option<T>)> {
        let start_time = other.start_time;
        let end_time = other.end_time;

        let mut out: Vec<IntervalWithData<(Option<T>, Option<T>)>> = vec![];

        let mut previous_additional_data: Option<T> = None;
        // The loop won't consider intervals starting before this time
        let mut previous_end_time = start_time;

        for interval in self.intervals.iter() {
            // Avoid creating zero-width gaps
            if interval.start_time <= previous_end_time {
                continue;
            }
            // Otherwise, add the gap [previous_end_time, interval.start_time]
            out.push(IntervalWithData {
                start_time: previous_end_time,
                end_time: interval.start_time,
                additional_data: (
                    previous_additional_data,
                    Some(interval.additional_data.clone()),
                ),
            });
            previous_additional_data = Some(interval.additional_data.clone());
            previous_end_time = interval.end_time;

            // Avoid going past end_time
            if end_time <= interval.end_time {
                break;
            }
        }

        // Could have the last interval remaining
        if previous_end_time < end_time {
            out.push(IntervalWithData {
                start_time: previous_end_time,
                end_time,
                additional_data: (previous_additional_data, None),
            });
        }

        return IntervalWithDataChain::from_intervals(out);
    }

    pub fn get_intervals(&self) -> &Vec<IntervalWithData<T>> {
        return &self.intervals;
    }

    pub fn get_intervals_mut(&mut self) -> &mut Vec<IntervalWithData<T>> {
        return &mut self.intervals;
    }

    /// Remove interval at index `index`, panic if index out of bounds
    pub fn remove(&mut self, index: usize) -> IntervalWithData<T> {
        return self.intervals.remove(index);
    }

    /// Inserts a transition, returns true if and only if
    /// this addition was valid (i.e. non-overlapping)
    pub fn try_add(&mut self, new: IntervalWithData<T>) -> bool {
        // Find index at which it can be put in
        // first index which is after `new`
        let index = self
            .intervals
            .iter()
            .position(|interval| interval.start_time >= new.end_time);

        if let Some(index) = index {
            // If a previous interval exists, check that `new`
            // occurs after the previous interval
            if index > 0 {
                let prev = self.intervals.get(index - 1).unwrap();
                if !(prev.end_time <= new.start_time) {
                    return false;
                }
            }
            self.intervals.insert(index, new);
            return true;
        } else {
            // It should be the last interval
            self.intervals.push(new);
            return true;
        }
    }

    pub fn total_length(&self) -> NonNegativeTimeDelta {
        self.intervals
            .iter()
            .map(|interval| interval.get_duration())
            .sum()
    }

    /// Whether the total length of the intervals is 0
    pub fn is_empty(&self) -> bool {
        // since the individual intervals have a positive
        // length, the only way the total length can be 0
        // is if there are no intervals
        self.intervals.is_empty()
    }
}

pub trait IntervalWithDataChainIter {
    /// Takes an iterator of IntervalWithData and returns their intersection
    fn intersect_all<'a, T>(self) -> IntervalChain
    where
        Self: Iterator<Item = &'a IntervalWithDataChain<T>> + Sized,
        T: Clone + Eq + 'a;
}

impl<It> IntervalWithDataChainIter for It
where
    Self: Iterator + Sized,
{
    fn intersect_all<'a, T>(self) -> IntervalChain
    where
        Self: Iterator<Item = &'a IntervalWithDataChain<T>> + Sized,
        T: Clone + Eq + 'a,
    {
        let largest_interval = Interval {
            start_time: Time::MIN,
            end_time: Time::MAX,
            additional_data: (),
        };
        let empty_intersection = IntervalWithDataChain::from_interval(largest_interval);
        self.fold(empty_intersection, |intervals1, intervals2| {
            intervals1.intersect(&intervals2)
        })
    }
}
