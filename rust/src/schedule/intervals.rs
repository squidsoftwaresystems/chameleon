use std::cmp::max;
use std::cmp::min;

// TODO: convert these to struct Time(u64) and TimeDelta(i64)
// to make it more fool-proof
pub type Time = u64;
pub type TimeDelta = i64;

pub type Interval = IntervalWithData<()>;
pub type IntervalChain = IntervalWithDataChain<()>;

#[derive(PartialEq, Eq, Hash, Clone, Debug, PartialOrd, Ord)]
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
        return self.start_time;
    }

    pub fn get_end_time(&self) -> Time {
        return self.end_time;
    }

    pub fn get_duration(&self) -> TimeDelta {
        return (self.end_time - self.start_time).try_into().unwrap();
    }

    pub fn get_additional_data(&self) -> &T {
        return &self.additional_data;
    }

    pub fn get_additional_data_mut(&mut self) -> &mut T {
        return &mut self.additional_data;
    }

    /// Try to create a copy of self with offset start and end times.
    /// If the result is invalid (e.g. length is non-positive), return None
    pub fn reschedule(&self, start_change: TimeDelta, end_change: TimeDelta) -> Option<Self> {
        Self::new(
            // Try add start_change, return None if overflows
            self.start_time.checked_add_signed(start_change)?,
            self.end_time.checked_add_signed(end_change)?,
            self.additional_data.clone(),
        )
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
#[derive(Clone, Debug)]
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
}
