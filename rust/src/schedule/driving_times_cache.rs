use std::collections::BTreeMap;

use super::common_types::{NonNegativeTimeDelta, Terminal};

type DrivingTimesMap = BTreeMap<(Terminal, Terminal), NonNegativeTimeDelta>;
/// A map from (from_terminal, to_terminal) to cached driving times
#[derive(PartialEq, Eq, Debug)]
pub struct DrivingTimesCache {
    // NOTE: assumes that driving from A to B might take a different time than
    // driving from B to A
    data: DrivingTimesMap,
}

impl DrivingTimesCache {
    pub fn new() -> Self {
        Self {
            data: DrivingTimesMap::new(),
        }
    }
    pub fn from_map(map: DrivingTimesMap) -> Self {
        Self { data: map }
    }

    pub fn get_driving_time(&mut self, from: Terminal, to: Terminal) -> NonNegativeTimeDelta {
        if from == to {
            return 0;
        }

        // Get cached or recalculate cache
        let out = self
            .data
            .entry((from, to))
            .or_insert_with(|| {
                // TODO: add a way to do this
                unimplemented!(
                    "Being able to get driving times on-demand hasn't been implemented yet. Requested driving time {:?}->{:?}", from, to
                );
            })
            .to_owned();

        out
    }
}
