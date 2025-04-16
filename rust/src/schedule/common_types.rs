// NOTE: this prevents recognising them as the same type, and e.g.
// assigning a truck to a cargo by mistake
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Debug)]
pub struct Terminal(usize);

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Debug)]
pub struct Cargo(usize);

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Debug)]
pub struct Truck(usize);

pub trait IsID {
    fn get_id(&self) -> usize;
    fn from_id(id: usize) -> Self;
}

impl IsID for Terminal {
    fn get_id(&self) -> usize {
        self.0
    }
    fn from_id(id: usize) -> Self {
        Self(id)
    }
}

impl IsID for Cargo {
    fn get_id(&self) -> usize {
        self.0
    }
    fn from_id(id: usize) -> Self {
        Self(id)
    }
}

impl IsID for Truck {
    fn get_id(&self) -> usize {
        self.0
    }
    fn from_id(id: usize) -> Self {
        Self(id)
    }
}

// TODO: maybe convert these to struct Time(u64), TimeDelta(i64)
// and NonNegativeTimeDelta(i64)
// to make it more fool-proof

/// Time in seconds
pub type Time = u64;
/// Time duration in seconds
pub type NonNegativeTimeDelta = u64;
