mod schedule;

use schedule::schedule::{Booking, Schedule, ScheduleGenerator};

use pyo3::prelude::*;

/// The module for handling schedules
#[pymodule]
fn chameleon_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Booking>()?;
    m.add_class::<Schedule>()?;
    m.add_class::<ScheduleGenerator>()?;
    Ok(())
}
