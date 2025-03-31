mod schedule;

use schedule::schedule::{Booking, Cargo, Schedule, ScheduleGenerator, Terminal, Truck};

use pyo3::prelude::*;

/// The module for handling schedules
#[pymodule]
fn chameleon_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Truck>()?;
    m.add_class::<Cargo>()?;
    m.add_class::<Terminal>()?;
    m.add_class::<Booking>()?;
    m.add_class::<Schedule>()?;
    m.add_class::<ScheduleGenerator>()?;
    Ok(())
}
