mod schedule;

use schedule::schedule::{PyBooking, PyTruckData, Schedule, ScheduleGenerator};

use pyo3::prelude::*;

/// The module for handling schedules
#[pymodule]
fn chameleon_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTruckData>()?;
    m.add_class::<PyBooking>()?;
    m.add_class::<Schedule>()?;
    m.add_class::<ScheduleGenerator>()?;
    Ok(())
}
