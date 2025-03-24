mod schedule;

use schedule::schedule::{Schedule, ScheduleGenerator, TransportRequest};

use pyo3::prelude::*;

/// The module for handling schedules
#[pymodule]
fn chameleon_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TransportRequest>()?;
    m.add_class::<Schedule>()?;
    m.add_class::<ScheduleGenerator>()?;
    Ok(())
}
