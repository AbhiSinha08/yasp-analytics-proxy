//! Check libpython linking and imports; this is not the engine's hook runtime.

use pyo3::prelude::*;

fn main() -> PyResult<()> {
    let module_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "decimal".to_owned());

    Python::initialize();
    Python::attach(|py| {
        let sys = py.import("sys")?;
        println!("Python: {}", sys.getattr("version")?);
        println!("Import paths: {}", sys.getattr("path")?);

        let module = py.import(&module_name)?;
        println!("Imported: {}", module.getattr("__name__")?);
        println!("Origin: {}", module.getattr("__spec__")?.getattr("origin")?);
        Ok(())
    })
}
