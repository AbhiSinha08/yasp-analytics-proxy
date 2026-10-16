//! Local gateway host: environment resources, logging, and process lifecycle.

mod host;

fn main() {
    if let Err(error) = host::run() {
        eprintln!("YASP startup failed: {error}");
        std::process::exit(1);
    }
}
