//! Network-only probe of the candidate Rust driver; uses disposable fake data.

use adbc_core::{
    Connection, Database, Driver, Statement,
    options::{AdbcVersion, OptionDatabase, OptionValue},
};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{Array, Decimal128Array};
use serde_json::json;
use std::{
    env,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 || !matches!(args[3].as_str(), "connection" | "statement") {
        return Err(
            "usage: yasp-snowflake-probe DRIVER_LIBRARY LOOPBACK_PORT connection|statement".into(),
        );
    }
    let port: u16 = args[2].parse()?;
    if port == 0 {
        return Err("the disposable server requires a nonzero loopback port".into());
    }
    let mut driver = ManagedDriver::load_dynamic_from_filename(&args[1], None, AdbcVersion::V110)?;
    let options = [
        ("adbc.snowflake.sql.account", "fakesnow"),
        ("username", "fake"),
        ("password", "snow"),
        ("adbc.snowflake.sql.uri.host", "127.0.0.1"),
        ("adbc.snowflake.sql.uri.port", args[2].as_str()),
        ("adbc.snowflake.sql.uri.protocol", "http"),
        ("adbc.snowflake.sql.client_option.login_timeout", "3s"),
        ("adbc.snowflake.sql.client_option.request_timeout", "3s"),
        ("adbc.snowflake.sql.client_option.disable_telemetry", "true"),
        (
            "adbc.snowflake.sql.client_option.use_high_precision",
            "true",
        ),
    ]
    .map(|(key, value)| (OptionDatabase::Other(key.into()), OptionValue::from(value)));
    let database = driver.new_database_with_opts(options)?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement
        .set_sql_query("SELECT 12345678901234567890.123456789::NUMBER(38,9) AS D, NULL AS Z")?;
    let mut reader = statement.execute()?;
    let batch = reader.next().ok_or("missing result batch")??;
    let decimal = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or("NUMBER was not returned as Decimal128")?;
    let exact = batch.num_rows() == 1
        && decimal.precision() == 38
        && decimal.scale() == 9
        && decimal.value(0) == 12_345_678_901_234_567_890_123_456_789_i128
        && batch.column(1).is_null(0);
    drop(reader);
    drop(statement);

    let mut statement = connection.new_statement()?;
    statement.set_sql_query("SELECT SUM(RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT => 100000000))")?;
    let mut cancel_statement = statement.clone();
    let (finished, outcome) = mpsc::channel();
    thread::spawn(move || {
        let result = statement.execute().map(|reader| {
            reader
                .map(|batch| batch.map(|batch| batch.num_rows()))
                .collect::<Result<Vec<_>, _>>()
        });
        // Retain only the outcome, never native results or credentials.
        let status = match result {
            Ok(Ok(_)) => "completed".to_owned(),
            Ok(Err(error)) => format!("fetch error: {error}"),
            Err(error) => format!("execute error: {error}"),
        };
        let _ = finished.send(status);
    });
    thread::sleep(Duration::from_millis(200));
    let early = outcome.try_recv().ok();
    if let Some(status) = early {
        println!(
            "{}",
            json!({"decimal_exact": exact, "cancel_mode": args[3],
            "gate": "inconclusive", "reason": "query finished before cancellation", "outcome": status})
        );
        return Ok(());
    }
    let started = Instant::now();
    let cancel_result = if args[3] == "statement" {
        cancel_statement.cancel()
    } else {
        connection.cancel()
    };
    let cancel_ms = started.elapsed().as_millis();
    let outcome = outcome
        .recv_timeout(Duration::from_secs(15))
        .unwrap_or_else(|_| "worker did not stop within 15 seconds".into());
    println!(
        "{}",
        json!({"decimal_exact": exact, "cancel_mode": args[3],
        "cancel_returned_success": cancel_result.is_ok(), "cancel_call_ms": cancel_ms,
        "outcome": outcome})
    );
    Ok(())
}
