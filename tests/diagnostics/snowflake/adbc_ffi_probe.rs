//! Network gate for the owned native connector, isolated from the Rust manager.

#[path = "../../../src/backend/snowflake/connection.rs"]
#[allow(dead_code)] // Probe only the connector gates, not the runtime worker API.
mod snowflake_connection;

use arrow_array::{Array, Decimal128Array};
use serde_json::json;
use snowflake_connection::{Connection, Driver};
use std::{
    env,
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

fn query(connection: &mut Connection, sql: &str) -> Result<usize, snowflake_connection::Error> {
    let mut statement = connection.statement(sql)?;
    let reader = statement.execute()?;
    let mut rows = 0;
    for batch in reader {
        rows += batch?.num_rows();
    }
    Ok(rows)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if !(3..=4).contains(&args.len())
        || args[2].parse::<u16>().ok().filter(|p| *p != 0).is_none()
        || args
            .get(3)
            .is_some_and(|v| !matches!(v.as_str(), "no-recovery" | "prefetch" | "fetch-cancel"))
    {
        return Err(
            "usage: yasp-snowflake-ffi-probe DRIVER_LIBRARY LOOPBACK_PORT [no-recovery]".into(),
        );
    }
    let mut connection = Driver::load(Path::new(&args[1]))?.open(
        &[
            ("adbc.snowflake.sql.account", "fakesnow"),
            ("username", "fake"),
            ("password", "snow"),
            ("adbc.snowflake.sql.uri.host", "127.0.0.1"),
            ("adbc.snowflake.sql.uri.port", &args[2]),
            ("adbc.snowflake.sql.uri.protocol", "http"),
            ("adbc.snowflake.sql.client_option.login_timeout", "5s"),
            ("adbc.snowflake.sql.client_option.request_timeout", "5s"),
            ("adbc.snowflake.sql.client_option.disable_telemetry", "true"),
            (
                "adbc.snowflake.sql.client_option.use_high_precision",
                "true",
            ),
        ],
        None,
    )?;
    if let Some(mode) = args
        .get(3)
        .filter(|v| matches!(v.as_str(), "prefetch" | "fetch-cancel"))
    {
        return fetch_probe(connection, args[2].parse()?, mode);
    }
    let (exact, stale_cancel) = {
        let mut statement = connection
            .statement("SELECT 12345678901234567890.123456789::NUMBER(38,9) AS D, NULL AS Z")?;
        let stale = statement.cancel_handle();
        assert_eq!(
            stale.cancel().unwrap_err().status,
            6,
            "cancel before execute is rejected"
        );
        let mut reader = statement.execute()?;
        let batch = reader.next().ok_or("missing result batch")??;
        let decimal = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or("missing Decimal128")?;
        let exact = batch.num_rows() == 1
            && decimal.precision() == 38
            && decimal.scale() == 9
            && decimal.value(0) == 12_345_678_901_234_567_890_123_456_789_i128
            && batch.column(1).is_null(0);
        drop(reader);
        assert!(
            statement.execute().is_err(),
            "one statement belongs to one operation"
        );
        (exact, stale)
    };
    connection.autocommit(false)?;
    let read_only_transaction = query(&mut connection, "SELECT 1 AS N")? == 1;
    connection.rollback()?;
    connection.autocommit(true)?;

    {
        let mut statement = connection.statement("SELECT 1")?;
        statement.cancel_handle().request_cancel()?;
        assert!(matches!(statement.execute(), Err(error) if error.status == 11), "cancellation before native execution is latched");
    }

    // Statement creation/execution errors retire all child handles normally.
    let execution_error_recovery = query(&mut connection, "SELECT N FROM YASP_MISSING_FIXTURE")
        .is_err()
        && query(&mut connection, "SELECT 2 AS N")? == 1;
    for _ in 0..10 {
        assert_eq!(query(&mut connection, "SELECT 3 AS N")?, 1);
    }

    let (status, error, cancel_ms, cancelled, inactive_rejected) = {
        let mut statement = connection
            .statement("SELECT SUM(RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT => 100000000))")?;
        let handle = statement.cancel_handle();
        thread::scope(|scope| -> Result<_, Box<dyn std::error::Error>> {
            let (sender, receiver) = mpsc::channel();
            let worker = scope.spawn(move || {
                let result = statement.execute().and_then(|reader| {
                    for batch in reader {
                        batch?;
                    }
                    Ok(())
                });
                let outcome = match result {
                    Ok(()) => (0, None),
                    Err(error) => (error.status, Some(error.to_string())),
                };
                let _ = sender.send(outcome);
            });
            thread::sleep(Duration::from_millis(300));
            if receiver.try_recv().is_ok() {
                worker.join().map_err(|_| "native worker panicked")?;
                return Err("query completed before cancellation".into());
            }
            // This old handle must not interrupt the next operation on its parent.
            let inactive_rejected = stale_cancel.cancel().is_err();
            let started = Instant::now();
            let cancelled = handle.cancel();
            let cancel_ms = started.elapsed().as_millis();
            let outcome = receiver.recv_timeout(Duration::from_secs(12));
            if outcome.is_err() {
                // This isolated child process owns the deadline. Never drop or
                // report released native resources while its worker is alive.
                eprintln!("native worker did not stop; terminating probe process");
                std::process::exit(2);
            }
            worker.join().map_err(|_| "native worker panicked")?;
            assert!(
                handle.cancel().is_err(),
                "completed operation cannot be cancelled again"
            );
            let (status, error) = outcome?;
            Ok((
                status,
                error,
                cancel_ms,
                cancelled.is_ok(),
                inactive_rejected,
            ))
        })?
    };
    let recovery = if args.len() == 4 {
        None
    } else {
        Some(query(&mut connection, "SELECT 42 AS N")? == 1)
    };
    // A retained inactive cancellation handle owns its native ancestors safely.
    // Dropping the public connection first cannot leave dangling driver pointers.
    drop(connection);
    assert!(stale_cancel.cancel().is_err());
    drop(stale_cancel);
    println!(
        "{}",
        json!({"decimal_exact": exact, "read_only_transaction": read_only_transaction,
        "connection_recovery": recovery, "execution_error_recovery": execution_error_recovery,
        "inactive_cancel_rejected": inactive_rejected, "owned_handle_cleanup": true,
        "cancel_returned_success": cancelled, "cancel_call_ms": cancel_ms,
        "execute_status": status, "outcome": error.unwrap_or_else(|| "completed".into())})
    );
    Ok(())
}

fn status(port: u16) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut socket = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    socket.write_all(
        b"GET /_yasp_probe/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )?;
    let mut response = String::new();
    socket.take(16_384).read_to_string(&mut response)?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or("invalid fixture status response")?;
    Ok(serde_json::from_str(body)?)
}

fn fetch_probe(
    mut connection: Connection,
    port: u16,
    mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut statement = connection.statement(
        "SELECT 99::NUMBER(38,0) AS N FROM TABLE(GENERATOR(ROWCOUNT => 17)) /* yasp-fetch-probe */",
    )?;
    let handle = statement.cancel_handle();
    if mode == "prefetch" {
        let reader = statement.execute()?;
        thread::sleep(Duration::from_millis(500));
        let before = status(port)?;
        let mut rows = 0;
        for batch in reader {
            rows += batch?.num_rows();
        }
        drop(statement);
        let recovery = query(&mut connection, "SELECT 1 AS N")? == 1;
        drop(connection);
        drop(handle);
        println!(
            "{}",
            json!({"mode": mode, "rows": rows, "before_consumption": before,
            "recovery": recovery, "queue_size": 1, "prefetch_concurrency": 1})
        );
        return Ok(());
    }
    let result = thread::scope(|scope| -> Result<_, Box<dyn std::error::Error>> {
        let (started, first_row) = mpsc::channel();
        let worker = scope.spawn(move || -> Result<(), snowflake_connection::Error> {
            let reader = statement.execute()?;
            let _ = started.send(true);
            for batch in reader {
                batch?;
            }
            Ok(())
        });
        let first = first_row.recv_timeout(Duration::from_secs(4))?;
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut before = status(port)?;
        while before["chunk_active"].as_u64() == Some(0) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
            before = status(port)?;
        }
        let started = Instant::now();
        let cancelled = handle.cancel();
        let cancel_ms = started.elapsed().as_millis();
        // The outer runner's process deadline bounds a native reader/Release that
        // fails to stop. Scoped threads retain all native objects until joining.
        let outcome = worker.join().map_err(|_| "fetch worker panicked")?;
        Ok(
            json!({"mode": mode, "execute_returned": first, "before_cancel": before,
            "cancel_returned_success": cancelled.is_ok(), "cancel_call_ms": cancel_ms,
            "outcome": outcome.err().map(|error| error.to_string()).unwrap_or_else(|| "completed".into())}),
        )
    })?;
    let recovery = query(&mut connection, "SELECT 1 AS N")? == 1;
    drop(connection);
    drop(handle);
    let mut result = result;
    result["recovery"] = json!(recovery);
    println!("{result}");
    Ok(())
}
