//! Native connector. Ordinary operations require exclusive Rust borrows;
//! only an operation-scoped cancellation handle can overlap execution or fetching.

use adbc_ffi::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError,
    FFI_AdbcStatement,
};
use arrow_array::{
    RecordBatch, RecordBatchReader,
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use std::{
    cell::UnsafeCell,
    ffi::{CStr, CString},
    fmt,
    marker::PhantomData,
    mem::ManuallyDrop,
    path::Path,
    ptr,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct Error {
    pub status: u8,
    pub sqlstate: Option<String>,
    message: String,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ADBC status {}: {}", self.status, self.message)
    }
}
impl std::error::Error for Error {}
fn invalid(message: &str) -> Error {
    Error {
        status: 6,
        sqlstate: None,
        message: message.into(),
    }
}
fn checked(status: u8, error: FFI_AdbcError) -> Result<(), Error> {
    if status == 0 {
        return Ok(());
    }
    let message = if error.message.is_null() {
        String::new()
    } else {
        // SAFETY: the error owns its NUL-terminated message until it is released.
        unsafe { CStr::from_ptr(error.message).to_string_lossy().into_owned() }
    };
    let state = error.sqlstate.map(|c| c as u8);
    let sqlstate = state
        .iter()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        .then(|| String::from_utf8_lossy(&state).into_owned())
        .filter(|s| s != "00000");
    Err(Error {
        status,
        sqlstate,
        message,
    })
}

// Every callback is checked during driver loading. All pointer arguments below
// are owned by the current handle or a retained parent. Only Cancel runs in
// parallel; its operation-state lock also excludes retirement and destruction.
macro_rules! call {
    ($driver:expr, $method:ident $(, $arg:expr)* $(,)?) => {{
        let mut error = FFI_AdbcError::default();
        let method = $driver.$method.expect("validated ADBC callback");
        // SAFETY: private callers uphold the exclusive-operation/Cancel rule
        // above. No raw pointer or callback is exposed by the safe interface.
        let status = unsafe { method($($arg,)* &mut error) };
        checked(status, error)
    }};
}

struct NativeConnection {
    connection: UnsafeCell<FFI_AdbcConnection>,
    database: FFI_AdbcDatabase,
    driver: Box<FFI_AdbcDriver>,
}
struct ConnectionInner {
    native: ManuallyDrop<Box<NativeConnection>>,
    cleanup_failure: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}
// SAFETY: Connection exclusively owns ordinary operations and its statements
// borrow it exclusively. Only StatementCancel may overlap execution/fetch.
unsafe impl Send for ConnectionInner {}
unsafe impl Sync for ConnectionInner {}
impl ConnectionInner {
    fn retire(&self) {
        if let Some(failed) = self
            .cleanup_failure
            .lock()
            .expect("cleanup state is not poisoned")
            .take()
        {
            failed();
        }
    }
}
impl Drop for ConnectionInner {
    fn drop(&mut self) {
        // SAFETY: this last Arc owner takes the box exactly once. Keeping the
        // entire box on failed cleanup preserves native handle addresses too.
        let mut native = unsafe { ManuallyDrop::take(&mut self.native) };
        let released = (|| {
            if !native.connection.get_mut().private_data.is_null() {
                call!(
                    native.driver,
                    ConnectionRelease,
                    native.connection.get_mut()
                )?;
            }
            if !native.database.private_data.is_null() {
                call!(native.driver, DatabaseRelease, &mut native.database)?;
            }
            if let Some(release) = native.driver.release.take() {
                let mut error = FFI_AdbcError::default();
                // SAFETY: all children released and this box owns the driver.
                let status = unsafe { release(&mut *native.driver, &mut error) };
                checked(status, error)?;
            }
            Ok::<(), Error>(())
        })();
        if released.is_err() {
            self.retire();
            Box::leak(native);
        }
    }
}

pub struct Connection {
    inner: Arc<ConnectionInner>,
}
pub struct Driver {
    init: FFI_AdbcDriverInitFunc,
}
impl Driver {
    pub fn load(path: &Path) -> Result<Self, Error> {
        // SAFETY: the caller explicitly chooses a trusted, ABI-compatible driver.
        let library =
            unsafe { libloading::Library::new(path) }.map_err(|e| invalid(&e.to_string()))?;
        // Go shared libraries cannot safely be unloaded. A host loads its driver
        // once at startup; native function/Arrow release callbacks stay valid.
        let library = Box::leak(Box::new(library));
        // SAFETY: the symbol has ADBC's published initialization signature.
        let init = unsafe {
            library
                .get::<FFI_AdbcDriverInitFunc>(b"AdbcDriverInit")
                .map(|s| *s)
        }
        .map_err(|e| invalid(&e.to_string()))?;
        Ok(Self { init })
    }

    pub fn open(
        &self,
        options: &[(&str, &str)],
        cleanup_failure: Option<Box<dyn FnOnce() + Send + Sync>>,
    ) -> Result<Connection, Error> {
        let mut driver = Box::new(FFI_AdbcDriver::default());
        let mut error = FFI_AdbcError::default();
        // SAFETY: this symbol has the published ADBC C signature; boxed driver
        // and error layouts are supplied by adbc_ffi and live through the call.
        unsafe {
            checked(
                (self.init)(
                    1_001_000,
                    (&mut *driver as *mut FFI_AdbcDriver).cast(),
                    &mut error,
                ),
                error,
            )?;
        }
        if ![
            driver.DatabaseNew.is_some(),
            driver.DatabaseInit.is_some(),
            driver.DatabaseSetOption.is_some(),
            driver.DatabaseRelease.is_some(),
            driver.ConnectionNew.is_some(),
            driver.ConnectionInit.is_some(),
            driver.ConnectionSetOption.is_some(),
            driver.ConnectionRollback.is_some(),
            driver.ConnectionCommit.is_some(),
            driver.ConnectionRelease.is_some(),
            driver.StatementNew.is_some(),
            driver.StatementSetSqlQuery.is_some(),
            driver.StatementSetOptionInt.is_some(),
            driver.StatementExecuteQuery.is_some(),
            driver.StatementCancel.is_some(),
            driver.StatementRelease.is_some(),
        ]
        .into_iter()
        .all(|present| present)
        {
            return Err(invalid("driver lacks a required ADBC 1.1 callback"));
        }
        let mut inner = Arc::new(ConnectionInner {
            native: ManuallyDrop::new(Box::new(NativeConnection {
                connection: UnsafeCell::new(FFI_AdbcConnection::default()),
                database: FFI_AdbcDatabase::default(),
                driver,
            })),
            cleanup_failure: Mutex::new(cleanup_failure),
        });
        let owned = &mut **Arc::get_mut(&mut inner)
            .expect("connection has no external owners during initialization")
            .native;
        call!(owned.driver, DatabaseNew, &mut owned.database)?;
        for (name, value) in options {
            let name = CString::new(*name).map_err(|_| invalid("option name contains NUL"))?;
            let value = CString::new(*value).map_err(|_| invalid("option value contains NUL"))?;
            call!(
                owned.driver,
                DatabaseSetOption,
                &mut owned.database,
                name.as_ptr(),
                value.as_ptr()
            )?;
        }
        call!(owned.driver, DatabaseInit, &mut owned.database)?;
        call!(owned.driver, ConnectionNew, owned.connection.get_mut())?;
        call!(
            owned.driver,
            ConnectionInit,
            owned.connection.get_mut(),
            &mut owned.database
        )?;
        Ok(Connection { inner })
    }
}

impl Connection {
    pub fn autocommit(&mut self, enabled: bool) -> Result<(), Error> {
        let value = if enabled { c"true" } else { c"false" };
        call!(
            self.inner.native.driver,
            ConnectionSetOption,
            self.inner.native.connection.get(),
            c"adbc.connection.autocommit".as_ptr(),
            value.as_ptr()
        )
    }
    pub fn rollback(&mut self) -> Result<(), Error> {
        call!(
            self.inner.native.driver,
            ConnectionRollback,
            self.inner.native.connection.get()
        )
    }
    pub fn commit(&mut self) -> Result<(), Error> {
        call!(
            self.inner.native.driver,
            ConnectionCommit,
            self.inner.native.connection.get()
        )
    }
    pub fn statement(&mut self, sql: &str) -> Result<Statement<'_>, Error> {
        let sql = CString::new(sql).map_err(|_| invalid("SQL contains NUL"))?;
        let inner = Arc::new(StatementInner {
            handle: ManuallyDrop::new(Box::new(UnsafeCell::new(FFI_AdbcStatement::default()))),
            connection: self.inner.clone(),
            phase: Mutex::new(Phase::Ready),
        });
        call!(
            inner.connection.native.driver,
            StatementNew,
            self.inner.native.connection.get(),
            inner.handle.get()
        )?;
        call!(
            inner.connection.native.driver,
            StatementSetSqlQuery,
            inner.handle.get(),
            sql.as_ptr()
        )?;
        call!(
            inner.connection.native.driver,
            StatementSetOptionInt,
            inner.handle.get(),
            c"adbc.rpc.result_queue_size".as_ptr(),
            1
        )?;
        call!(
            inner.connection.native.driver,
            StatementSetOptionInt,
            inner.handle.get(),
            c"adbc.snowflake.rpc.prefetch_concurrency".as_ptr(),
            1
        )?;
        Ok(Statement {
            inner,
            _borrow: PhantomData,
        })
    }
}

#[derive(PartialEq, Eq)]
enum Phase {
    Ready,
    Executing,
    Finished,
    Cancelled,
}
struct StatementInner {
    handle: ManuallyDrop<Box<UnsafeCell<FFI_AdbcStatement>>>,
    connection: Arc<ConnectionInner>,
    phase: Mutex<Phase>,
}
// SAFETY: Statement is not cloneable and exclusively borrows its parent. The
// only shared method is Cancel, which ADBC requires to be thread-safe. Phase is
// never locked across Execute/fetch, so cancellation cannot wait behind them.
unsafe impl Send for StatementInner {}
unsafe impl Sync for StatementInner {}
impl Drop for StatementInner {
    fn drop(&mut self) {
        if !self.handle.get_mut().private_data.is_null()
            && call!(
                self.connection.native.driver,
                StatementRelease,
                self.handle.get_mut()
            )
            .is_err()
        {
            self.connection.retire();
            std::mem::forget(self.connection.clone());
            // SAFETY: retain the failed handle at its original native address.
            Box::leak(unsafe { ManuallyDrop::take(&mut self.handle) });
        } else {
            // SAFETY: successful release makes this the only remaining owner.
            drop(unsafe { ManuallyDrop::take(&mut self.handle) });
        }
    }
}

pub struct Statement<'connection> {
    inner: Arc<StatementInner>,
    _borrow: PhantomData<&'connection mut Connection>,
}
impl Statement<'_> {
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(self.inner.clone())
    }
    pub fn execute(&mut self) -> Result<Rows<'_>, Error> {
        {
            let mut phase = self
                .inner
                .phase
                .lock()
                .expect("cancellation state is not poisoned");
            if *phase == Phase::Cancelled {
                return Err(Error {
                    status: 11,
                    sqlstate: Some("57014".into()),
                    message: "query cancelled".into(),
                });
            }
            if *phase != Phase::Ready {
                return Err(invalid("statement is single-use"));
            }
            *phase = Phase::Executing;
        }
        let flight = Flight(self.inner.clone());
        let mut stream = FFI_ArrowArrayStream::empty();
        // stream is dropped before Flight on every failure path.
        call!(
            self.inner.connection.native.driver,
            StatementExecuteQuery,
            self.inner.handle.get(),
            &mut stream,
            ptr::null_mut()
        )?;
        let reader =
            ArrowArrayStreamReader::try_new(stream).map_err(|e| invalid(&e.to_string()))?;
        Ok(Rows {
            reader: Some(reader),
            flight,
            _borrow: PhantomData,
        })
    }
}
impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // A stale handle can keep native ownership alive but cannot cancel a
        // subsequent statement on the same connection.
        *self
            .inner
            .phase
            .lock()
            .expect("cancellation state is not poisoned") = Phase::Finished;
    }
}

struct Flight(Arc<StatementInner>);
impl Drop for Flight {
    fn drop(&mut self) {
        *self
            .0
            .phase
            .lock()
            .expect("cancellation state is not poisoned") = Phase::Finished;
    }
}

#[derive(Clone)]
pub struct CancelHandle(Arc<StatementInner>);
impl CancelHandle {
    /// Latch cancellation before Execute starts. Retirement makes late requests
    /// harmless, so disconnects cannot cancel the next operation on this connection.
    pub fn request_cancel(&self) -> Result<(), Error> {
        let mut phase = self
            .0
            .phase
            .lock()
            .expect("cancellation state is not poisoned");
        match *phase {
            Phase::Ready => {
                *phase = Phase::Cancelled;
                Ok(())
            }
            Phase::Finished | Phase::Cancelled => Ok(()),
            Phase::Executing => {
                drop(phase);
                self.cancel()
            }
        }
    }
    pub fn cancel(&self) -> Result<(), Error> {
        let phase = self
            .0
            .phase
            .lock()
            .expect("cancellation state is not poisoned");
        if *phase != Phase::Executing {
            return Err(invalid("no operation is in flight"));
        }
        call!(
            self.0.connection.native.driver,
            StatementCancel,
            self.0.handle.get()
        )
    }
}

pub struct Rows<'statement> {
    reader: Option<ArrowArrayStreamReader>,
    flight: Flight,
    _borrow: PhantomData<&'statement mut ()>,
}
impl Iterator for Rows<'_> {
    type Item = Result<RecordBatch, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.as_mut().and_then(Iterator::next).map(|batch| {
            batch.map_err(|error| Error {
                status: 10,
                sqlstate: None,
                message: error.to_string(),
            })
        })
    }
}
impl Rows<'_> {
    pub fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.reader.as_ref().expect("live Arrow reader").schema()
    }
}
impl Drop for Rows<'_> {
    fn drop(&mut self) {
        let mut phase = self
            .flight
            .0
            .phase
            .lock()
            .expect("cancellation state is not poisoned");
        *phase = Phase::Finished;
        // Exclude a concurrent Cancel while releasing the stream. Parents remain
        // retained by Flight until after every Arrow release callback completes.
        drop(self.reader.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    unsafe extern "C" fn connection_failure(
        _: *mut FFI_AdbcConnection,
        _: *mut FFI_AdbcError,
    ) -> u8 {
        9
    }
    unsafe extern "C" fn statement_failure(_: *mut FFI_AdbcStatement, _: *mut FFI_AdbcError) -> u8 {
        9
    }

    #[test]
    fn failed_native_release_retires_its_binding_once() {
        for statement_fails in [false, true] {
            let retired = Arc::new(AtomicUsize::new(0));
            let callback = retired.clone();
            let mut native = Box::new(NativeConnection {
                connection: UnsafeCell::new(FFI_AdbcConnection::default()),
                database: FFI_AdbcDatabase::default(),
                driver: Box::default(),
            });
            native.connection.get_mut().private_data = std::ptr::dangling_mut();
            native.driver.ConnectionRelease = Some(connection_failure);
            native.driver.StatementRelease = Some(statement_failure);
            let parent = Arc::new(ConnectionInner {
                native: ManuallyDrop::new(native),
                cleanup_failure: Mutex::new(Some(Box::new(move || {
                    callback.fetch_add(1, Ordering::SeqCst);
                }))),
            });
            if statement_fails {
                let mut handle = Box::new(UnsafeCell::new(FFI_AdbcStatement::default()));
                handle.get_mut().private_data = std::ptr::dangling_mut();
                drop(StatementInner {
                    handle: ManuallyDrop::new(handle),
                    connection: parent.clone(),
                    phase: Mutex::new(Phase::Finished),
                });
            }
            drop(parent);
            assert_eq!(
                retired.load(Ordering::SeqCst),
                1,
                "failed cleanup cannot make capacity reusable"
            );
        }
    }
}
