//! Configured engine bindings. PostgreSQL remains the discovery authority until
//! the Snowflake virtual catalog is enabled.

use super::{
    PostgresBackends,
    execution::Route,
    result::{ExecutionError, ExecutionResult},
    snowflake::SnowflakeBackend,
};
use crate::hooks::{BackendDecision, BackendSelection};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};

pub struct Backends {
    pub(crate) postgres: Arc<PostgresBackends>,
    snowflake: BTreeMap<BackendSelection, Arc<SnowflakeBackend>>,
}
impl Backends {
    pub fn new(postgres: Arc<PostgresBackends>) -> Self {
        Self {
            postgres,
            snowflake: BTreeMap::new(),
        }
    }

    pub fn with_snowflake(
        mut self,
        selection: BackendSelection,
        backend: Arc<SnowflakeBackend>,
    ) -> io::Result<Self> {
        if [&selection.target, &selection.backend_login]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256 || s.contains('\0'))
            || self
                .postgres
                .selections()
                .any(|s| s.target == selection.target)
            || self.snowflake.contains_key(&selection)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate or invalid Snowflake binding",
            ));
        }
        self.snowflake.insert(selection, backend);
        Ok(self)
    }

    pub(crate) fn resolve(
        &self,
        decision: &BackendDecision,
    ) -> ExecutionResult<(BackendSelection, Route<'_>)> {
        let rejected = || ExecutionError::rejected("42501", "backend selection is not configured");
        let keys = || self.postgres.selections().chain(self.snowflake.keys());
        let targets: BTreeSet<_> = keys().map(|s| s.target.as_str()).collect();
        let target = decision
            .target
            .as_deref()
            .or_else(|| (targets.len() == 1).then(|| *targets.first().unwrap()))
            .ok_or_else(rejected)?;
        let logins: BTreeSet<_> = keys()
            .filter(|s| s.target == target)
            .map(|s| s.backend_login.as_str())
            .collect();
        let login = decision
            .backend_login
            .as_deref()
            .or_else(|| (logins.len() == 1).then(|| *logins.first().unwrap()))
            .ok_or_else(rejected)?;
        let selection = BackendSelection {
            target: target.into(),
            backend_login: login.into(),
        };
        if let Some((binding, backend)) = self.snowflake.get_key_value(&selection) {
            if decision.compute_type.is_some() {
                return Err(rejected());
            }
            return Ok((selection, Route::Snowflake(backend, binding)));
        }
        let decision = BackendDecision {
            target: Some(target.into()),
            backend_login: Some(login.into()),
            compute_type: decision.compute_type.clone(),
        };
        self.postgres
            .resolve(&decision)
            .map(|route| (selection, Route::Postgres(route)))
            .map_err(|_| rejected())
    }

    pub(crate) fn close(&self) {
        self.postgres.close();
        for backend in self.snowflake.values() {
            backend.close();
        }
    }
}
