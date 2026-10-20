//! Configured engine bindings and their explicit discovery authority.

use super::{
    ConnectionBudget, PostgresBackends,
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
    budget: Arc<ConnectionBudget>,
    pub(crate) postgres: Option<Arc<PostgresBackends>>,
    snowflake: BTreeMap<BackendSelection, Arc<SnowflakeBackend>>,
}
impl Backends {
    pub fn new(postgres: Arc<PostgresBackends>) -> Self {
        Self {
            budget: postgres.budget.clone(),
            postgres: Some(postgres),
            snowflake: BTreeMap::new(),
        }
    }

    /// A Snowflake-only registry; add its configured bindings before serving.
    pub fn without_postgres() -> Self {
        Self::with_budget(
            ConnectionBudget::new(crate::config::BackendSettings::default().max_connections_total)
                .expect("valid default backend capacity"),
        )
    }

    /// An empty registry under an explicit shared total; register bindings before use.
    pub fn with_budget(budget: Arc<ConnectionBudget>) -> Self {
        Self {
            budget,
            postgres: None,
            snowflake: BTreeMap::new(),
        }
    }

    fn selections(&self) -> impl Iterator<Item = &BackendSelection> {
        self.postgres
            .iter()
            .flat_map(|pg| pg.selections())
            .chain(self.snowflake.keys())
    }

    pub(crate) fn discovery_selection(
        &self,
        configured: Option<&BackendSelection>,
    ) -> io::Result<BackendSelection> {
        if let Some(selection) = configured {
            if self.selections().any(|s| s == selection) {
                return Ok(selection.clone());
            }
        } else {
            let mut selections = self.selections();
            if let Some(first) = selections.next()
                && selections.all(|s| s.target == first.target)
            {
                return Ok(first.clone());
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "discovery requires a configured target and login",
        ))
    }

    pub(crate) fn discovery(
        &self,
        selection: &BackendSelection,
        compute: Option<&str>,
    ) -> Option<super::discovery::DiscoverySource<'_>> {
        if compute.is_none()
            && let Some((binding, backend)) = self.snowflake.get_key_value(selection)
        {
            return Some(super::discovery::DiscoverySource::Snowflake(
                backend, binding,
            ));
        }
        self.postgres
            .as_ref()?
            .get_discovery(selection, compute)
            .map(|pg| pg.discovery())
    }

    pub async fn catalog(
        &self,
        selection: &BackendSelection,
        compute: Option<&str>,
        settings: crate::catalog::CatalogSettings,
    ) -> Result<Arc<crate::catalog::Catalog>, crate::catalog::CatalogError> {
        if compute.is_none()
            && let Some(source) = self.snowflake.get(selection)
        {
            return crate::catalog::Catalog::snowflake(source.clone(), settings).await;
        }
        let source = self
            .postgres
            .as_ref()
            .and_then(|pg| pg.get_discovery(selection, compute))
            .ok_or(crate::catalog::CatalogError::InvalidSettings)?;
        crate::catalog::Catalog::postgres(Arc::new(source.clone()), settings).await
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
                .iter()
                .flat_map(|pg| pg.selections())
                .any(|s| s.target == selection.target)
            || self.snowflake.contains_key(&selection)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate or invalid Snowflake binding",
            ));
        }
        backend.attach_budget(&self.budget)?;
        self.snowflake.insert(selection, backend);
        Ok(self)
    }

    pub(crate) fn resolve(
        &self,
        decision: &BackendDecision,
    ) -> ExecutionResult<(BackendSelection, Route<'_>)> {
        let rejected = || ExecutionError::rejected("42501", "backend selection is not configured");
        let keys = || self.selections();
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
            .as_ref()
            .ok_or_else(rejected)?
            .resolve(&decision)
            .map(|route| (selection, Route::Postgres(route)))
            .map_err(|_| rejected())
    }

    pub(crate) fn close(&self) {
        if let Some(postgres) = &self.postgres {
            postgres.close();
        }
        for backend in self.snowflake.values() {
            backend.close();
        }
    }
}
