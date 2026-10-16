//! Routing, source permissions, metadata, pool isolation, and cancellation scenarios.

#![cfg(unix)]

mod support;
use std::{
    process::Output,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};
use support::{
    GatewayServer, Identity, PostgresConnection, TestDatabase, TestEnvironment, quote_ident,
};

#[path = "routing/cancellation.rs"]
mod cancellation;
#[path = "routing/metadata.rs"]
mod metadata;
#[path = "routing/permissions.rs"]
mod permissions;
#[path = "routing/pools.rs"]
mod pools;
#[cfg(feature = "python")]
#[path = "routing/python.rs"]
mod python;
#[path = "routing/scenarios.rs"]
mod scenarios;
#[cfg(feature = "python")]
use python::python_routing_contract;
use {
    cancellation::cancellation_discards_backend, metadata::metadata_discovery,
    permissions::source_permissions, pools::pool_isolation,
};
