//! Cache integration scenarios with disposable PostgreSQL databases and Redis namespaces.

#![cfg(unix)]
mod support;
use std::{
    collections::BTreeMap,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::{
    GatewayServer, Identity, PostgresConnection, RedisNamespace, TestDatabase, TestEnvironment,
    assert_success, quote_ident,
};
use yasp::{
    backend::PostgresBackends,
    cache::RedisCache,
    config::{BackendSettings, GatewayConfig},
    hooks::{BackendSelection, bind_cache_eligibility, bind_query_context, bind_select_backend},
};

#[path = "cache/support.rs"]
mod cache_support;
use cache_support::*;
#[path = "cache/cancellation.rs"]
mod cancellation;
#[path = "cache/context.rs"]
mod context;
#[path = "cache/eligibility.rs"]
mod eligibility;
#[path = "cache/hits.rs"]
mod hits;
#[path = "cache/results.rs"]
mod results;
#[path = "cache/transactions.rs"]
mod transactions;
#[path = "cache/wire.rs"]
mod wire;
