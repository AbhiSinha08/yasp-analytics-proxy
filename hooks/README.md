# Backend selection callbacks

The library exposes a typed Rust selector contract. The consuming application
registers its callback and any application-owned policy data when constructing the
engine. It can use `yasp::hooks::bind_select_backend` to bind a callback that reads
application state. Organization-specific metadata parsing and RBAC interpretation
remain in the host application.

The binder accepts `Fn(&SelectionRequest, &State) -> Result<BackendSelection,
SelectionError>` and owns the registered state through an `Arc`.

Both state and function must be `Send + Sync + 'static`. Rust checks callback
arguments, return types, and these bounds when the host registers the function.

```rust
use yasp::hooks::{bind_select_backend, BackendSelection, SelectionRequest};

let default_pair = BackendSelection {
    target: "primary".into(),
    backend_login: "reader".into(),
};
let selector = bind_select_backend(
    default_pair,
    |_request: &SelectionRequest<'_>, pair| Ok((*pair).clone()),
);
```

`SelectionRequest<'a>` carries the client and query IDs and borrows the authenticated
frontend user and parsed query. It does not expose backend credentials. A callback returns a
`BackendSelection` containing a configured target and backend login, or a
`SelectionError` (`Denied` or `Failed`). The router validates the returned pair and
owns connection acquisition and credential use. Denial is returned as SQLSTATE
`42501`; selector failure is returned as `XX000`. An invalid configured-pair decision
is denied with `42501`.

The built-in `builtin.select_backend.passthrough` selector selects the configured
default target/login pair. If only one pair is configured, it is inferred; with
multiple pairs, set `routing.default_backend`. The repository host currently registers
this built-in selector. A consuming host can map `hooks.select_backend` to its own
compiled Rust callback. Add and test reusable repository selectors under
`hooks/builtin/`; application-specific callbacks belong in the consuming application.

The callback runs synchronously on the query path. Keep it fast and nonblocking; the
engine does not provide a dedicated callback thread or a hard callback timeout.

Each configured pair has its own pool; physical connections open on demand.
Per-pool capacity defaults to 8 connections and total capacity defaults to 16.
Configuration is rejected if all
configured per-pool capacities cannot fit within the total bound.

The PostgreSQL source login remains the authority for database access. Selector
callbacks choose only among pairs configured by the host; they do not open connections
or receive passwords.

For the current simple-query protocol, selection runs for each executable request,
then the router executes and cleans up the selected backend lease. Transactions,
prepared statements, and cancellation are later gateway capabilities.

The current selector request does not claim to authenticate each BI viewer or derive
organization-specific metadata. Applications must supply trusted context and policy
data appropriate to their deployment. BI client compatibility, RBAC policy, and
per-viewer identity remain separate integration work.

## Later hook capabilities

Query rewrites, result processing, lifecycle hooks, and Python callback execution are
not part of this routing milestone. Their contracts will be documented when those
capabilities are implemented. Metadata extraction, load balancing, and caching are
also later milestones. Rust selectors can inspect the borrowed PostgreSQL
statement tree through `request.query.statement()`; driver objects remain internal.
Python receives no Rust AST and will need its own bounded context projection.

## Python dependencies

Build with the optional `python` Cargo feature. Cargo installs PyO3; the consuming
application installs its Python packages into one versioned environment before
startup. The supplied [requirements.txt](requirements.txt) has no third-party
requirements. Applications pin their own complete dependency set, including any
Phase 2 model SDK or native inference runtime.

PYO3_PYTHON chooses build/link settings. Runtime imports still need the environment's
site-packages and the registered hook modules on an explicit trusted search path.
All embedded hooks share package versions, sys.modules, and interpreter state;
there is no venv per hook or query. Native wheels must match Python, OS, and CPU.
No package installation happens while serving. Fail startup on missing required
imports and replace the process to activate dependency changes.

See [development setup](../docs/development.md#python-embedding) for commands and
the runnable Rust import probe. Conflicting dependencies or enforced termination
require separate process workers. A virtual environment is not a security sandbox.

## Phase 2 dynamic masking

A System 1 decision-model integration classifies column data and provides tags or
scores to a configured masking policy. The result-processing stage applies that
policy before forwarding affected data. The callback receives the selected role and
query context so policy can distinguish permitted and restricted outputs.
Run optional result transformations before mandatory masking and validation; no
later hook may restore raw values. A prefix sample fixes a decision for the whole
column but may miss later sensitive values. Per-batch classification cannot retract
earlier batches; whole-result policies require a result-size cap or conservative
masking from the start. Address columns by ordinal and metadata, not alias alone.

If a later batch fails, send an error without a success completion and do not cache
the partial result. Already emitted, authorized batches cannot be retracted. Raw
samples must remain local unless an approved external model/data policy permits
transmission; suppress raw sample logs.

Model failure, uncertain classification, schema-preserving masks, and cache versioning
are part of the component's contract; see docs/PLAN.md. No model SDK or inference
implementation is present in the repository.
