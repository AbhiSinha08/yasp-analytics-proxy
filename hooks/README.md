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
multiple pairs, set `routing.default_backend`. The repository host registers
this built-in selector and external Python references. A consuming host can map `hooks.select_backend` to its own
compiled Rust callback. Add and test reusable repository selectors under
`hooks/builtin/`; application-specific callbacks belong in the consuming application.

Rust callbacks run synchronously on the query path. Keep them fast and nonblocking;
Rust execution has no dedicated callback thread or enforceable timeout.

Each configured pair has its own pool; physical connections open on demand.
Per-pool capacity defaults to 8 connections and total capacity defaults to 16.
Configuration is rejected if all
configured per-pool capacities cannot fit within the total bound.

The PostgreSQL source login remains the authority for database access. Selector
callbacks choose only among pairs configured by the host; they do not open connections
or receive passwords.

For the current simple-query protocol, selection runs for each executable request,
then the router executes and cleans up the selected backend lease. PostgreSQL
CancelRequest cancels selection waits or active backend queries. Transactions and
prepared statements remain later gateway capabilities.

The current selector request does not claim to authenticate each BI viewer or derive
organization-specific metadata. Applications must supply trusted context and policy
data appropriate to their deployment. BI client compatibility, RBAC policy, and
per-viewer identity remain separate integration work.

## External Python callbacks

Enable the `python` Cargo feature to use `hooks::python::PythonRuntime` and
`PythonCallback`. Register trusted application files before opening the listener.
The platform can invoke any named synchronous function with the signature
`function(request, options)`; backend selection is its first engine integration.
UTF-8 Python files are deployment resources, not compiled into the binary. The runnable
passthrough example is [examples/select_backend.py](examples/select_backend.py).

```rust
use serde_json::json;
use yasp::{config::PythonSettings, hooks::python::PythonRuntime};

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = PythonRuntime::new(PythonSettings::default()).await?;
    let callback = runtime.register(
        std::path::Path::new("/opt/application/selector.py"),
        "select_backend",
        json!({"target": "primary", "backend_login": "reader"})
            .as_object().unwrap().clone(),
    ).await?;
    let _decision = callback.call(json!({
        "authenticated_user": "analytics", "client_id": 1,
        "query_id": 2, "sql": "SELECT 1",
    })).await?;
    // gateway::serve_routed adapts this callback and validates selected pairs.
    runtime.shutdown();
    Ok(())
}
```

Create one runtime per process and clone it to register more callbacks. Each
registration owns a uniquely named module and retains its globals. Every invocation
receives fresh request and options objects. Mutating options in Python does not alter
later calls. Callback registration and execution run on one dedicated worker outside
Tokio. Coroutine functions, async generators, and unsupported return objects fail.

The data boundary supports null, booleans, signed/unsigned 64-bit integers, finite
floats, Unicode strings, lists, and string-keyed dictionaries. It accepts exact
built-in types; no pickle, driver objects, or automatic native-value conversions are
used. The nesting limit is 64. Future result hooks need their own lossless value
contract; this boundary is not a database result codec.

The selector receives exactly `authenticated_user`, `client_id`, `query_id`, and
original `sql`. It receives no backend credentials or Rust AST. Return exactly
`{"target": "configured target", "backend_login": "configured login"}` with
nonempty strings, or `None` to deny. Denial and unconfigured pairs produce `42501`.
Invalid output, exceptions, saturation, or timeout produce a redacted `XX000`;
selection never falls back to another route. Cancellation while awaiting selection
produces `57014`. The router validates the returned pair before acquiring a lease.

The default waiting deadline is 2 seconds, including queue time and registration.
The queue admits 32 waiting jobs and charges queued plus executing inputs against
a 4 MiB byte budget. Individual payloads are limited to 2 MiB; invocation input
includes request and options. Registration reserves one maximum payload for bounded
script loading. Bounds measure encoded JSON bytes (or script bytes), not total Python
heap usage. Invalid settings fail before initialization; saturation fails immediately.

A timeout, cancellation, or disconnect abandons the response. Executing Python retains
its worker and byte reservation until it returns. Expired or abandoned queued jobs
never execute. Shutdown stops admission and wakes waiters without joining executing
code. Embedded Python is trusted code, not a sandbox; a stuck callback requires
process replacement. Hook errors exclude exception text and source data. Application
code remains responsible for its own logging and side effects.

See [runtime configuration](../docs/development.md#python-selector-configuration)
for file references, import paths, and local testing.

## Later hook capabilities

Query rewrites, result processing, and lifecycle hooks are
not part of this routing milestone. Their contracts will be documented when those
capabilities are implemented. Metadata extraction, load balancing, and caching are
also later milestones. Rust selectors can inspect the borrowed PostgreSQL
statement tree through `request.query.statement()`; driver objects remain internal.
Python selection uses the bounded request projection described above.

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
