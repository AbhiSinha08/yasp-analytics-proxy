# Python callback design

These signatures and scaffold examples describe the planned API; the Rust binary
does not load them yet. Python files are trusted local code loaded at startup.
Rust callbacks and context providers are supplied by the consuming application
and compiled with it. The engine defines their contracts and invokes registrations;
organization implementations remain outside core modules. Applications register
context derivation before role selection so their own metadata formats can be used.

## Source-role selection

One BI connection configuration uses shared proxy credentials. For each executable query,
select_backend receives extracted metadata and the RBAC policy from YAML, and
chooses a configured backend target/login. PostgreSQL roles enforce the actual
source privileges. The callback does not open a connection or receive passwords.

| Callback | Input | Return contract |
| --- | --- | --- |
| select_backend(context, policy) | Effective query context and configured RBAC data | Required mapping with target, backend_login, and cache_scope. |
| before_query(context, sql, parameters) | Query context, SQL, typed binds | None for no rewrite, or a mapping containing sql. |
| after_result(context, columns, rows) | Selected-role context, column metadata, bounded typed batch | A batch preserving schema and row order. |
| on_connection(context, event) | Connection context and lifecycle event | None. |

The selector's cache_scope identifies additional metadata/policy inputs affecting
results; None disables caching. Target and selected role are always separate cache
key inputs. Selection runs before cache lookup on both hits and misses. Decisions
outside configured permitted target/login pairs fail, as do missing/unmapped required
metadata or callback errors. Recognized discovery/setup operations have an explicit
restricted selection in policy. Query context identifies these operations; scripts
must not infer a privileged role merely from a client-supplied tag.

Rust acquires an idle matching connection or creates one within pool limits. Queries
in the same frontend session may select different roles in autocommit mode. An
explicit transaction pins its selected role/connection; conflicting decisions fail.
BEGIN can defer acquisition until the first operation requiring a backend. COMMIT,
ROLLBACK, and cancellation use the current session's pinned state without needing
new routing metadata. Parse/Describe may invoke selection before parameter values
exist; policies requiring unavailable inputs must reject that preparation. Each
execution is reauthorized, and advertised parameter/result types must stay compatible.

Phase 1 assumes role-selection metadata is supplied by the trusted BI integration.
Frontend service authentication does not independently authenticate each viewer.
The example `group` field needs an application-defined metadata integration; it is
not assumed to be present in stock Metabase comments. Preserve metadata provenance
across SQL rewrites. Disable BI caching until its security scope has been verified.
The selector example is an unimplemented stub and raises an error when called.

## Context and execution

Query context contains BI service identity, SQL/dialect, typed parameters, extracted
metadata, session/transaction state, and configuration/policy/hook generations.
Target and backend role are populated after selection. Connection context contains
only fields available at its lifecycle stage. SQL rewrites precede selection and
preserve bind positions/types. Driver objects and the Rust AST remain internal.

Lifecycle events are created, checkout, return, and closed. Pool return does not
shut down compute. Cleanup events cannot promise exactly-once delivery after a crash.

Use bounded workers outside Tokio's async workers. Python 3.12 CPU callbacks share
the GIL, and a timeout does not terminate executing embedded code. Required callback
errors fail the query. Nondeterministic work disables caching unless its inputs can
be represented in the cache context.

Backend adapters preserve exact values and column metadata at the hook boundary.
Batch byte limits cover oversized individual rows. The passthrough example uses
only the standard library and performs no result transformation.
NULL maps to None; numbers must not lose precision through float or JSON conversion.
Define each supported native type's conversion explicitly, including timestamps,
arrays, and special values. Unsupported typed transformations fail explicitly.
Validate output types, row counts, and byte limits after each callback. Preserving
row order is also a contract of the trusted implementation.

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
