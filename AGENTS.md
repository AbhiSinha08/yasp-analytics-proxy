# Project guidelines

## Product direction

YASP is a SQL proxy for analytics workloads across multiple database engines.
PostgreSQL is the first implementation phase, not the limit of the product.
Phase 1 uses one BI connection configuration and a custom callback that selects
source-role credentials per query using BI metadata and a local RBAC policy.
Source roles enforce database access. Phase 2 adds decision-model dynamic masking.
Read docs/PLAN.md for phase boundaries and component responsibilities. Runtime
implementation proceeds through requested subtasks

## Design and implementation

- Always follow YAGNI (You Aren't Gonna Need It). Use the Ponytail skill when
  available; these principles still apply when it is unavailable.
- Prefer existing code, the standard library, native platform features, and
  installed dependencies before introducing new code or dependencies.
- Give components clear responsibilities and narrow boundaries. Avoid speculative
  abstractions, generic plugin systems, unused configuration, and premature crates.
- Design phase-specific components so another database can be added with localized
  changes. Keep database drivers, dialect rules, native types, session behavior,
  and error/protocol conversion inside the relevant adapters. Shared routing,
  authorization, hooks, and caching must not depend on a specific driver.
- Define only contracts required by the current phase. Add polymorphic interfaces
  when another implementation needs them, rather than building unused frameworks.
- Preserve security checks, data integrity, lossless values, resource bounds, and
  necessary error handling while simplifying.
- Follow the Rust structuring and verification norms in
  [docs/development.md](docs/development.md#rust-structure-and-testing-norms).

## Core engine and customization boundary

- Treat YASP as a reusable library plus a host application. Organizations can
  depend on the library and compile their own proxy application without editing
  core engine code.
- Keep core capabilities and user customization in separate code. The core owns
  protocol/backend adapters, execution, pools, sessions, cache mechanics, resource
  bounds, extension invocation, and validation of extension decisions.
- Keep organization-specific context derivation, role mappings, RBAC interpretation,
  model integrations, and query/result/lifecycle behavior outside core modules.
  Example Python hooks live under hooks/; custom Rust implementations belong to
  the consuming application or its own crate, not the engine's src/ modules.
- Define narrow library extension contracts as the active phase needs them. The
  consuming application registers its context provider and custom Rust/Python
  hooks and passes them with configuration when constructing the engine.
- Compile Rust extensions with the consuming application. Supply Python scripts
  as application resources before startup. Do not implement runtime compilation,
  a dynamic plugin framework, or organization-specific policy inside the engine.
- Keep authenticated identity and engine-owned session/security state distinct
  from derived custom context. Extensions cannot bypass configured bounds.
- Shared components consume extension decisions through contracts; they must not
  import organization code or interpret hard-coded organization metadata formats.
- Reuse `hooks::bind_select_backend` for compiled routing callbacks and
  `hooks::builtin::select_backend::passthrough` for a configured default route.
  Configure Rust/Python references with `type`, `callback`, and application-owned
  `params`; Python also requires a file `path`. The host parses Rust params into
  typed state and maps names at startup. Passthrough selection belongs in its params,
  and the loadable example preserves future settings in commented blocks.
  Add reusable repository hooks under
  `hooks/builtin`; application-specific callbacks belong in the consuming host.
  Add lifecycle contracts only when their milestone is active.
- Routed PostgreSQL hosts construct `backend::PostgresBackends` with configured
  pairs and pool limits, then use `gateway::serve_routed`. The fixed-backend
  `gateway::serve` shares the same session and cleanup handling.
- Reuse feature-gated `hooks::python::PythonRuntime` and `PythonCallback` for trusted
  external Python functions. Create one runtime per process and clone it for further
  registration. `RoutedSelector` adapts compiled Rust or Python selection; examples
  remain under `hooks/examples`, and engine routing validates each returned pair.
- Keep the Redis result cache disabled by default; `cache.enabled` is its only opt-in.
  Hits do not reach PostgreSQL, so source grants and RLS are not rechecked and cached
  results can remain available until expiry. Use the shared multiplexed Redis
  connection, bounded `GETRANGE` reads, opaque versioned PostgreSQL payloads, and
  miss-on-error behavior. `cache.max_rows_per_query` bounds rows captured and replayed;
  exceeding it bypasses caching while the full result streams. Reserve three times
  the serialized-entry budget per active cached query across Redis, buffered, and
  response-wire copies. `cache.max_buffered_bytes_total` is an aggregate in-process
  budget, not Redis storage; the internal per-entry guard is derived as one third of
  it, and the Redis server has a separate memory/eviction policy.
  Bind optional eligibility with `hooks::bind_cache_eligibility`, configured under
  `cache.eligibility`; it may bypass queries but cannot expand supported SQL or
  configured limits. Cache keys use normalized effective-SQL SHA-256 query IDs.
  Validate frontend identity, database, selected backend endpoint/database and source
  user, fixed text settings, query/write timers, and codec version before replay.
  Source-role authorization still applies when a query reaches PostgreSQL. Future
  engine RBAC and masking must also apply to cached data. Initial Redis transport is
  loopback TCP; verified remote TLS remains deferred. Redis operation deadlines are
  fixed internally at 100 ms.

## Tests

- Test meaningful behavior of a component or function through its observable
  contract. Favor integration scenarios and focused regression tests for real bugs.
- Cover access isolation, protocol compatibility, data integrity, and failure
  recovery at the appropriate boundary.
- Do not duplicate small internal logic, test trivial helpers independently, or
  assert internal structure merely to mirror the implementation.
- Refactoring should not require editing tests when observable behavior is the
  same. Change expectations when the behavior contract intentionally changes.
- Use the smallest useful test suite. Do not add placeholder success tests or
  tests whose main effect is redundant maintenance during intended code changes.

## Logging

- Use the shared `logger` module to initialize host logging; library consumers
  can call `yasp::logger::init()` before starting the gateway, or pass a value
  resolved from their own configuration source to `init_with_level()`.
- Logs go to stdout. `LOG_LEVEL` accepts `DEBUG`, `INFO`, `WARN`, or `ERROR`
  and defaults to `INFO`; each level includes messages at higher severities.
- Query logs at DEBUG include a query ID, client ID, backend target, database user,
  and SQL.
  Log client connection changes at INFO and failures at ERROR; avoid logging
  secrets or raw protocol errors that may contain client data.

## Documentation and comments

- Write for humans using plain language, clear component descriptions, and
  explicit phase boundaries. Explain necessary technical terms.
- Describe the final design and actual current behavior. Distinguish planned
  capabilities from implemented features without presenting a change history.
- Do not include implementation narratives such as "changed from X to Y",
  comparisons with previous proposals, or abandoned approaches in code comments
  or maintained documentation. Use commits and review descriptions for history.
- Keep comments focused on purpose, contracts, constraints, and non-obvious
  reasons. Keep README, plan, setup instructions, and examples consistent.
- Use [docs/README.md](docs/README.md) as the documentation index. Keep component
  behavior in architecture, configuration, and extension guides, and planned scope
  in docs/PLAN.md.
- For large functionality changes, update the relevant component documentation and
  Mermaid query, session, or component diagrams in the same change. Verify those
  flows against the code; mark future stages explicitly.
- Add Sub items to the file docs/deliverables.md if a plan implements multiple
  features or som sub-item is left delegated to the next session.

<!-- graft:start -->
## Graft — repo context graph

This repo is indexed in `graft/`: small linked markdown nodes that explain each
system and carry exact file:line spans, kept in sync with the code through git.

For ANY task here — understanding how something works, finding where code lives,
or scoping a change — get context from the graph before grepping or opening
source files. Re-ask freely (it's cheap) and reuse literal identifiers you
already have (symbol, error string, file name) as the query. New to this repo?
Run `graft map` first — a token-budgeted orientation (dir clusters, hubs,
hotspots), no LLM, no key.

- Run `graft ask "<your question>" --source` → ranked nodes with the relevant
  code spans inlined (each hit's ≤8-line crux by default; `--full` for whole
  definitions when the crux isn't enough). Match the tool to the task shape:
  for understanding or editing, the top node IS the answer — cite its
  `covers:` file:line spans and edit straight from `--source`. For
  exhaustive tasks ("every occurrence / every caller of this pattern"), ranked
  results are top-N, not complete — run `graft grep "<literal>"` instead
  (exhaustive over indexed files, grouped by enclosing symbol), falling back
  to raw `grep -rn` only for unindexed files.
- `graft skeleton <file>` → every definition's signature + span, ~10× cheaper
  than reading the file; use it to skim an API surface.
- `graft callers <symbol>` gives precomputed, exact edges — who calls this.
  Add `--direction out` for what it calls, or `--depth N` to walk
  transitively for the full blast radius. For structural questions, skip
  ranking and use this directly.
- Or browse: `graft/INDEX.md` lists every node; follow the links.
- Monorepos and folders of multiple repos rank fairly across sub-projects —
  hits carry `[scope/]` labels naming which one they're from. Narrow with
  `graft ask "<task>" --in <scope>/` once you know where you're working.

If a returned span is truncated ("+N more lines"), open the file at that exact
range before finalizing. Only open source files when a node genuinely lacks a
needed detail, and then at the exact file:line the node points to — never
re-read whole files.

After big code changes, refresh the graph with `graft build` (deterministic,
no API key, $0).
<!-- graft:end -->
