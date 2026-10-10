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
  The host maps names at startup. Add reusable repository hooks under
  `hooks/builtin`; application-specific callbacks belong in the consuming host.
  Add lifecycle contracts only when their milestone is active.
- Routed PostgreSQL hosts construct `backend::PostgresBackends` with configured
  pairs and pool limits, then use `gateway::serve_routed`. The fixed-backend
  `gateway::serve` shares the same session and cleanup handling.

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
- Add Sub items to the file docs/deliverables.md if a plan implements multiple
  features or som sub-item is left delegated to the next session.
