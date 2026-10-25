# YASP documentation

Start with [Architecture](architecture.md) for the system overview, component map,
and query/session diagrams. It explains the current runtime in plain language.
Use the references below for exact settings and contracts.

## Setup and development

| Guide                                  | Use it for                                                                       |
| -------------------------------------- | -------------------------------------------------------------------------------- |
| [Configuration](configuration.md)      | Run the host, resolve secrets, configure routes and limits, and connect clients. |
| [TLS](tls.md)                          | Certificates and encrypted frontend, source, and Redis connections.              |
| [Development](development.md)          | Toolchain, source tour, Rust conventions, dependencies, and Python embedding.    |
| [Tests](../tests/README.md)            | Disposable services, feature matrices, fixtures, and focused test commands.      |
| [Local environments](../dev/README.md) | Persistent BI tools, decision model, and Snowflake emulator.                     |

## Implementation reference

| Reference                                     | Owns the detailed contract for                                                           |
| --------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [Extensions](extensions.md)                   | Library assembly, callback stages, query context, dataset ownership, and Python runtime. |
| [Hook examples](../hooks/README.md)           | Built-in callback names and runnable application examples.                               |
| [Routing](source-access-routing.md)           | Selector decisions, overflow, and connection ownership.                                  |
| [Multi-engine compatibility](multi-engine.md) | Frontend/backend combinations, Snowflake values, and compatibility gates.                |
| [Schema catalog](catalog.md)                  | Source facts, immutable generations, refresh, and frontend metadata.                     |
| [AST lineage](ast-lineage.md)                 | Output-column sources, JSON paths, uncertainty, and cache association.                   |
| [Query modification](query-modification.md)   | SQL replacement, revalidation, failures, and transformation examples.                    |
| [Result processing](result-processing.md)     | Text/NULL edits, row filtering, native batches, and raw cache replay.                    |
| [Result classification](decision-model.md)    | Bounded System 1 requests, outcomes, failures, and decision sidecars.                    |

## Product scope

[PLAN.md](PLAN.md) defines phase goals, remaining capabilities, and acceptance gates.
[deliverables.md](deliverables.md) tracks implementation work. Current behavior is
described in architecture and the implementation references; planned features are
explicitly marked.

## Maintaining these docs

Keep each fact in its owning guide: settings/defaults in configuration, callback
contracts in extensions or their stage reference, runnable examples beside their
resources, and future requirements in the plan. Architecture keeps the high-level
explanation and diagrams; link to details rather than copying their full contract.
Code comments explain local purpose, invariants, and non-obvious constraints.
Record session history and abandoned approaches in version control.
