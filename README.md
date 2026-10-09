# YASP

**Yet Another SQL Proxy**

YASP is a reusable SQL proxy engine for analytics across multiple database engines. It aims to let BI applications use one connection while an organization supplies its own context, access policy, and query behavior. PostgreSQL is the first implementation phase; additional database engines, dynamic masking, and management features are part of the broader product direction.

The Rust library keeps protocol and database adapters, execution, resource limits, and extension contracts separate from organization-specific policy and integrations. Applications can build their own proxy host and provide their customizations without editing core engine modules.

- [Phases and components](docs/PLAN.md)
- [Development setup](docs/development.md)
- [Test strategy](tests/README.md)
