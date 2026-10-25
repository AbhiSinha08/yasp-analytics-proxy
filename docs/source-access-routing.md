# Source access and compute routing

This guide describes current routing semantics. The [configuration guide](configuration.md#routing-and-pools)
owns YAML fields, defaults, credential overrides, and capacity settings. The
[architecture](architecture.md#compute-and-connection-ownership) shows the allocation
flow; the [plan](PLAN.md) owns future queuing and compute lifecycle requirements.

## Decisions and responsibilities

```python
{"target": "olap", "backend_login": "restricted", "compute_type": "small"}
```

The application selector chooses a source, login profile, and optional workload
class. Every field is optional: the engine infers a sole target, then a sole login
within that target. Compute-enabled targets use their explicit default type or
sole type. Ambiguous omissions and invalid choices deny the query. Python `None`
denies explicitly; `{}` requests configured resolution. An ordinary target rejects
an explicit compute type.

| Decision                                            | Owner                                 |
| --------------------------------------------------- | ------------------------------------- |
| Source, access profile, workload class              | Application `select_backend` callback |
| Default resolution and configured eligibility       | Engine                                |
| Available instance within the selected type         | Engine overflow allocator             |
| Credentials, native sessions, cleanup, cancellation | Backend adapter                       |
| User metadata meanings and source-role mappings     | Consuming application                 |

Labels such as `small` and `large` do not estimate SQL cost or provision machines.
If OLTP and OLAP expose the same tables, the application supplies its freshness rule
or workload hint. SQL comments alone do not authenticate a user. A shared login
profile name does not establish equivalent grants across instances.

## Overflow and connection ownership

For instances ordered `[small_a, small_b]`, the allocator uses `small_a` while it can
serve the selected login, then `small_b`. Saturation never escalates to another
compute type, login, or target. A connection failure before SQL submission may try
another eligible instance; submitted SQL and source permission failures are not
replayed.

Pools are partitioned by target, login, and physical instance and open on demand.
Two logins on four instances create eight possible pools, without reserving all
pool maxima. Instance work/connection limits cover all logins; the global budget
covers every engine in the registry. PostgreSQL holds a slot until its physical
connection closes, including idle pooling and cleanup. Snowflake holds one through
its admitted session and cleanup. Idle PostgreSQL connections can be reclaimed;
active work cannot. See [capacity and waiting](configuration.md#capacity-and-waiting)
for acquisition deadlines and cancellation.

## Cache, discovery, and transactions

Selection and configured-bound validation run even on cache hits. Cache scope
checks the actual physical source and frontend/session identity; overflow records
the instance actually used. A hit acquires no source capacity and does not recheck
source grants until execution resumes after expiry. The [cache reference](configuration.md#redis-result-cache)
describes keys, limits, and failure behavior.

Discovery uses its configured identity and instance, bypasses application policy
callbacks and result caching, and shares source capacity. Explicit transactions
retain their selected physical route and bypass the cache; subsequent selection
must remain compatible with that route. Adapter cleanup owns session reset.
Returning one login's connection cannot suspend compute shared by other logins.
