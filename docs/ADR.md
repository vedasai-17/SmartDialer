# SmartDialer v3 architecture decision

## PostgreSQL owns admission and reservations

The dialer reads a pacing snapshot and passes it to a pure function. Safety reads PostgreSQL independently, limits the requested volume by current eligible agents, the campaign concurrency cap, and the provider admission limit, and records the inputs and reasons. The allocator receives a decision ID and repeats the eligibility and cap checks in the transaction that consumes the decision. Both admission and dispatch enforce a rolling one-second cap per campaign and provider. Dispatch serializes its start permits using a transaction-scoped advisory lock and counts those permits in the audit log.

Allocation locks the campaign row to serialize the count used by the campaign-wide caps. It then locks eligible agent and borrower rows with `FOR UPDATE SKIP LOCKED`. A locked candidate is skipped, and a committed candidate in `RESERVED` fails the eligibility predicate on the next scan. Two transactions therefore cannot select the same candidate for reservation. A batch can be shorter when a row is locked or the borrower queue runs out. The transaction inserts one call, updates both owners, and consumes the approval together. [PostgreSQL 16 locking clauses](https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE)

Partial unique indexes enforce one active call per agent and one active call per borrower. Deferred foreign keys check at commit that each active agent-bound call and its agent point at each other. The call row is the future dispatch outbox. An insert trigger checks the approval ID, campaign, mode, budget, and the PostgreSQL transaction ID that consumed the approval, so a later transaction cannot use the same approval. [PostgreSQL 16 transaction ID functions](https://www.postgresql.org/docs/16/functions-info.html#FUNCTIONS-PG-SNAPSHOT)

A normalized phone number belongs to one borrower row globally. Duplicate enrollment in a second campaign is ignored. Suppression locks and changes that canonical row. It wins against future eligibility checks and survives call settlement if suppression happens after admission.

## State and failure behavior

Agent and borrower updates compare the expected state and version. Heartbeat expiry moves a reserved agent to `RECOVERY_HOLD` and preserves the call binding until the call is reconciled. A fresh heartbeat cannot bring an `OFFLINE` agent back without explicit login. Calls advance by rank, including terminal jumps when an earlier event is missing. The first terminal transition releases the agent and settles the borrower in the same transaction. `ANSWERED` records that the borrower picked up. `CONNECTED` requires a fresh bound agent and a prior answer.

PostgreSQL makes the reservation and settlement atomic. It also makes one busy campaign's row lock a throughput limit. The allocator performs ordered scans of eligible rows and does work per pair while holding that lock. Measure lock waits and batch duration before considering more complex counters or set-based writes.

| Omitted technology | Cost in this prototype |
| --- | --- |
| Redis | A second view of reservation state with invalidation and failover logic. Reservation checks still need PostgreSQL. |
| Kafka | Another durable log and a handoff from the database transaction, plus consumer offsets and replay operations. The call and event rows already provide those durable inputs. |
| Kubernetes | Deployment manifests, probes, cluster access, and debugging infrastructure for a local prototype. |
| Microservices | Network contracts, version coordination, and partial failures across boundaries that currently fit in one process and database. |
| ML | Training data, calibration, monitoring for drift, and harder-to-explain safety inputs. Counts and explicit statistical bounds are sufficient for the design. |

## Dispatch and event processing

The dispatcher claims a call row with `FOR UPDATE SKIP LOCKED` and commits a lease with a monotonically increasing fencing token. It sends the committed call's idempotency key as the client reference. Each provider response accesses the call through an unexpired lease check under a row lock before it can change status or release the lease. Provider calls run outside that transaction. Concurrent workers may repeat a request when its outcome is uncertain. They can only resend an initiation after the original provider confirms the reference is absent. The durable call row, lease, and provider idempotency give at-least-once execution with idempotent processing and reconciliation. Exactly-once execution across PostgreSQL and an external provider is outside the design's guarantees.

An uncertain start, cancel, bridge, or status query uses `UNKNOWN` with the last known rank. The dispatcher applies exponential backoff with full jitter. It records an exhausted start as terminal `FAILED` with `DISPATCH_DEAD_LETTER` and an audit entry only once it knows the provider has no call. If status remains unavailable, PostgreSQL retains ownership until reconciliation succeeds. Healthy active calls are polled so a lost terminal callback can settle the call.

The webhook authenticates the exact body before parsing it. It commits a receipt before returning 2xx and returns 503 on a failed database insert. Event IDs are unique per provider. For providers without an ID, the fallback is SHA-256 over the JSON array `[provider_call_id, event_type, UTC timestamp, sequence]`. Signed malformed or unresolved events are stored and audited. Event processors claim pending rows with `SKIP LOCKED` and commit the transition and receipt disposition together. A late `ANSWERED` record can contribute to derived abandonment metrics when it falls within an unbridged terminal call's lifetime, without reapplying settlement.

The reaper moves expired reservations into `RECOVERY_HOLD`. Before an answer, the dispatcher cancels and confirms the result before releasing ownership. After an answer, it locks a replacement agent and rebinds the same call at most once within the hold deadline. If it cannot reassign the call, confirmed release counts as abandonment. The mocks check the lease at initiation, bridge, and cancellation acceptance. The bridge also checks the current agent reservation in that transaction. A future real adapter would need to support an equivalent fenced handoff to preserve those guarantees across the external boundary.

The mocks store their provider-side acceptance, cancellation, bridge, profile and callback delivery records in the `mock:A` and `mock:B` namespaces of `call_events`. Their commits are independent of dispatcher commits. This keeps restart tests durable without a second service or table. These records do not enter the webhook processor and are excluded from webhook metrics. It models a provider that survives a worker crash. It does not model independent provider storage surviving a PostgreSQL outage.

## Phase 2 limits

The PostgreSQL path still runs strict progressive admission. The predictive controller and speculative policies are later phases. Speculation can bound the probability and rate of unassigned answers. A deterministic guarantee against them requires hard capacity.

The least certain part is how a real telecom adapter would honor the local fence and time limit at the moment it bridges. The current mock verifies both inside PostgreSQL, and the tests exercise a drop immediately before the bridge call. A real integration also needs provider-specific guarantees about authoritative absence and cancellation by client reference. The first scaling measurements should isolate the campaign row lock, agent row churn, and the scan of due mock callbacks.
