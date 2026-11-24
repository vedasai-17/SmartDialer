# Phase 4 verification results

## Utilization ladder

The measured run used Python 3.12.16, PostgreSQL 16.8, seed 17, six agents, a 180-second progressive warm-up, and a 360-second measurement window. The progressive baseline paced every five seconds. JIT used the same cadence plus a wake-up when an agent became available. The live CLI normally polls every 0.2 seconds, so this comparison measures the effect of the specified five-second baseline. The mock profiles kept their default transport failures, delays, and callback behavior. All tiers used a 20-second reconciliation interval for connected calls and shorter polling before answer.

Each cell shows **utilization · initiated / connected**. Utilization is bridged talk time clipped to the measurement window, divided by staffed agent time. Counts cover calls initiated inside that window, with final outcomes collected during the drain. Warm-up calls can contribute talk time if they are still connected when the window starts.

| Scenario | Provider | Progressive | JIT reservation | JIT with bounded speculation enabled | JIT gain |
| --- | --- | ---: | ---: | ---: | ---: |
| A · 20%, 120 s | A | 81.98% · 62 / 14 | 92.45% · 86 / 18 | 92.45% · 86 / 18 | 10.47 pp |
| A · 20%, 120 s | B | 83.12% · 48 / 14 | 87.99% · 49 / 15 | 87.99% · 49 / 15 | 4.87 pp |
| B · 50%, 90 s | A | 84.76% · 43 / 21 | 92.59% · 47 / 24 | 92.59% · 47 / 24 | 7.83 pp |
| B · 50%, 90 s | B | 81.18% · 39 / 22 | 84.34% · 41 / 23 | 84.34% · 41 / 23 | 3.16 pp |
| C · 70%, 180 s | A | 93.52% · 16 / 12 | 96.59% · 16 / 12 | 96.59% · 16 / 12 | 3.07 pp |
| C · 70%, 180 s | B | 93.17% · 18 / 12 | 94.87% · 18 / 12 | 94.87% · 18 / 12 | 1.70 pp |
| D · changing conditions | A | 77.24% · 35 / 9 | 83.82% · 50 / 12 | 83.82% · 50 / 12 | 6.58 pp |
| D · changing conditions | B | 80.22% · 22 / 11 | 83.82% · 25 / 12 | 83.82% · 25 / 12 | 3.60 pp |

All 24 runs drained with zero invariant violations and zero observed abandons. All six A–C comparisons improved on the corresponding progressive baseline while staying within the 2.5% observed-abandonment threshold. The bounded tier started zero speculative calls because the cohorts were below the minimum abandonment sample. JIT accounts for the measured improvement. With low answer rates, setup and ringing still occupy hard seats. The separate PostgreSQL safety tests exercise actual speculative admission, concurrent answers, and policy outcomes.

The JSON report contains the per-run requested and approved counts, action counts, breaker transitions, staffing changes, and sample decisions. In scenario A with Provider A, the bounded tier requested 11 calls and received approval for one. Pacing saw one free agent and two predicted releases, with a conservative probability of 0.259. Safety recorded only one hard seat, insufficient abandonment evidence, and a downward rate shift. It stored the request, both probability estimates, and the fallback reasons in PostgreSQL.

### Scenario D reactions

The input answer rate fell from 70% to 10% at 90 s. Setup latency increased at 122.4 s, with the mock API unavailable from 144 s through 190.8 s. The rate then moved to 20% and back up to 70%. The six-agent roster lost three agents at 226.8 s because the 40% shock was rounded up to whole agents.

| JIT run | Requested / approved | Safety actions | Breaker transitions after start | Staffing response |
| --- | ---: | --- | --- | --- |
| Provider A | 66 / 50 | 14 fallbacks, 81 rejects | OPEN 150 s → HALF_OPEN 170 s → OPEN 196.7 s → HALF_OPEN 220 s → CLOSED 220.85 s | Two decisions recorded a sudden drop |
| Provider B | 35 / 25 | 20 fallbacks, 84 rejects | OPEN 155 s → HALF_OPEN 175 s → OPEN 199.50 s → HALF_OPEN 220 s → CLOSED 222.63 s | Three decisions recorded a sudden drop |

The PostgreSQL 100-to-60 test checks the exact 40% availability loss and verifies that speculative ringing calls are cancelled while the hard call remains active.

## Concurrent load

Four allocation workers processed 200 borrowers with 100 agents and a batch limit of ten. The workload used the real dialer, dispatcher, PostgreSQL event processor, and FastAPI webhook route through an in-process HTTP transport. Domain time and mock call duration were fixed to isolate database and request overhead. Each callback was posted four times.

| Completed allocations | Allocations/s | Allocation request p95 | Webhook POSTs | Webhooks/s | Duplicate POSTs | Invariant violations |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 200 | 7.25 | 76.40 ms | 2,400 | 66.42 | 1,800 | 0 |

Allocation throughput uses the interval through the final allocation, and webhook throughput uses the full 36.13-second run through the drain. The p95 includes empty polls and time spent waiting for the controller, allocator, and database. The simulator shared the local PostgreSQL server during this measurement. These figures describe an end-to-end workload on this host and do not establish a production capacity limit.

## Failure checks

The red-team suite kills the worker after reservation commit, after lease claim, before the provider request, after provider acceptance with a lost response, and after a received response before the database update. It uses SIGKILL on POSIX and process termination on Windows. Replacement workers reconcile the same client reference. Other checks exercise two workers racing for one agent, stale leases and heartbeats, concurrent webhook replay, reordered callbacks, PostgreSQL availability overriding a stale advisory snapshot, and suppression between dispatch preparation and provider acceptance.

The pass tightened pre-answer polling, preserved staleness for same-rank webhook replays, and rechecked suppression at the mock provider's acceptance point. Parallel lab runs now serialize schema migrations because Alembic installs process-wide context proxies. The complete suite passed 129 tests and six subtests. Docker Compose configuration validated. Docker Desktop returned HTTP 500 at engine startup, so the measured verification used a native PostgreSQL instance.
