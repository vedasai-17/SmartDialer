# Five-minute SmartDialer walkthrough

From the folder containing the README, run the demo. It seeds its own synthetic data, injects a worker crash, and runs a short comparison of the dialing strategies.

```powershell
python -m smartdialer demo
```

| Time | Show | Explain |
|---|---|---|
| **0:00–0:45** | The request-flow diagram in [Architecture and decisions](architecture.md) | Progressive pacing proposes one call per available agent. Predictive pacing estimates call volume. Both send their proposals through the same Safety Controller, which reserves confirmed agent capacity before dispatch. |
| **0:45–1:30** | `two_workers_one_agent` | Two workers compete for one agent. The result is `reservations: 1` and `duplicate_active: 0`. The claim is transactional, and the database enforces one active call per agent and borrower. |
| **1:30–2:30** | `worker_crash` | Exit code `97` marks the injected crash after provider acceptance. The replacement exits with `0`. It reconciles the stable attempt ID and records one provider call, one bridge, and one finalization. The agent returns to `AVAILABLE`. |
| **2:30–3:15** | `pacing` | With a 57% estimated answer rate and 10 free agents, `floor(10 / 0.57)` proposes 17 calls. The controller approves 10, and the demo shows all 10 starting, bridging, and completing. |
| **3:15–4:15** | The three Scenario A rows | Queue-order predictive matches progressive under the same capacity cap. ML ranking raises talk-time utilization from 87.74% to 93.34% in this synthetic run and bridges 51 calls from 97 starts, compared with 49 bridges from 211 starts. The gain comes from borrower selection within the allowed batch. |
| **4:15–5:00** | The [load and scaling section](architecture.md#load-and-scaling) | All three allocation tests filled their slots with zero duplicates. They measure database reservations. Agent loss during ringing can still race with a borrower answering, and the failure suite records that outcome. |

## If asked to show more

```powershell
python -m smartdialer failures
python -m unittest discover -s tests -v
python -m smartdialer benchmark
```

- **What happens if `COMPLETED` arrives after an `ANSWERED` crash?** The replacement processes the terminal event, settles the reservation once, and skips the pending bridge. The `completion_while_worker_is_down` case shows zero bridge attempts and one answered call without an agent.
- **What happens when the answer rate suddenly drops?** The rolling estimate adapts as outcomes arrive. A lower estimate can increase the raw proposal, but batch limits and confirmed agent capacity bound what starts. The provider circuit can block starts independently.
- **Where would you scale first?** Check allocation latency, database lock waits, and callback backlog. SQLite serializes writes. If allocation becomes the bottleneck, PostgreSQL row-level claims let independent workers claim eligible rows concurrently while retaining the reservation constraints.
