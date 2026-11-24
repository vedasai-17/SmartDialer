# SmartDialer

## PostgreSQL v3 — Phase 3

SmartDialer defaults to `STRICT_PROGRESSIVE`, with one eligible agent and borrower reserved per call. `HYBRID` can prepare a borrower shortlist and wake at a wrap-up deadline. With explicit opt-in, `BOUNDED_PREDICTIVE` and `HYBRID` can admit bounded speculative calls. The dispatcher leases committed calls, and the events process durably receives signed callbacks and serves metrics. Provider A and B are seeded synthetic mocks. [Design and trade-offs](docs/ADR.md)

From the checkout root in PowerShell, use Python 3.12 and a running Docker engine.

```powershell
docker compose up -d --wait
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-v3.txt
.\.venv\Scripts\python.exe -m alembic upgrade head
.\.venv\Scripts\python.exe -m smartdialer.v3 seed --agents 10 --borrowers 50 --provider A
```

Run each worker in its own terminal. Use the `campaign_id` printed by `seed` and start the workers within five minutes while the synthetic seed heartbeats are fresh. The reaper expires those heartbeats unless an agent client renews them.

```powershell
.\.venv\Scripts\python.exe -m smartdialer.v3 events --port 8000
.\.venv\Scripts\python.exe -m smartdialer.v3 dispatcher
.\.venv\Scripts\python.exe -m smartdialer.v3 dialer --campaign-id 1
```

Run `tick --campaign-id 1` in place of `dialer` for a single allocation cycle. Both dialer and dispatcher accept `--seconds` for a bounded run. Start another dispatcher to exercise concurrent leases. Run another events process on a different port to exercise concurrent inbox processing.

```powershell
.\.venv\Scripts\python.exe -m smartdialer.v3 status --campaign-id 1
Invoke-RestMethod http://127.0.0.1:8000/metrics
.\.venv\Scripts\python.exe -m smartdialer.v3 agents paused 1
.\.venv\Scripts\python.exe -m smartdialer.v3 mock-config --provider A --set outage=true
.\.venv\Scripts\python.exe -m smartdialer.v3 mock-config --provider A --set outage=false
.\.venv\Scripts\python.exe -m pytest -q tests/v3
```

Use `seed --name another-demo --provider B` to compare the same code with the slower mock. Runtime changes apply to all workers through the provider's mock profile. To change mock setup latency, pass both `--set setup_min_seconds=1` and `--set setup_max_seconds=2`. Defaults are simulation inputs.

With a local PostgreSQL 16 instance, set `SMARTDIALER_DATABASE_URL` to a `postgresql+psycopg://` URL in each terminal. Set `SMARTDIALER_TEST_DATABASE_URL` to test a separate database. Tests create and remove a uniquely named schema in the configured database.

The webhook endpoint is `POST /webhooks/{provider}`. Mock signatures use HMAC-SHA256 over the exact request bytes in the `X-SmartDialer-Signature` header, formatted as `sha256=<hex>`. `SMARTDIALER_MOCK_A_SECRET` and `SMARTDIALER_MOCK_B_SECRET` override the local mock keys. The metrics endpoint reports the call and inbox state counts, answered calls, and abandonment rate.

### Predictive and hybrid campaigns

```powershell
.\.venv\Scripts\python.exe -m smartdialer.v3 seed --name hybrid-demo --mode HYBRID --agents 10 --borrowers 500
.\.venv\Scripts\python.exe -m smartdialer.v3 seed --name predictive-demo --mode BOUNDED_PREDICTIVE --agents 10 --borrowers 500 --max-concurrent-calls 15 --provider-rate-limit 15 --enable-speculation --max-speculative-calls 5 --risk-budget 0.01
```

Start the same dialer, dispatcher, and events workers with the new campaign ID. Cold campaigns run progressively until enough recent outcome, timing, and abandonment evidence is available for their configured mode. `status` includes the proposed count, effective mode, hard and speculative approvals, the risk calculation, fallback reasons, and the breaker state. This phase upgrades an existing PostgreSQL database with `alembic upgrade head`.

`SAFE_TERMINATE` is the default excess-answer policy. Choose `SHORT_HOLD` to keep trying for a seat within a maximum of two seconds, or `PROGRESSIVE_FALLBACK` to stop speculative starts after an excess answer. An answer initially left unclaimed counts as an abandonment even if a short hold later finds an agent.

### How would you keep progressive safety while improving utilization?

Reserve an agent and a borrower atomically for each hard-capacity call. Use forecasts to pre-select eligible borrowers and wake the dialer when wrap-up ends. Recheck availability in PostgreSQL before reserving. A future lead-time claim would need a provider-guaranteed earliest answer time and a reliable wrap-up deadline. An observed p1 latency still allows earlier answers, so it cannot establish a deterministic guarantee.

Keep speculation off by default and give it a separate budget. When enabled, an independent Safety Controller uses a conservative answer estimate and a binomial overflow bound to limit unassigned calls. The Answer Gate must claim a fresh agent atomically before bridging. Count every unclaimed answer and fall back to progressive admission when the rolling abandonment bound breaches its guardrail or the forecasts become unreliable. Speculation bounds modeled risk and observed abandonment rates. Deterministic reservation safety comes from hard capacity.

## Earlier SQLite prototype

SmartDialer is a local collections dialer prototype with progressive and predictive pacing, transactional call admission, two mock telecom providers, and optional ML-based borrower ranking. It uses synthetic records and simulated calls.

Start with `python -m smartdialer demo` from the project folder. The submission ZIP includes the source, tests, architecture and demo notes, and recorded benchmark reports under `evidence/`.

## Run locally

Use Python 3.11 or newer. The project uses the Python standard library, including SQLite and `unittest`. Run these commands from the checkout root. Use a fresh `--db` path to seed a new campaign.

```powershell
python -m smartdialer seed --db .smartdialer/demo.sqlite3 --agents 10 --borrowers 50
python -m smartdialer worker --db .smartdialer/demo.sqlite3 --mode progressive --provider A
```

Open a second terminal in the same directory and run the same `worker` command. Both workers share the campaign and can allocate calls, process events, and recover jobs. Press `Ctrl+C` to stop each worker or pass `--seconds 15` to run for a fixed duration.

```powershell
python -m smartdialer status --db .smartdialer/demo.sqlite3
python -m smartdialer agents offline 1 2 --db .smartdialer/demo.sqlite3
python -m smartdialer agents online 1 2 --db .smartdialer/demo.sqlite3
```

For the Phase 2 check, `active_calls` should stay at or below 10 with these seed settings, and `duplicate_active` should be 0. `initiated` is cumulative, so it can exceed 10 as earlier attempts finish and agents become available again.

For predictive mode, run both workers with `--mode predictive`. Provider A sets up calls quickly. Provider B adds longer delays, timeouts, repeated answers, and late ringing events.

```powershell
python -m smartdialer train --output .smartdialer/answer-model.json
python -m smartdialer seed --db .smartdialer/predictive.sqlite3 --agents 10 --borrowers 50
python -m smartdialer worker --db .smartdialer/predictive.sqlite3 --mode predictive --provider B --model .smartdialer/answer-model.json --seconds 15
python -m smartdialer status --db .smartdialer/predictive.sqlite3
```

Omit `--model` to use queue order. A missing or invalid model also falls back to queue order. Predictive pacing uses the progressive proposal until ten resolved attempts are available in the five-minute estimation window. The worked example below shows the predictive calculation immediately.

### Phase 4 pacing example

```powershell
python -m smartdialer pacing-example
```

The example seeds 100 resolved attempts with 57 answers and runs the same predictive tick as a worker. Its decision shows the observed answer rate, setup horizon, in-flight calls, and expected agent releases. It proposes 17 calls with 10 confirmed agent slots, and the Safety Controller approves 10. The demo runs that batch through initiation, bridging, and completion.

### Phase 5 ML comparison

```powershell
python -m smartdialer train --seed 17 --output .smartdialer/answer-model.json
python -m smartdialer simulate --scenario A --seed 17 --model .smartdialer/answer-model.json --report .smartdialer/phase5.json
```

The simulation loads the same artifact accepted by `worker --model`. Compare `predictive / queue` with `predictive / ml` to see the effect of borrower ranking under identical pacing and safety rules. The JSON report includes the changes in utilization, starts, bridges, and unserved calls. Use `--scenario all` to evaluate all four scenarios.

## Tests, simulation, and load

```powershell
python -m unittest discover -s tests -v
python -m smartdialer failures --report .smartdialer/failures.json
python -m smartdialer pacing-example
python -m smartdialer simulate --seconds 600 --seed 17 --report .smartdialer/simulation.json
python -m smartdialer load-test --agents 1000 --workers 4 --report .smartdialer/load.json
```

The failure runner exits nonzero if an invariant fails. It starts a fresh process after each crash and checks that the original call is finalized once and the agent becomes available. It also covers a borrower hanging up while the worker is down, a provider outage with calls in flight, a 40-agent drop, a cancellation that races with an answer, duplicate callbacks, and completion arriving before ringing or answer callbacks.

The simulation compares progressive with queue order, predictive with queue order, and predictive with ML. Use `--scenario A`, `B`, `C`, or `D` to run a single comparison. The JSON report includes pacing by minute, controller decisions by reason, and an example of a reduced proposal for each run where one occurred.

### Phase 6 benchmark

```powershell
python -m smartdialer benchmark
```

This runs all four scenarios with all three strategies, six failure checks, and allocation load tests at 100 and 1,000 agents. The combined report is saved to `.smartdialer/phase6.json`. It includes the effect of ML ranking for each scenario and checks that every simulated attempt reached a terminal state. The load section measures the database reservation path with four workers, including the p95 time per allocation request. It reports reservations per second separately from simulated calls.

### Measured results

This seeded run used 16 agents and a 600-second observation window in each scenario. The model was trained on 2,000 synthetic rows and evaluated on 600 later rows. Its held-out Brier score was **0.1747**, compared with **0.2495** for the constant answer-rate baseline. Lower is better.

| Scenario | Talk-time utilization with predictive queue order | Talk-time utilization with predictive ML | Difference | Starts, queue → ML | Bridges, queue → ML |
|---|---:|---:|---:|---:|---:|
| A · 20% / 120 s | 88.41% | 93.27% | +4.86 pp | 323 → 165 | 79 → 81 |
| B · 50% / 90 s | 83.17% | 88.73% | +5.56 pp | 202 → 123 | 96 → 101 |
| C · 70% / 180 s | 95.95% | 96.70% | +0.75 pp | 96 → 68 | 60 → 60 |
| D · 70% / 180 s → 10% / 70 s | 82.27% | 86.93% | +4.66 pp | 178 → 128 | 52 → 57 |

Answer rates above are simulator inputs. Realized rates vary with borrower mix and time of day. D also takes six agents offline and injects a 25-second start-endpoint outage. Utilization is bridged talk time within the observation window divided by staffed agent time. Call counts cover attempts admitted during the window, with a drain period to capture terminal outcomes. Scenarios reuse the same seed and per-attempt random draws. Call times can differ because each policy selects borrowers in a different order.

Queue-order progressive matched queue-order predictive in this run because confirmed agent capacity limited actual admissions. All 12 comparison runs recorded zero answered-but-unserved calls. A separate deterministic cancellation-race demo recorded one. The ML uplift is measured under synthetic conditions where contact history helps predict answers.

### Five-minute demo

From a new PowerShell window, change to the folder containing `README.md` and the `smartdialer` directory. Replace the example path below with your project path. If Python reports `No module named smartdialer`, use `Get-Location` to check which folder you are in.

```powershell
Set-Location -LiteralPath 'C:\path\to\cred_resolve_problem_statement'
python -m smartdialer demo
```

Use the [five-minute walkthrough](docs/demo.md) for the expected output and discussion points. See [Architecture and decisions](docs/architecture.md) for the state diagrams, recovery sequence, scaling analysis, and final-question answer.
