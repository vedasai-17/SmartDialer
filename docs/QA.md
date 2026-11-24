# SmartDialer discussion questions

## What happens when two workers see one free agent?

Both can make proposals, and two valid approvals can exist briefly. Allocation repeats the limits under the campaign row lock. It locks the agent and borrower with `FOR UPDATE SKIP LOCKED`, writes the call and both reservations, and consumes the approval in one transaction. The other allocator sees the changed state or skips the locked row. Partial unique indexes and deferred foreign keys enforce the same outcome at commit. One agent gets one active call.

## What if PostgreSQL says AVAILABLE and a cache says RESERVED?

I use PostgreSQL to decide whether the agent can be reserved. A cached value could help choose when to ask, but it has no authority over the reservation. Safety reads availability independently and the allocator rechecks it under a row lock with a versioned state update. The red-team test passes an advisory snapshot with zero free agents while PostgreSQL has one and verifies that Safety can approve one.

## The borrower answers, a worker crashes, and COMPLETED arrives. What happens?

The webhook is acknowledged after the inbox row commits. A replacement processor can see it even if the first worker exits. `COMPLETED` can advance directly to the terminal rank. Settlement and resource release happen together once. A later `ANSWERED` is stale and cannot reopen the call. If the borrower answered and no bridge exists, the terminal evidence or the late signed answer contributes to abandonment accounting. If the provider accepted a bridge before the crash, reconciliation can recover that evidence from the original provider.

## What happens when the answer rate changes from 70% to 10%?

The recent estimate falls first. I keep the larger conservative bound while the short and reference windows disagree. A large drop causes a progressive fallback until the estimates agree sufficiently and the abandonment guard allows speculation. Fewer answers by themselves reduce overflow pressure. The abrupt change also tells me the old forecast is a poor basis for raising volume. A sharp increase is the more immediate overflow risk.

## What changes between 1,000 and 100,000 agents?

I would start with campaign-lock wait, ready-row scan cost, heartbeat churn, and inbox age. At around 1,000, faster heartbeats can turn the reservation table into a vacuum problem. At 100,000, event retention and analytical reads also compete with writes, and one campaign lock limits a large pool. I would separate liveness writes, batch reservations, partition by campaign, and archive events. If campaigns share a provider or the same phone can appear on different shards, the cap and suppression authority must continue to work across those shards. [Scale discussion](ADR.md#scaling-from-100-to-100000-agents)

## Why did pacing ask for 17 while Safety approved 10?

With ten free agents, three in-flight calls, and an answer probability of 0.5, the proposal expects 1.5 of the in-flight calls to answer. It divides the remaining 8.5 seats by 0.5 and requests 17. A hard-only decision is capped by the ten available agents. In a speculative campaign Safety would still recompute its own estimate and apply the campaign, provider, overflow, and abandonment limits. The decision row stores the proposal inputs, the calculation, the independent Safety evidence, and the reasons for the cap.

## Which part am I least confident about?

The predictive assumptions under a shared shock. A group of borrowers can answer together, a wrap-up forecast can be late, and a fast provider response can arrive earlier than the observed p1. The binomial bound depends on the inputs and independence assumption. I would use the observed abandonment guard and progressive fallback, and I would need a provider-guaranteed earliest answer time before treating any lead-time claim as deterministic.
