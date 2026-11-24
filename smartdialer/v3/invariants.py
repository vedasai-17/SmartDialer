from sqlalchemy import Connection, text


def verify_invariants(connection: Connection, *, drained: bool) -> dict[str, int]:
    """Check the seven invariants over a committed run with the real mock providers."""
    row = connection.execute(text("""
        WITH active AS (SELECT id, agent_id, borrower_id FROM calls WHERE state_rank < 100),
        applied AS (
            SELECT e.call_id, CASE WHEN e.event_type IN ('COMPLETED','FAILED','CANCELLED','BUSY','NO_ANSWER')
                THEN 'TERMINAL' ELSE e.event_type END AS transition
            FROM call_events e JOIN calls c ON c.id = e.call_id AND c.provider = e.provider_name
            WHERE e.processing_status = 'APPLIED'
        ), used_approvals AS (
            SELECT safety_decision_id, count(*) AS total, count(*) FILTER (WHERE is_speculative) AS speculative,
                count(*) FILTER (WHERE NOT is_speculative) AS hard FROM calls GROUP BY safety_decision_id
        )
        SELECT
            (SELECT count(*) FROM (SELECT agent_id FROM active WHERE agent_id IS NOT NULL
                GROUP BY agent_id HAVING count(*) > 1) d) AS one_reservation_per_agent,
            (SELECT count(*) FROM (SELECT borrower_id FROM active GROUP BY borrower_id HAVING count(*) > 1) d)
                AS one_active_call_per_borrower,
            (SELECT count(*) FROM active c LEFT JOIN agents a ON a.id = c.agent_id AND a.current_call_id = c.id
                WHERE c.agent_id IS NOT NULL AND (a.id IS NULL OR a.state NOT IN ('RESERVED','DIALING','CONNECTED','RECOVERY_HOLD')))
                AS valid_agent_reservations,
            (SELECT count(*) FROM calls c LEFT JOIN safety_decisions s ON s.id = c.safety_decision_id
                WHERE s.id IS NULL OR s.consumed_at IS DISTINCT FROM c.created_at OR s.consumed_in IS NULL
                    OR s.campaign_id <> c.campaign_id OR s.consumed_at >= s.expires_at)
              + (SELECT count(*) FROM used_approvals u JOIN safety_decisions s ON s.id = u.safety_decision_id
                WHERE u.total > s.approved_calls OR u.speculative > coalesce((s.safety_inputs->>'approved_speculative')::int, 0)
                    OR u.hard > coalesce((s.safety_inputs->>'approved_hard')::int, s.approved_calls)) AS consumed_approval,
            (SELECT count(*) FROM (SELECT call_id, transition FROM applied GROUP BY call_id, transition
                HAVING count(*) > 1) d) + (SELECT count(*) FROM (SELECT provider_name, provider_event_id FROM call_events
                GROUP BY provider_name, provider_event_id HAVING count(*) > 1) d) AS idempotent_events,
            (SELECT count(*) FROM calls c WHERE c.connected_at IS NOT NULL AND (c.answered_at IS NULL OR
                NOT EXISTS (SELECT 1 FROM call_events e WHERE e.provider_name = 'mock:' || c.provider
                    AND e.provider_event_id = c.idempotency_key || ':' || 'bridge' AND e.event_type = 'MOCK_BRIDGE'
                    AND (e.payload->>'agent_id')::bigint = c.agent_id AND (e.payload->>'at')::timestamptz = c.connected_at
                ))) AS bridge_requires_reservation,
            (SELECT count(*) FROM agents a LEFT JOIN active c ON c.id = a.current_call_id AND c.agent_id = a.id
                WHERE a.current_call_id IS NOT NULL AND c.id IS NULL)
              + greatest(0, (SELECT count(*) FROM active WHERE agent_id IS NOT NULL) -
                            (SELECT count(*) FROM agents WHERE current_call_id IS NOT NULL)) AS active_calls_fit_reservations,
            (SELECT count(*) FROM active) AS active_calls,
            (SELECT count(*) FROM borrowers b WHERE b.attempts <> (SELECT count(*) FROM calls c WHERE c.borrower_id = b.id))
                AS inconsistent_borrower_attempts
    """)).mappings().one()
    result = dict(row)
    violations = {key: value for key, value in result.items() if value and (key != "active_calls" or drained)}
    if violations:
        raise AssertionError(f"invariant violations: {violations}")
    return result
