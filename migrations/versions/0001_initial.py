"""SmartDialer v3 PostgreSQL foundation."""

from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE campaigns (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name text NOT NULL UNIQUE,
            mode text NOT NULL DEFAULT 'STRICT_PROGRESSIVE'
                CHECK (mode IN ('STRICT_PROGRESSIVE', 'BOUNDED_PREDICTIVE', 'HYBRID')),
            status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED', 'STOPPED')),
            max_concurrent_calls integer NOT NULL CHECK (max_concurrent_calls >= 0),
            max_speculative_calls integer NOT NULL DEFAULT 0 CHECK (max_speculative_calls >= 0),
            provider_rate_limit integer NOT NULL CHECK (provider_rate_limit >= 0),
            risk_budget double precision NOT NULL DEFAULT 0 CHECK (risk_budget BETWEEN 0 AND 1),
            fallback_mode text NOT NULL DEFAULT 'STRICT_PROGRESSIVE'
                CHECK (fallback_mode = 'STRICT_PROGRESSIVE'),
            config jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object')
        )
    """)
    op.execute("""
        CREATE TABLE agents (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            campaign_id bigint NOT NULL REFERENCES campaigns,
            state text NOT NULL DEFAULT 'OFFLINE' CHECK (state IN (
                'OFFLINE', 'AVAILABLE', 'RESERVED', 'DIALING', 'CONNECTED', 'WRAP_UP',
                'PAUSED', 'RECOVERY_HOLD'
            )),
            version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
            current_call_id uuid,
            last_heartbeat_at timestamptz,
            lease_expires_at timestamptz,
            wrap_up_ends_at timestamptz,
            last_call_ended_at timestamptz,
            CHECK ((state IN ('RESERVED', 'DIALING', 'CONNECTED', 'RECOVERY_HOLD')) =
                   (current_call_id IS NOT NULL)),
            CHECK ((state = 'WRAP_UP') = (wrap_up_ends_at IS NOT NULL)),
            CHECK ((last_heartbeat_at IS NULL) = (lease_expires_at IS NULL)),
            UNIQUE (id, campaign_id),
            UNIQUE (id, current_call_id)
        )
    """)
    op.execute("CREATE UNIQUE INDEX one_owner_per_call ON agents (current_call_id) WHERE current_call_id IS NOT NULL")
    op.execute("CREATE INDEX available_agents ON agents (campaign_id, last_call_ended_at NULLS FIRST, id) WHERE state = 'AVAILABLE'")
    op.execute("""
        CREATE TABLE borrowers (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            campaign_id bigint NOT NULL REFERENCES campaigns,
            phone_normalized text NOT NULL UNIQUE CHECK (phone_normalized ~ '^[1-9][0-9]{7,14}$'),
            state text NOT NULL DEFAULT 'QUEUED' CHECK (state IN (
                'QUEUED', 'RESERVED', 'CONTACTED', 'RETRY_SCHEDULED', 'EXHAUSTED', 'SUPPRESSED'
            )),
            version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
            priority integer NOT NULL DEFAULT 0,
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts > 0 AND attempts <= max_attempts),
            queued_at timestamptz NOT NULL,
            next_attempt_at timestamptz NOT NULL,
            last_attempt_at timestamptz,
            UNIQUE (id, campaign_id)
        )
    """)
    op.execute("""
        CREATE INDEX eligible_borrowers ON borrowers
            (campaign_id, priority DESC, next_attempt_at, queued_at, attempts, id)
            WHERE state IN ('QUEUED', 'RETRY_SCHEDULED') AND attempts < max_attempts
    """)
    op.execute("""
        CREATE TABLE safety_decisions (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            campaign_id bigint NOT NULL REFERENCES campaigns,
            action text NOT NULL CHECK (action IN ('APPROVE','REDUCE','REJECT','FALLBACK_TO_PROGRESSIVE')),
            requested_calls integer NOT NULL CHECK (requested_calls >= 0),
            approved_calls integer NOT NULL CHECK (approved_calls BETWEEN 0 AND requested_calls),
            effective_mode text NOT NULL CHECK (effective_mode IN ('STRICT_PROGRESSIVE','BOUNDED_PREDICTIVE','HYBRID')),
            reasons text[] NOT NULL CHECK (cardinality(reasons) > 0),
            pacing_inputs jsonb NOT NULL,
            pacing_reasoning jsonb NOT NULL,
            safety_inputs jsonb NOT NULL,
            decided_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL CHECK (expires_at > decided_at),
            consumed_at timestamptz,
            consumed_in xid8,
            CHECK (action <> 'REJECT' OR approved_calls = 0),
            CHECK ((consumed_at IS NULL) = (consumed_in IS NULL)),
            CHECK (consumed_at IS NULL OR (
                consumed_at >= decided_at AND consumed_at < expires_at AND approved_calls > 0
            )),
            UNIQUE (id, campaign_id)
        )
    """)
    op.execute("CREATE INDEX decisions_by_campaign ON safety_decisions (campaign_id, decided_at DESC, id DESC)")
    op.execute("""
        CREATE TABLE calls (
            id uuid PRIMARY KEY,
            campaign_id bigint NOT NULL REFERENCES campaigns,
            borrower_id bigint NOT NULL,
            agent_id bigint,
            safety_decision_id bigint NOT NULL,
            idempotency_key text NOT NULL UNIQUE,
            state text NOT NULL,
            state_rank smallint NOT NULL,
            is_speculative boolean NOT NULL DEFAULT false,
            provider text NOT NULL,
            provider_call_id text,
            attempt_number integer NOT NULL CHECK (attempt_number > 0),
            created_at timestamptz NOT NULL,
            initiated_at timestamptz,
            ringing_at timestamptz,
            answered_at timestamptz,
            connected_at timestamptz,
            ended_at timestamptz,
            abandoned_at timestamptz,
            failure_reason text,
            version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
            lease_owner text,
            lease_until timestamptz,
            fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
            next_retry_at timestamptz NOT NULL,
            dispatch_attempts integer NOT NULL DEFAULT 0 CHECK (dispatch_attempts >= 0),
            active_agent_id bigint GENERATED ALWAYS AS (
                CASE WHEN state_rank < 100 THEN agent_id ELSE NULL END
            ) STORED,
            CHECK (CASE state
                WHEN 'QUEUED' THEN state_rank = 10
                WHEN 'RESERVED' THEN state_rank = 20
                WHEN 'INITIATING' THEN state_rank = 25
                WHEN 'INITIATED' THEN state_rank = 30
                WHEN 'RINGING' THEN state_rank = 40
                WHEN 'ANSWERED' THEN state_rank = 50
                WHEN 'CONNECTED' THEN state_rank = 60
                WHEN 'UNKNOWN' THEN state_rank IN (25, 30, 40, 50, 60)
                WHEN 'COMPLETED' THEN state_rank = 100
                WHEN 'FAILED' THEN state_rank = 100
                WHEN 'CANCELLED' THEN state_rank = 100
                WHEN 'BUSY' THEN state_rank = 100
                WHEN 'NO_ANSWER' THEN state_rank = 100
                ELSE false END),
            CHECK ((state_rank = 100) = (ended_at IS NOT NULL)),
            CHECK (is_speculative OR agent_id IS NOT NULL),
            CHECK (state <> 'ANSWERED' OR answered_at IS NOT NULL),
            CHECK (state <> 'CONNECTED' OR (
                agent_id IS NOT NULL AND answered_at IS NOT NULL AND connected_at IS NOT NULL
            )),
            CHECK (connected_at IS NULL OR (agent_id IS NOT NULL AND answered_at IS NOT NULL)),
            CHECK (abandoned_at IS NULL OR answered_at IS NOT NULL),
            CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
            CHECK (idempotency_key = campaign_id::text || ':' || borrower_id::text || ':' || attempt_number::text),
            FOREIGN KEY (borrower_id, campaign_id) REFERENCES borrowers (id, campaign_id),
            FOREIGN KEY (agent_id, campaign_id) REFERENCES agents (id, campaign_id),
            FOREIGN KEY (safety_decision_id, campaign_id) REFERENCES safety_decisions (id, campaign_id),
            FOREIGN KEY (active_agent_id, id) REFERENCES agents (id, current_call_id)
                DEFERRABLE INITIALLY DEFERRED,
            UNIQUE (id, active_agent_id),
            UNIQUE (provider, provider_call_id)
        )
    """)
    op.execute("""
        ALTER TABLE agents ADD CONSTRAINT agent_reservation_matches_call
            FOREIGN KEY (current_call_id, id) REFERENCES calls (id, active_agent_id)
            DEFERRABLE INITIALLY DEFERRED
    """)
    op.execute("CREATE UNIQUE INDEX one_active_call_per_agent ON calls (agent_id) WHERE state_rank < 100 AND agent_id IS NOT NULL")
    op.execute("CREATE UNIQUE INDEX one_active_call_per_borrower ON calls (borrower_id) WHERE state_rank < 100")
    op.execute("CREATE INDEX active_calls_by_campaign ON calls (campaign_id) WHERE state_rank < 100")
    op.execute("CREATE INDEX call_admissions_by_campaign ON calls (campaign_id, provider, created_at DESC)")
    op.execute("CREATE INDEX dispatchable_calls ON calls (next_retry_at, id) WHERE state = 'RESERVED'")
    op.execute("CREATE INDEX leased_calls ON calls (lease_until) WHERE lease_until IS NOT NULL AND state_rank < 100")
    op.execute("""
        CREATE FUNCTION require_safety_approval() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            approval safety_decisions%ROWTYPE;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF OLD.safety_decision_id IS DISTINCT FROM NEW.safety_decision_id THEN
                    RAISE EXCEPTION 'call approval cannot be changed' USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            SELECT * INTO approval FROM safety_decisions WHERE id = NEW.safety_decision_id;
            IF NOT FOUND OR approval.campaign_id <> NEW.campaign_id
                OR approval.consumed_in IS DISTINCT FROM pg_current_xact_id()
                OR approval.consumed_at IS DISTINCT FROM NEW.created_at
                OR approval.approved_calls = 0
                OR (approval.effective_mode = 'STRICT_PROGRESSIVE' AND NEW.is_speculative)
                OR NEW.state <> 'RESERVED'
            THEN
                RAISE EXCEPTION 'call requires a valid approval consumed in this transaction'
                    USING ERRCODE = '23514';
            END IF;
            IF (SELECT count(*) FROM calls WHERE safety_decision_id = approval.id) >= approval.approved_calls THEN
                RAISE EXCEPTION 'call exceeds approval budget' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("CREATE INDEX calls_by_approval ON calls (safety_decision_id)")
    op.execute("""
        CREATE TRIGGER call_requires_approval BEFORE INSERT OR UPDATE OF safety_decision_id ON calls
        FOR EACH ROW EXECUTE FUNCTION require_safety_approval()
    """)
    op.execute("""
        CREATE TABLE call_events (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            call_id uuid REFERENCES calls,
            provider_name text NOT NULL,
            provider_event_id text NOT NULL,
            provider_sequence bigint,
            event_type text NOT NULL,
            provider_timestamp timestamptz,
            received_at timestamptz NOT NULL,
            processed_at timestamptz,
            payload jsonb NOT NULL,
            processing_status text NOT NULL DEFAULT 'PENDING'
                CHECK (processing_status IN ('PENDING','APPLIED','DUPLICATE','STALE','INVALID','UNKNOWN_CALL')),
            UNIQUE (provider_name, provider_event_id)
        )
    """)
    op.execute("CREATE INDEX pending_events ON call_events (received_at, id) WHERE processing_status = 'PENDING'")
    op.execute("""
        CREATE TABLE audit_log (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            occurred_at timestamptz NOT NULL,
            campaign_id bigint REFERENCES campaigns,
            entity_type text NOT NULL,
            entity_id text NOT NULL,
            action text NOT NULL,
            details jsonb NOT NULL DEFAULT '{}'::jsonb
        )
    """)
    op.execute("CREATE INDEX audit_by_entity ON audit_log (entity_type, entity_id, id)")


def downgrade() -> None:
    op.execute("DROP TABLE audit_log, call_events, calls, safety_decisions, borrowers, agents, campaigns")
    op.execute("DROP FUNCTION require_safety_approval()")
