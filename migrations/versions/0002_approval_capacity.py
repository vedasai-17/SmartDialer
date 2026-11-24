"""Enforce the capacity class authorized by a consumed approval."""

from alembic import op

revision = "0002_approval_capacity"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION require_approved_capacity() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            permitted integer;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.is_speculative IS DISTINCT FROM OLD.is_speculative THEN
                    RAISE EXCEPTION 'call capacity class cannot be changed' USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            SELECT CASE WHEN NEW.is_speculative
                THEN coalesce((safety_inputs->>'approved_speculative')::int, 0)
                ELSE coalesce((safety_inputs->>'approved_hard')::int, approved_calls)
                END INTO permitted
            FROM safety_decisions WHERE id = NEW.safety_decision_id;
            IF (NEW.is_speculative AND NEW.agent_id IS NOT NULL) OR (SELECT count(*) FROM calls
                WHERE safety_decision_id = NEW.safety_decision_id AND is_speculative = NEW.is_speculative) >= permitted
            THEN
                RAISE EXCEPTION 'call exceeds its approved capacity class' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER call_requires_approved_capacity BEFORE INSERT OR UPDATE OF is_speculative ON calls
        FOR EACH ROW EXECUTE FUNCTION require_approved_capacity()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER call_requires_approved_capacity ON calls")
    op.execute("DROP FUNCTION require_approved_capacity()")
