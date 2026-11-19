"""Run analyst-approved investigations in priority order."""
import time

from fastapi import HTTPException

from .agent.cli import report_exit_code
from .api import db_connect, investigation_case, run_investigation


def main():
    while True:
        with db_connect() as owner:
            locked = owner.execute(
                "SELECT pg_try_advisory_lock(hashtextextended('aml-dispatcher', 0)) AS locked"
            ).fetchone()["locked"]
            if not locked:
                time.sleep(2)
                continue
            owner.execute("""
                UPDATE investigation_queue SET status = 'failed', error = 'Dispatcher interrupted; inspect the case before retrying',
                    updated_at = now() WHERE status = 'running'
            """)
            owner.commit()
            while True:
                row = owner.execute("""
                    UPDATE investigation_queue SET status = 'running', error = NULL, updated_at = now()
                    WHERE (alert_id, party) = (
                        SELECT alert_id, party FROM investigation_queue
                        WHERE status = 'queued' AND available_at <= now() ORDER BY priority DESC, created_at
                        FOR UPDATE SKIP LOCKED LIMIT 1
                    ) RETURNING alert_id, party
                """).fetchone()
                owner.commit()
                if row is None:
                    time.sleep(2)
                    continue
                try:
                    with db_connect() as conn:
                        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        case = investigation_case(conn, row["alert_id"], row["party"])
                        owner.execute("""
                            UPDATE investigation_queue SET case_id = %s, updated_at = now()
                            WHERE alert_id = %s AND party = %s
                        """, (case["case_id"], row["alert_id"], row["party"]))
                        owner.commit()
                        result = run_investigation(conn, case)
                    status = "completed" if report_exit_code(result) == 0 else "partial"
                    owner.execute("""
                        UPDATE investigation_queue SET status = %s, case_id = %s,
                            updated_at = now() WHERE alert_id = %s AND party = %s
                    """, (status, result["case"]["case_id"], row["alert_id"], row["party"]))
                except HTTPException as exc:
                    retry = exc.status_code == 429 or (
                        exc.status_code == 409 and exc.detail == "Trigger transaction is not archived yet; retry shortly"
                    )
                    status = "queued" if retry else "failed"
                    owner.execute("""
                        UPDATE investigation_queue SET status = %s, error = %s, updated_at = now(),
                            available_at = now() + interval '2 seconds'
                        WHERE alert_id = %s AND party = %s
                    """, (status, str(exc.detail), row["alert_id"], row["party"]))
                except Exception:
                    owner.execute("""
                        UPDATE investigation_queue SET status = 'failed', error = %s, updated_at = now()
                        WHERE alert_id = %s AND party = %s
                    """, ("Investigation failed; inspect the case", row["alert_id"], row["party"]))
                owner.commit()


if __name__ == "__main__":
    main()
