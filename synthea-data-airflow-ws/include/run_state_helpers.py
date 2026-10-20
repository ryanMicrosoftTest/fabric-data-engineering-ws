"""
run_state_helpers — paste-in helpers for the Synthea stage notebooks
====================================================================
Drop these two functions into 01_synthea_generate, 02_bronze_to_silver, and
03_silver_to_gold (Fabric notebooks run on the Custom Live Pool). They implement
the per-cohort skip/restart contract against gold.control.run_state.

Usage pattern at the TOP of each stage notebook, AFTER the `parameters` cell
(which receives run_id_root, dataset_id/cohort fields, and stage):

    cohort_id = dataset_id              # for generate / bronze_to_silver
    # cohort_id = "ALL"                 # for silver_to_gold (whole-run stage)

    if already_succeeded(spark, run_id_root, cohort_id, stage):
        notebookutils.notebook.exit("SKIPPED: already SUCCEEDED")

    mark(spark, run_id_root, cohort_id, stage, "RUNNING")
    try:
        ...                            # <-- the existing notebook body
        mark(spark, run_id_root, cohort_id, stage, "SUCCEEDED")
    except Exception as e:
        mark(spark, run_id_root, cohort_id, stage, "FAILED", error=str(e))
        raise

This keeps the DAG simple (it does not query the lakehouse) and makes every
notebook self-idempotent. The existing run_id MERGE / gold-overwrite idempotency
is unchanged; this only adds cheap cohort-level skip on re-run.
"""

from datetime import datetime, timezone

RUN_STATE_TABLE = "lh_synthea_gold.control.run_state"


def already_succeeded(spark, run_id_root: str, cohort_id: str, stage: str) -> bool:
    """True if this (run_id_root, cohort_id, stage) already completed."""
    df = spark.sql(
        f"""
        SELECT 1 FROM {RUN_STATE_TABLE}
        WHERE run_id_root = '{run_id_root}'
          AND cohort_id   = '{cohort_id}'
          AND stage       = '{stage}'
          AND status      = 'SUCCEEDED'
        LIMIT 1
        """
    )
    return df.count() > 0


def mark(
    spark,
    run_id_root: str,
    cohort_id: str,
    stage: str,
    status: str,
    error: str | None = None,
) -> None:
    """Idempotent UPSERT of a run_state row for this triple via MERGE."""
    now = datetime.now(timezone.utc)
    started = "started_ts = CASE WHEN s.status = 'RUNNING' THEN t.started_ts END"
    err = (error or "").replace("'", "''")[:4000]
    spark.sql(
        f"""
        MERGE INTO {RUN_STATE_TABLE} AS t
        USING (
            SELECT
                '{run_id_root}' AS run_id_root,
                '{cohort_id}'   AS cohort_id,
                '{stage}'       AS stage,
                '{status}'      AS status,
                TIMESTAMP('{now.isoformat()}') AS ts,
                '{err}'         AS error
        ) AS s
        ON  t.run_id_root = s.run_id_root
        AND t.cohort_id   = s.cohort_id
        AND t.stage       = s.stage
        WHEN MATCHED THEN UPDATE SET
            t.status   = s.status,
            t.attempt  = COALESCE(t.attempt, 0) + CASE WHEN s.status = 'RUNNING' THEN 1 ELSE 0 END,
            t.started_ts = CASE WHEN s.status = 'RUNNING' THEN s.ts ELSE t.started_ts END,
            t.ended_ts   = CASE WHEN s.status IN ('SUCCEEDED','FAILED') THEN s.ts ELSE t.ended_ts END,
            t.error      = CASE WHEN s.status = 'FAILED' THEN s.error ELSE NULL END
        WHEN NOT MATCHED THEN INSERT (
            run_id_root, cohort_id, stage, status, attempt, started_ts, ended_ts, error
        ) VALUES (
            s.run_id_root, s.cohort_id, s.stage, s.status, 1, s.ts, NULL,
            CASE WHEN s.status = 'FAILED' THEN s.error ELSE NULL END
        )
        """
    )
