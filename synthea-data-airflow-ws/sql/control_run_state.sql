-- control.run_state — per-cohort, per-stage execution state for skip/restart.
--
-- Add this DDL to the 00_run_log_init notebook (run via the DAG's init_run_log
-- task). It is idempotent: safe to run on every DAG run. Lives in the gold
-- lakehouse alongside agent.run_log.
--
-- The "skip/restart" contract (see include/run_state_helpers.py):
--   * Each stage notebook UPSERTs status='RUNNING' on entry for its
--     (run_id_root, cohort_id, stage), then 'SUCCEEDED' / 'FAILED' on exit.
--   * On entry it first checks for an existing 'SUCCEEDED' row for the same
--     triple; if found it exits early (skip). Re-triggering the DAG with the
--     SAME run_id_root therefore replays only failed/missing cohorts; a NEW
--     run_id_root forces a clean full run.

CREATE TABLE IF NOT EXISTS lh_synthea_gold.control.run_state (
    run_id_root   STRING  COMMENT 'Run grouping key (utcnow yyyyMMddHHmmss)',
    cohort_id     STRING  COMMENT 'Cohort = dataset_id; "ALL" for whole-run stages',
    stage         STRING  COMMENT 'generate | bronze_to_silver | silver_to_gold',
    status        STRING  COMMENT 'RUNNING | SUCCEEDED | FAILED',
    attempt       INT     COMMENT 'Incrementing attempt counter',
    started_ts    TIMESTAMP,
    ended_ts      TIMESTAMP,
    error         STRING  COMMENT 'Truncated error text when status=FAILED'
)
USING DELTA;
