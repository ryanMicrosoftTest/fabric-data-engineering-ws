-- One row per non-fatal error encountered during a collection run (e.g. a single workspace
-- returned 403, or one notebook's definition couldn't be fetched). Fatal errors that abort
-- the whole run land in collection_run_log.error_summary.
CREATE TABLE [meta].[collection_error_log] (
    [error_id]            varchar(36)    NOT NULL,
    [collector_run_id]    varchar(36)    NOT NULL,
    [workspace_id]        varchar(36)        NULL,
    [item_id]             varchar(36)        NULL,
    [endpoint]            varchar(512)       NULL,
    [http_method]         varchar(8)         NULL,
    [status_code]         int                NULL,
    [error_message]       varchar(max)       NULL,
    [attempt_number]      int                NULL,
    [occurred_at_utc]     datetime2(3)   NOT NULL
);
