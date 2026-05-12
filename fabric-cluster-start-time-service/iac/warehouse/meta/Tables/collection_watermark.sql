-- High-water mark per (workspace_id, source) so the Function can do incremental pulls
-- against the Job Instance API and friends (request only jobs ended after this timestamp).
-- One row per source; the Function updates it at the END of each successful collection.
CREATE TABLE [meta].[collection_watermark] (
    [workspace_id]        varchar(36)    NOT NULL,
    [source]              varchar(64)    NOT NULL,
    [last_collected_utc]  datetime2(3)   NOT NULL,
    [updated_at_utc]      datetime2(3)   NOT NULL
);
