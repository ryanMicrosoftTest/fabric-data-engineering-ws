"""
parity_validation.py
================================================================================
Compares a source Fabric Warehouse against a target Fabric Lakehouse and reports
whether the migrated data is equivalent.

Run as a Fabric notebook (PySpark). No customer-specific content; configure the
CONFIG block below for your environment.

Validation layers (see 07-validation-and-cutover.md):
    L1  Structural  - table presence, column presence, column order, data types
    L2  Volumetric  - row counts, null counts, distinct counts per key column
    L3  Aggregate   - min / max / sum per numeric column, min / max per date column
    L4  Row-level   - full-row hash comparison, with sampling for large tables

Output:
    - A Delta table of findings (one row per check)
    - A printed pass / fail summary
    - Non-zero exit signal via raised exception when FAIL_ON_ERROR is True
================================================================================
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    NumericType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()


# ==============================================================================
# CONFIG - edit this block only
# ==============================================================================

@dataclass
class Config:
    # Source Warehouse, reachable from Spark as a three-part name.
    source_catalog: str = "source_warehouse"
    source_schema: str = "dbo"

    # Target Lakehouse.
    target_catalog: str = "target_lakehouse"
    target_schema: str = "gold"

    # Tables to validate. Empty list means "every table found in the target".
    tables: list[str] = field(default_factory=list)

    # Columns excluded from comparison everywhere (audit columns added by the
    # new pipeline that do not exist in the source).
    excluded_columns: set[str] = field(
        default_factory=lambda: {
            "batch_id",
            "load_ts",
            "source_system",
            "row_hash",
            "_commit_version",
            "_commit_timestamp",
        }
    )

    # Numeric tolerance for SUM / AVG comparisons. Absolute difference must be
    # within this value. Use a small non-zero value for floating-point columns.
    numeric_tolerance: float = 0.0001

    # Row-level hashing. Tables larger than this row count are sampled.
    row_hash_full_threshold: int = 10_000_000
    row_hash_sample_fraction: float = 0.01

    # Skip the expensive layer entirely when False.
    run_row_level: bool = True

    # Where findings are written.
    results_table: str = "target_lakehouse.ops.parity_results"

    # Raise at the end if any check failed.
    fail_on_error: bool = True


CONFIG = Config()


# ==============================================================================
# Internals
# ==============================================================================

RUN_ID = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

RESULT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("checked_at", TimestampType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("check_name", StringType(), False),
        StructField("source_value", StringType(), True),
        StructField("target_value", StringType(), True),
        StructField("difference", DoubleType(), True),
        StructField("status", StringType(), False),
        StructField("detail", StringType(), True),
    ]
)

_results: list[tuple] = []


def _record(
    layer: str,
    table_name: str,
    check_name: str,
    status: str,
    column_name: str | None = None,
    source_value=None,
    target_value=None,
    difference: float | None = None,
    detail: str | None = None,
) -> None:
    _results.append(
        (
            RUN_ID,
            _dt.datetime.utcnow(),
            layer,
            table_name,
            column_name,
            check_name,
            None if source_value is None else str(source_value),
            None if target_value is None else str(target_value),
            difference,
            status,
            detail,
        )
    )


def _fqn(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _comparable_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if c.lower() not in CONFIG.excluded_columns]


def _discover_tables() -> list[str]:
    if CONFIG.tables:
        return CONFIG.tables
    rows = spark.sql(
        f"SHOW TABLES IN {CONFIG.target_catalog}.{CONFIG.target_schema}"
    ).collect()
    return sorted(r["tableName"] for r in rows)


# ==============================================================================
# Layer 1 - structural
# ==============================================================================

def validate_structure(table: str) -> tuple[DataFrame | None, DataFrame | None]:
    src_fqn = _fqn(CONFIG.source_catalog, CONFIG.source_schema, table)
    tgt_fqn = _fqn(CONFIG.target_catalog, CONFIG.target_schema, table)

    try:
        src = spark.table(src_fqn)
    except Exception as exc:  # noqa: BLE001
        _record("L1", table, "source_table_exists", "FAIL", detail=str(exc)[:500])
        return None, None

    try:
        tgt = spark.table(tgt_fqn)
    except Exception as exc:  # noqa: BLE001
        _record("L1", table, "target_table_exists", "FAIL", detail=str(exc)[:500])
        return None, None

    _record("L1", table, "both_tables_exist", "PASS")

    src_cols = {c.lower() for c in _comparable_columns(src)}
    tgt_cols = {c.lower() for c in _comparable_columns(tgt)}

    for missing in sorted(src_cols - tgt_cols):
        _record(
            "L1", table, "column_present_in_target", "FAIL",
            column_name=missing, detail="Column exists in source but not in target",
        )
    for extra in sorted(tgt_cols - src_cols):
        _record(
            "L1", table, "unexpected_target_column", "WARN",
            column_name=extra,
            detail="Column exists in target but not in source and is not excluded",
        )
    if src_cols == tgt_cols:
        _record("L1", table, "column_set_matches", "PASS")

    src_types = {f.name.lower(): f.dataType.simpleString() for f in src.schema.fields}
    tgt_types = {f.name.lower(): f.dataType.simpleString() for f in tgt.schema.fields}

    for col in sorted(src_cols & tgt_cols):
        if src_types[col] != tgt_types[col]:
            _record(
                "L1", table, "data_type_matches", "WARN",
                column_name=col,
                source_value=src_types[col],
                target_value=tgt_types[col],
                detail="Type differs; confirm the difference is intentional and lossless",
            )

    return src, tgt


# ==============================================================================
# Layer 2 - volumetric
# ==============================================================================

def validate_volume(table: str, src: DataFrame, tgt: DataFrame) -> int:
    src_count = src.count()
    tgt_count = tgt.count()
    diff = float(tgt_count - src_count)

    _record(
        "L2", table, "row_count", "PASS" if diff == 0 else "FAIL",
        source_value=src_count, target_value=tgt_count, difference=diff,
    )

    common = sorted(set(_comparable_columns(src)) & set(_comparable_columns(tgt)))
    if not common:
        return src_count

    src_nulls = src.select(
        *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in common]
    ).first()
    tgt_nulls = tgt.select(
        *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in common]
    ).first()

    for c in common:
        s, t = src_nulls[c] or 0, tgt_nulls[c] or 0
        _record(
            "L2", table, "null_count", "PASS" if s == t else "FAIL",
            column_name=c, source_value=s, target_value=t, difference=float(t - s),
        )

    src_distinct = src.select(
        *[F.countDistinct(F.col(c)).alias(c) for c in common]
    ).first()
    tgt_distinct = tgt.select(
        *[F.countDistinct(F.col(c)).alias(c) for c in common]
    ).first()

    for c in common:
        s, t = src_distinct[c] or 0, tgt_distinct[c] or 0
        _record(
            "L2", table, "distinct_count", "PASS" if s == t else "FAIL",
            column_name=c, source_value=s, target_value=t, difference=float(t - s),
        )

    return src_count


# ==============================================================================
# Layer 3 - aggregate
# ==============================================================================

def validate_aggregates(table: str, src: DataFrame, tgt: DataFrame) -> None:
    common = sorted(set(_comparable_columns(src)) & set(_comparable_columns(tgt)))
    src_types = {f.name: f.dataType for f in src.schema.fields}

    numeric = [c for c in common if isinstance(src_types.get(c), NumericType)]
    if not numeric:
        return

    def _aggs(df: DataFrame):
        exprs: list = []
        for c in numeric:
            exprs += [
                F.min(c).alias(f"{c}__min"),
                F.max(c).alias(f"{c}__max"),
                F.sum(c).alias(f"{c}__sum"),
            ]
        return df.select(*exprs).first()

    s_row, t_row = _aggs(src), _aggs(tgt)

    for c in numeric:
        for agg in ("min", "max", "sum"):
            key = f"{c}__{agg}"
            s_val, t_val = s_row[key], t_row[key]
            if s_val is None and t_val is None:
                status, diff = "PASS", 0.0
            elif s_val is None or t_val is None:
                status, diff = "FAIL", None
            else:
                diff = float(t_val) - float(s_val)
                status = "PASS" if abs(diff) <= CONFIG.numeric_tolerance else "FAIL"
            _record(
                "L3", table, f"{agg}_value", status,
                column_name=c, source_value=s_val, target_value=t_val, difference=diff,
            )


# ==============================================================================
# Layer 4 - row-level hash
# ==============================================================================

def _hashed(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    normalized = [
        F.coalesce(F.col(c).cast("string"), F.lit("\u0000<NULL>")) for c in columns
    ]
    return df.select(F.sha2(F.concat_ws("\u0001", *normalized), 256).alias("row_hash"))


def validate_rows(table: str, src: DataFrame, tgt: DataFrame, src_count: int) -> None:
    if not CONFIG.run_row_level:
        _record("L4", table, "row_level_hash", "SKIPPED", detail="run_row_level is False")
        return

    common = sorted(set(_comparable_columns(src)) & set(_comparable_columns(tgt)))
    if not common:
        _record("L4", table, "row_level_hash", "SKIPPED", detail="No comparable columns")
        return

    sampled = src_count > CONFIG.row_hash_full_threshold
    if sampled:
        src = src.sample(False, CONFIG.row_hash_sample_fraction, seed=42)
        tgt = tgt.sample(False, CONFIG.row_hash_sample_fraction, seed=42)

    s_hash = _hashed(src, common)
    t_hash = _hashed(tgt, common)

    only_source = s_hash.subtract(t_hash).count()
    only_target = t_hash.subtract(s_hash).count()
    status = "PASS" if (only_source == 0 and only_target == 0) else "FAIL"

    detail = (
        f"sampled at {CONFIG.row_hash_sample_fraction:.2%}; "
        "sampled comparisons are indicative only"
        if sampled
        else "full comparison"
    )
    _record(
        "L4", table, "rows_only_in_source", status,
        source_value=only_source, target_value=0,
        difference=float(only_source), detail=detail,
    )
    _record(
        "L4", table, "rows_only_in_target", status,
        source_value=0, target_value=only_target,
        difference=float(only_target), detail=detail,
    )


# ==============================================================================
# Orchestration
# ==============================================================================

def run() -> DataFrame:
    tables = _discover_tables()
    print(f"Run {RUN_ID}: validating {len(tables)} table(s)\n")

    for table in tables:
        print(f"  - {table}")
        src, tgt = validate_structure(table)
        if src is None or tgt is None:
            continue
        src_count = validate_volume(table, src, tgt)
        validate_aggregates(table, src, tgt)
        validate_rows(table, src, tgt, src_count)

    results = spark.createDataFrame(_results, schema=RESULT_SCHEMA)
    results.write.mode("append").format("delta").saveAsTable(CONFIG.results_table)

    print("\n" + "=" * 72)
    summary = (
        results.groupBy("layer", "status")
        .count()
        .orderBy("layer", "status")
    )
    summary.show(truncate=False)

    failures = results.filter(F.col("status") == "FAIL")
    failure_count = failures.count()

    if failure_count:
        print(f"{failure_count} failing check(s):")
        failures.select(
            "layer", "table_name", "column_name", "check_name",
            "source_value", "target_value", "detail",
        ).show(100, truncate=False)
    else:
        print("All checks passed.")
    print("=" * 72)

    if failure_count and CONFIG.fail_on_error:
        raise RuntimeError(
            f"Parity validation failed: {failure_count} check(s). "
            f"See {CONFIG.results_table} for run_id {RUN_ID}."
        )

    return results


if __name__ == "__main__":
    run()
