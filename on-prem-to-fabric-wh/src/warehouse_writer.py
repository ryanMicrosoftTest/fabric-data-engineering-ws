"""Fabric Warehouse writer module.

Provides helpers to:
  * Open a pyodbc connection to a Microsoft Fabric Warehouse using a
    pre-acquired Azure AD access token (passed via the ODBC
    ``SQL_COPT_SS_ACCESS_TOKEN`` connection attribute).
  * Map pandas dtypes to T-SQL column types.
  * Write a ``pandas.DataFrame`` to a warehouse table with configurable
    create/replace/append semantics.

Requires the ``ODBC Driver 18 for SQL Server`` and the ``pyodbc`` Python
package.
"""

from __future__ import annotations
import logging
from decimal import Decimal
import pandas as pd
from .token_provider import SQL_COPT_SS_ACCESS_TOKEN
import pyodbc



logger = logging.getLogger(__name__)


def connect(server: str, database: str, token_struct: bytes) -> "pyodbc.Connection":
    """
    Open a pyodbc connection to a Fabric Warehouse using an AAD access token.

    The connection string intentionally omits ``Authentication=``, ``UID=``,
    and ``PWD=``: authentication is performed exclusively via the
    ``SQL_COPT_SS_ACCESS_TOKEN`` (1256) ODBC pre-connect attribute, which
    carries an Azure AD bearer token packaged in the SQL Server token
    struct format. Including any of those keywords would either conflict
    with the access-token attribute or cause the driver to fall back to a
    different auth flow, breaking unit tests that assert on the exact
    connection string and breaking real auth at runtime.

    Args:
        server: Fully qualified Fabric Warehouse server name.
        database: Warehouse (database) name.
        token_struct: Access token already encoded in the SQL Server
            ``SQL_COPT_SS_ACCESS_TOKEN`` byte format (see
            ``token_provider``).

    Returns:
        An open ``pyodbc.Connection``. The caller is responsible for
        closing it (e.g. with a ``with contextlib.closing(...)`` block).
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )


def _pandas_dtype_to_sql(series: pd.Series) -> str:
    """Map a pandas Series dtype to a T-SQL column type string."""
    dtype = series.dtype
    kind = getattr(dtype, "kind", None)

    if pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2(6)"

    if kind == "O":
        for value in series.dropna():
            if isinstance(value, Decimal):
                return "DECIMAL(18,2)"
            break

    return "VARCHAR(8000)"


def _build_create_table_sql(df: pd.DataFrame, schema: str, table: str) -> str:
    cols = ", ".join(
        f"[{col}] {_pandas_dtype_to_sql(df[col])}" for col in df.columns
    )
    return f"CREATE TABLE [{schema}].[{table}] ({cols})"


def write_dataframe(
    conn: "pyodbc.Connection",
    df: pd.DataFrame,
    schema: str,
    table: str,
    if_exists: str = "append",
) -> int:
    """Write a DataFrame to a Fabric Warehouse table.

    Args:
        conn: Open pyodbc connection (see :func:`connect`).
        df: DataFrame to insert. Column order is preserved.
        schema: Target schema name.
        table: Target table name.
        if_exists: One of ``"append"``, ``"replace"``, ``"fail"``.

            * ``"append"`` -- create the table if it does not exist, then
              insert rows.
            * ``"replace"`` -- drop the table if it exists, recreate it,
              then insert rows.
            * ``"fail"`` -- raise ``RuntimeError`` if the table already
              exists; otherwise create it and insert rows.

    Returns:
        The number of rows inserted.

    Raises:
        ValueError: If ``if_exists`` is not one of the allowed values.
        RuntimeError: If ``if_exists="fail"`` and the table already exists.
    """
    if if_exists not in {"append", "replace", "fail"}:
        raise ValueError(
            f"if_exists must be one of 'append', 'replace', 'fail'; got {if_exists!r}"
        )

    fq_table = f"[{schema}].[{table}]"
    object_id_expr = f"OBJECT_ID('[{schema}].[{table}]', 'U')"
    create_sql = _build_create_table_sql(df, schema, table)

    cursor = conn.cursor()

    if if_exists == "replace":
        cursor.execute(
            f"IF {object_id_expr} IS NOT NULL DROP TABLE {fq_table};"
        )
        cursor.execute(create_sql)
    elif if_exists == "append":
        cursor.execute(
            f"IF {object_id_expr} IS NULL {create_sql};"
        )
    else:  # fail
        cursor.execute(f"SELECT {object_id_expr}")
        row = cursor.fetchone()
        if row is not None and row[0] is not None:
            raise RuntimeError(
                f"Table {fq_table} already exists and if_exists='fail'."
            )
        cursor.execute(create_sql)

    cleaned = df.where(pd.notnull(df), None)
    col_list = ", ".join(f"[{c}]" for c in df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = (
        f"INSERT INTO {fq_table} ({col_list}) VALUES ({placeholders})"
    )

    cursor.fast_executemany = True
    rows = list(cleaned.itertuples(index=False, name=None))
    if rows:
        cursor.executemany(insert_sql, rows)

    conn.commit()
    row_count = len(rows)
    logger.info("Inserted %d rows into %s", row_count, fq_table)
    return row_count
