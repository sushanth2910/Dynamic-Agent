#!/usr/bin/env python3
"""
MongoDB -> PostgreSQL sync worker.

- One Mongo collection -> one Postgres table
- Each top-level Mongo key -> one Postgres column
- Arrays/objects stored as JSONB in their column
- _id stored as TEXT PRIMARY KEY

Config via environment variables:
  MONGO_DETAILS     (default: mongodb://localhost:27017)
  MONGO_URI         (fallback alias)
  DB_NAME           (required)
  MONGO_DB          (fallback alias)
  PG_DSN            (optional full DSN)
  PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD used if PG_DSN missing
  PGUSER defaults to the current OS user if not provided
  COLLECTIONS       (optional, comma-separated list; default: all)
  EXCLUDE_COLLECTIONS (optional, comma-separated list; excluded from sync)
  BACKFILL          (default: true)
  BATCH_SIZE        (default: 500)
  COPY_ENABLED      (default: true; uses COPY for large batches)
  COPY_MIN_ROWS     (default: 200; min rows before COPY is used)
  WATCH             (default: true)
  LOG_LEVEL         (default: INFO)
"""

import os
import re
import sys
import time
import json
import logging
import hashlib
import getpass
import math
import io
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId, Decimal128, BSON

import psycopg2
from psycopg2 import sql
from psycopg2 import extras
from psycopg2 import errors as pg_errors

MAX_IDENT_LEN = 63

# ============================================================================
# FK EXTRACTION CONFIGURATION
# ============================================================================
# Fields that contain single ObjectIds and should be extracted as TEXT (FK-compatible)
# instead of being stored as JSONB. Format: { "collection": ["field1", "field2"] }
FK_EXTRACT_FIELDS = {
    "users": ["department", "admin"],
    "patients": ["admin", "contact_id"],
    "phonebooks": ["admin"],
    "branches": ["admin"],
    "departments": ["admin"],
    "appointments": ["user_id", "admin", "department_id", "branch_id", "contact_id"],
}

# Fields that are arrays of ObjectIds and should create junction tables
# Format: { "collection": {"field_name": "target_collection"} }
JUNCTION_TABLE_FIELDS = {
    "users": {"branch": "branches"},
    "phonebooks": {"branch": "branches"},
}
# ============================================================================


@dataclass
class ColumnInfo:
    mongo_key: str
    pg_column: str
    pg_type: str


@dataclass
class CollectionState:
    collection_name: str
    pg_table: str
    columns: Dict[str, ColumnInfo]
    column_order: List[ColumnInfo]
    upsert_sql: str


@dataclass
class SyncSettings:
    copy_enabled: bool
    copy_min_rows: int


class TypeConflict(Exception):
    pass


def short_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]


def sanitize_identifier(name: str, prefix: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = prefix
    if len(base) > MAX_IDENT_LEN:
        h = short_hash(name)
        base = base[: MAX_IDENT_LEN - len(h) - 1] + "_" + h
    return base


def make_hashed_name(base: str, original: str, attempt: int) -> str:
    h = short_hash(f"{original}:{attempt}")
    trimmed = base
    if len(trimmed) + 1 + len(h) > MAX_IDENT_LEN:
        trimmed = trimmed[: MAX_IDENT_LEN - len(h) - 1]
    return f"{trimmed}_{h}"


def build_pg_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if dsn:
        return dsn
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER") or os.getenv("USER") or getpass.getuser() or "postgres"
    password = os.getenv("PGPASSWORD", "")
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return f"postgresql://{user}@{host}:{port}/{db}"


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"").strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        return


def env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}




def ensure_registry_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mongo_collection_registry (
                collection_name TEXT PRIMARY KEY,
                pg_table_name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mongo_schema_registry (
                collection_name TEXT NOT NULL,
                mongo_key TEXT NOT NULL,
                pg_column_name TEXT NOT NULL,
                pg_type TEXT NOT NULL,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (collection_name, mongo_key),
                UNIQUE (collection_name, pg_column_name),
                FOREIGN KEY (collection_name)
                  REFERENCES mongo_collection_registry(collection_name)
                  ON DELETE CASCADE
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_schema_registry_collection
                ON mongo_schema_registry(collection_name);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mongo_resume_tokens (
                scope TEXT PRIMARY KEY,
                token BYTEA NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
    conn.commit()


def get_or_create_table_name(conn, collection_name: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_table_name FROM mongo_collection_registry WHERE collection_name = %s",
            (collection_name,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    base = sanitize_identifier(collection_name, "coll")
    attempt = 0
    while True:
        attempt += 1
        candidate = base if attempt == 1 else make_hashed_name(base, collection_name, attempt)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mongo_collection_registry (collection_name, pg_table_name) VALUES (%s, %s)",
                    (collection_name, candidate),
                )
            conn.commit()
            return candidate
        except psycopg2.IntegrityError:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_table_name FROM mongo_collection_registry WHERE collection_name = %s",
                    (collection_name,),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
            # else collision on pg_table_name; retry with new hash


def ensure_table(conn, pg_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {table} (_id TEXT PRIMARY KEY)").format(
                table=sql.Identifier(pg_table)
            )
        )
    conn.commit()


def get_or_create_column_name(conn, collection_name: str, mongo_key: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_column_name
            FROM mongo_schema_registry
            WHERE collection_name = %s AND mongo_key = %s
            """,
            (collection_name, mongo_key),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    base = sanitize_identifier(mongo_key, "key")
    attempt = 0
    while True:
        attempt += 1
        candidate = base if attempt == 1 else make_hashed_name(base, mongo_key, attempt)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mongo_schema_registry
                        (collection_name, mongo_key, pg_column_name, pg_type)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (collection_name, mongo_key, candidate, "pending"),
                )
            conn.commit()
            return candidate
        except psycopg2.IntegrityError:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pg_column_name
                    FROM mongo_schema_registry
                    WHERE collection_name = %s AND mongo_key = %s
                    """,
                    (collection_name, mongo_key),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
            # else collision on pg_column_name; retry


def load_columns(conn, collection_name: str) -> Dict[str, ColumnInfo]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT mongo_key, pg_column_name, pg_type
            FROM mongo_schema_registry
            WHERE collection_name = %s
            """,
            (collection_name,),
        )
        rows = cur.fetchall()
    cols = {}
    for mongo_key, pg_column, pg_type in rows:
        if pg_type == "pending":
            continue
        cols[mongo_key] = ColumnInfo(mongo_key=mongo_key, pg_column=pg_column, pg_type=pg_type)
    return cols


# ============================================================================
# FK EXTRACTION HELPER FUNCTIONS
# ============================================================================

def is_objectid_value(value: Any) -> bool:
    """Check if value is a single ObjectId or can be extracted as one."""
    if isinstance(value, ObjectId):
        return True
    if isinstance(value, str) and len(value) == 24:
        # Check if it looks like an ObjectId hex string
        try:
            int(value, 16)
            return True
        except ValueError:
            return False
    return False


def is_objectid_array(value: Any) -> bool:
    """Check if value is an array of ObjectIds."""
    if not isinstance(value, list):
        return False
    if len(value) == 0:
        return True  # Empty array is considered valid
    return all(is_objectid_value(v) for v in value)


def extract_objectid(value: Any) -> Optional[str]:
    """
    Extract ObjectId string from various MongoDB formats.
    Returns None if value is None or cannot be extracted.
    """
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        # Handle { "$oid": "..." } format from MongoDB extended JSON
        if "$oid" in value and len(value) == 1:
            return str(value["$oid"])
        # Handle { "_id": "..." } format
        if "_id" in value and len(value) == 1:
            oid = value["_id"]
            if isinstance(oid, ObjectId):
                return str(oid)
            return str(oid) if oid else None
    return None


def should_extract_as_fk(collection_name: str, field_name: str) -> bool:
    """Check if a field should be extracted as TEXT FK instead of JSONB."""
    return field_name in FK_EXTRACT_FIELDS.get(collection_name, [])


def is_junction_table_field(collection_name: str, field_name: str) -> bool:
    """Check if a field should create a junction table."""
    return field_name in JUNCTION_TABLE_FIELDS.get(collection_name, {})


def get_junction_target(collection_name: str, field_name: str) -> Optional[str]:
    """Get the target collection for a junction table field."""
    return JUNCTION_TABLE_FIELDS.get(collection_name, {}).get(field_name)

# ============================================================================


def infer_pg_type(value: Any) -> str:

    if value is None:
        return "jsonb"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if isinstance(value, Decimal128):
        return "numeric"
    if isinstance(value, Decimal):
        return "numeric"
    if isinstance(value, datetime):
        return "timestamptz"
    if isinstance(value, (ObjectId, str)):
        return "text"
    if isinstance(value, (list, dict)):
        return "jsonb"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "bytea"
    return "jsonb"


def infer_pg_type_with_context(
    value: Any, 
    collection_name: str, 
    field_name: str
) -> Optional[str]:
    """
    Infer PostgreSQL type with collection/field context.
    Returns None if field should be skipped (junction table fields).
    Returns 'text' for FK extraction fields even if value is dict/ObjectId.
    """
    # Skip junction table fields - they go to separate tables
    if is_junction_table_field(collection_name, field_name):
        return None  # Signal to skip this field
    
    # For FK extraction fields, always use text if we can extract an ObjectId
    if should_extract_as_fk(collection_name, field_name):
        extracted = extract_objectid(value)
        if extracted is not None or value is None:
            return "text"
        # Fall through to normal inference if extraction failed
    
    # Default type inference
    return infer_pg_type(value)


def is_type_compatible_with_context(
    value: Any, 
    pg_type: str,
    collection_name: str,
    field_name: str
) -> bool:
    """Type compatibility check that considers FK extraction."""
    if value is None:
        return True
    if pg_type == "text" and should_extract_as_fk(collection_name, field_name):
        # For FK fields, dict/ObjectId values are compatible with text
        extracted = extract_objectid(value)
        return extracted is not None
    return is_type_compatible(value, pg_type)


def is_type_compatible(value: Any, pg_type: str) -> bool:
    if value is None:
        return True
    if pg_type == "jsonb":
        return True
    if pg_type == "text":
        return not isinstance(value, (list, dict))
    if pg_type == "bigint":
        return isinstance(value, int) and not isinstance(value, bool)
    if pg_type == "double precision":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if pg_type == "numeric":
        return isinstance(value, (int, float, Decimal, Decimal128)) and not isinstance(value, bool)
    if pg_type == "boolean":
        return isinstance(value, bool)
    if pg_type == "timestamptz":
        return isinstance(value, datetime)
    if pg_type == "bytea":
        return isinstance(value, (bytes, bytearray, memoryview))
    return False


def to_json_compatible(value: Any) -> Any:
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        dec = value.to_decimal()
        if not dec.is_finite():
            return None
        return str(dec)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, list):
        return [to_json_compatible(v) for v in value]
    if isinstance(value, dict):
        return {k: to_json_compatible(v) for k, v in value.items()}
    return value


def adapt_value(value: Any, pg_type: str) -> Any:
    if value is None:
        return None
    if pg_type == "jsonb":
        return extras.Json(to_json_compatible(value), dumps=json.dumps)
    if pg_type == "text":
        return str(value)
    if pg_type == "bigint":
        if isinstance(value, bool):
            raise TypeConflict("bool is not bigint")
        try:
            return int(value)
        except (ValueError, TypeError):
            raise TypeConflict("cannot convert to bigint")
    if pg_type == "double precision":
        if isinstance(value, bool):
            raise TypeConflict("bool is not double precision")
        try:
            return float(value)
        except (ValueError, TypeError):
            raise TypeConflict("cannot convert to double precision")
    if pg_type == "numeric":
        if isinstance(value, Decimal128):
            return value.to_decimal()
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool):
            raise TypeConflict("bool is not numeric")
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            raise TypeConflict("cannot convert to numeric")
    if pg_type == "boolean":
        if not isinstance(value, bool):
            raise TypeConflict("not boolean")
        return value
    if pg_type == "timestamptz":
        if not isinstance(value, datetime):
            raise TypeConflict("not datetime")
        return value
    if pg_type == "bytea":
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeConflict("not bytes")
        return bytes(value)
    return value


def adapt_value_with_context(
    value: Any, 
    pg_type: str,
    collection_name: str,
    field_name: str
) -> Any:
    """
    Adapt value with FK extraction support.
    For FK fields, extract ObjectId before converting to text.
    """
    if value is None:
        return None
    
    # For FK extraction fields, extract ObjectId first
    if pg_type == "text" and should_extract_as_fk(collection_name, field_name):
        extracted = extract_objectid(value)
        if extracted is not None:
            return extracted
        # If extraction failed but value is not None, try normal text conversion
    
    return adapt_value(value, pg_type)


def escape_copy_text(value: str) -> str:
    value = value.replace("\\", "\\\\")
    value = value.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    return value


def encode_copy_value(value: Any, pg_type: str) -> Optional[str]:
    if value is None:
        return None
    if pg_type == "jsonb":
        return json.dumps(to_json_compatible(value), ensure_ascii=True, separators=(",", ":"))
    if pg_type == "text":
        return str(value)
    if pg_type == "bigint":
        if isinstance(value, bool):
            raise TypeConflict("bool is not bigint")
        try:
            return str(int(value))
        except (ValueError, TypeError):
            raise TypeConflict("cannot convert to bigint")
    if pg_type == "double precision":
        if isinstance(value, bool):
            raise TypeConflict("bool is not double precision")
        try:
            return str(float(value))
        except (ValueError, TypeError):
            raise TypeConflict("cannot convert to double precision")
    if pg_type == "numeric":
        if isinstance(value, Decimal128):
            return str(value.to_decimal())
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, bool):
            raise TypeConflict("bool is not numeric")
        try:
            return str(Decimal(str(value)))
        except (ValueError, TypeError):
            raise TypeConflict("cannot convert to numeric")
    if pg_type == "boolean":
        if not isinstance(value, bool):
            raise TypeConflict("not boolean")
        return "true" if value else "false"
    if pg_type == "timestamptz":
        if not isinstance(value, datetime):
            raise TypeConflict("not datetime")
        return value.isoformat()
    if pg_type == "bytea":
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeConflict("not bytes")
        return "\\x" + bytes(value).hex()
    return json.dumps(to_json_compatible(value), ensure_ascii=True)


def build_copy_buffer(docs: List[Dict[str, Any]], state: CollectionState) -> io.StringIO:
    buf = io.StringIO()
    for doc in docs:
        if "_id" not in doc:
            raise ValueError("document missing _id")
        id_value = doc["_id"]
        fields: List[Optional[str]] = [str(id_value)]
        for col in state.column_order:
            fields.append(encode_copy_value(doc.get(col.mongo_key), col.pg_type))
        escaped_fields = []
        for field in fields:
            if field is None:
                escaped_fields.append("\\N")
            else:
                escaped_fields.append(escape_copy_text(field))
        buf.write("\t".join(escaped_fields))
        buf.write("\n")
    buf.seek(0)
    return buf


def staging_table_name(pg_table: str) -> str:
    return sanitize_identifier(f"{pg_table}_staging", "staging")


def prepare_staging_table(conn, pg_table: str) -> str:
    staging = staging_table_name(pg_table)
    staging_ident = sql.Identifier("pg_temp", staging)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {staging}").format(staging=staging_ident))
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {staging} (LIKE {table})").format(
                staging=staging_ident,
                table=sql.Identifier(pg_table),
            )
        )
    return staging


def build_upsert_from_staging(
    pg_table: str,
    staging: str,
    cols: List[str],
    staging_schema: str = "pg_temp",
) -> sql.SQL:
    insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    select_cols = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    if len(cols) > 1:
        updates = sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in cols[1:]
        )
        query = sql.SQL(
            "INSERT INTO {table} ({cols}) SELECT {select_cols} FROM {staging} "
            "ON CONFLICT (_id) DO UPDATE SET {updates}"
        ).format(
            table=sql.Identifier(pg_table),
            cols=insert_cols,
            select_cols=select_cols,
            staging=sql.Identifier(staging_schema, staging),
            updates=updates,
        )
    else:
        query = sql.SQL(
            "INSERT INTO {table} ({cols}) SELECT {select_cols} FROM {staging} "
            "ON CONFLICT (_id) DO NOTHING"
        ).format(
            table=sql.Identifier(pg_table),
            cols=insert_cols,
            select_cols=select_cols,
            staging=sql.Identifier(staging_schema, staging),
        )
    return query


def copy_upsert_batch(conn, state: CollectionState, docs: List[Dict[str, Any]]) -> None:
    if not docs:
        return
    staging = prepare_staging_table(conn, state.pg_table)
    staging_ident = sql.Identifier("pg_temp", staging)
    cols = ["_id"] + [c.pg_column for c in state.column_order]
    buffer = build_copy_buffer(docs, state)
    copy_sql = sql.SQL(
        "COPY {staging} ({cols}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    ).format(
        staging=staging_ident,
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
    )
    upsert_sql = build_upsert_from_staging(state.pg_table, staging, cols)
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql.as_string(conn), buffer)
        cur.execute(upsert_sql)
    conn.commit()


def update_column_type(conn, collection_name: str, mongo_key: str, pg_type: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mongo_schema_registry
            SET pg_type = %s, last_seen_at = now()
            WHERE collection_name = %s AND mongo_key = %s
            """,
            (pg_type, collection_name, mongo_key),
        )
    conn.commit()


def add_column(conn, pg_table: str, pg_column: str, pg_type: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {coltype}").format(
                table=sql.Identifier(pg_table),
                column=sql.Identifier(pg_column),
                coltype=sql.SQL(pg_type),
            )
        )
    conn.commit()


def promote_column_to_jsonb(conn, pg_table: str, pg_column: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "ALTER TABLE {table} ALTER COLUMN {column} TYPE jsonb USING to_jsonb({column})"
            ).format(
                table=sql.Identifier(pg_table),
                column=sql.Identifier(pg_column),
            )
        )
    conn.commit()


# ============================================================================
# JUNCTION TABLE FUNCTIONS
# ============================================================================

def get_junction_table_name(source_table: str, field_name: str) -> str:
    """Generate junction table name: {source_table}_{field_name}"""
    base = f"{source_table}_{field_name}"
    return sanitize_identifier(base, "junction")


def ensure_junction_table(conn, source_table: str, field_name: str, target_table: str) -> str:
    """
    Create junction table for array FK field if it doesn't exist.
    Returns the junction table name.
    """
    junction_table = get_junction_table_name(source_table, field_name)
    source_fk_col = f"{source_table}_id"
    target_fk_col = f"{target_table}_id"
    
    with conn.cursor() as cur:
        # Create junction table with composite primary key
        cur.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {source_col} TEXT NOT NULL,
                    {target_col} TEXT NOT NULL,
                    PRIMARY KEY ({source_col}, {target_col})
                )
            """).format(
                table=sql.Identifier(junction_table),
                source_col=sql.Identifier(source_fk_col),
                target_col=sql.Identifier(target_fk_col),
            )
        )
        # Create indexes for efficient lookups
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {table} ({col})").format(
                idx=sql.Identifier(f"idx_{junction_table}_{source_fk_col}"),
                table=sql.Identifier(junction_table),
                col=sql.Identifier(source_fk_col),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {table} ({col})").format(
                idx=sql.Identifier(f"idx_{junction_table}_{target_fk_col}"),
                table=sql.Identifier(junction_table),
                col=sql.Identifier(target_fk_col),
            )
        )
    conn.commit()
    logging.info("Ensured junction table %s for %s.%s -> %s", 
                 junction_table, source_table, field_name, target_table)
    return junction_table


def sync_junction_table_data(
    conn,
    junction_table: str,
    source_table: str,
    target_table: str,
    source_id: str,
    target_ids: List[str]
) -> None:
    """
    Sync array data to junction table.
    Deletes existing entries for source_id and inserts new ones.
    """
    source_fk_col = f"{source_table}_id"
    target_fk_col = f"{target_table}_id"
    
    with conn.cursor() as cur:
        # Delete existing entries for this source
        cur.execute(
            sql.SQL("DELETE FROM {table} WHERE {col} = %s").format(
                table=sql.Identifier(junction_table),
                col=sql.Identifier(source_fk_col),
            ),
            (source_id,)
        )
        
        # Insert new entries
        if target_ids:
            values = [(source_id, tid) for tid in target_ids if tid]
            if values:
                insert_sql = sql.SQL(
                    "INSERT INTO {table} ({source_col}, {target_col}) VALUES %s ON CONFLICT DO NOTHING"
                ).format(
                    table=sql.Identifier(junction_table),
                    source_col=sql.Identifier(source_fk_col),
                    target_col=sql.Identifier(target_fk_col),
                )
                extras.execute_values(cur, insert_sql.as_string(conn), values)
    conn.commit()


def process_junction_fields_for_doc(conn, collection_name: str, pg_table: str, doc: Dict[str, Any]) -> None:
    """
    Process all junction table fields for a document.
    Extracts array values and syncs to junction tables.
    """
    junction_fields = JUNCTION_TABLE_FIELDS.get(collection_name, {})
    if not junction_fields:
        return
    
    source_id = str(doc.get("_id"))
    if not source_id:
        return
    
    for field_name, target_collection in junction_fields.items():
        array_value = doc.get(field_name)
        if array_value is None:
            array_value = []
        elif not isinstance(array_value, list):
            array_value = [array_value]  # Convert single value to array
        
        # Extract ObjectIds from array
        target_ids = []
        for item in array_value:
            extracted = extract_objectid(item)
            if extracted:
                target_ids.append(extracted)
        
        # Get target table name from registry
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_table_name FROM mongo_collection_registry WHERE collection_name = %s",
                (target_collection,)
            )
            row = cur.fetchone()
            target_table = row[0] if row else target_collection
        
        # Ensure junction table exists and sync data
        junction_table = ensure_junction_table(conn, pg_table, field_name, target_table)
        sync_junction_table_data(conn, junction_table, pg_table, target_table, source_id, target_ids)

# ============================================================================


def refresh_state(conn, state: CollectionState) -> CollectionState:
    columns = load_columns(conn, state.collection_name)
    column_order = sorted(columns.values(), key=lambda c: c.pg_column)
    upsert_sql = build_upsert_sql(conn, state.pg_table, column_order)
    return CollectionState(
        collection_name=state.collection_name,
        pg_table=state.pg_table,
        columns=columns,
        column_order=column_order,
        upsert_sql=upsert_sql,
    )


def build_upsert_sql(conn, pg_table: str, column_order: List[ColumnInfo]) -> str:
    cols = ["_id"] + [c.pg_column for c in column_order]
    insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    if len(cols) > 1:
        updates = sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in cols[1:]
        )
        query = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT (_id) DO UPDATE SET {updates}"
        ).format(table=sql.Identifier(pg_table), cols=insert_cols, updates=updates)
    else:
        query = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT (_id) DO NOTHING"
        ).format(table=sql.Identifier(pg_table), cols=insert_cols)
    return query.as_string(conn)


def ensure_columns_for_document(conn, state: CollectionState, doc: Dict[str, Any]) -> CollectionState:
    changed = False
    collection_name = state.collection_name
    for key, value in doc.items():
        if key == "_id":
            continue
        
        # Skip junction table fields - they don't get columns in the main table
        if is_junction_table_field(collection_name, key):
            continue
            
        col = state.columns.get(key)
        if col is None:
            # Use context-aware type inference
            pg_type = infer_pg_type_with_context(value, collection_name, key)
            if pg_type is None:
                continue  # Skip this field (junction table field)
            pg_column = get_or_create_column_name(conn, collection_name, key)
            add_column(conn, state.pg_table, pg_column, pg_type)
            update_column_type(conn, collection_name, key, pg_type)
            changed = True
        else:
            # Use context-aware type compatibility
            if not is_type_compatible_with_context(value, col.pg_type, collection_name, key):
                promote_column_to_jsonb(conn, state.pg_table, col.pg_column)
                update_column_type(conn, collection_name, key, "jsonb")
                changed = True
    if changed:
        return refresh_state(conn, state)
    return state


def doc_to_row(doc: Dict[str, Any], state: CollectionState) -> List[Any]:
    if "_id" not in doc:
        raise ValueError("document missing _id")
    collection_name = state.collection_name
    values = [str(doc["_id"])]
    for col in state.column_order:
        value = doc.get(col.mongo_key)
        # Use context-aware value adaptation for FK extraction
        values.append(adapt_value_with_context(value, col.pg_type, collection_name, col.mongo_key))
    return values



def upsert_batch(
    conn,
    state: CollectionState,
    docs: List[Dict[str, Any]],
    settings: SyncSettings,
) -> None:
    if not docs:
        return
    use_copy = settings.copy_enabled and len(docs) >= settings.copy_min_rows
    if use_copy:
        try:
            copy_upsert_batch(conn, state, docs)
            # Process junction table fields for each doc
            for doc in docs:
                process_junction_fields_for_doc(conn, state.collection_name, state.pg_table, doc)
            return
        except Exception:
            logging.exception(
                "COPY bulk load failed for %s; falling back to INSERT",
                state.collection_name,
            )
            conn.rollback()
    rows = [doc_to_row(item, state) for item in docs]
    with conn.cursor() as cur:
        extras.execute_values(cur, state.upsert_sql, rows, page_size=len(rows))
    conn.commit()
    
    # Process junction table fields for each doc
    for doc in docs:
        process_junction_fields_for_doc(conn, state.collection_name, state.pg_table, doc)



def delete_row(conn, pg_table: str, _id: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {table} WHERE _id = %s").format(table=sql.Identifier(pg_table)),
            (str(_id),),
        )
    conn.commit()


def load_resume_token(conn, scope: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT token FROM mongo_resume_tokens WHERE scope = %s", (scope,))
        row = cur.fetchone()
    if not row:
        return None
    token_bytes = row[0]
    try:
        return BSON(token_bytes).decode()
    except Exception:
        return None


def save_resume_token(conn, scope: str, token: Dict[str, Any]) -> None:
    token_bytes = BSON.encode(token)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mongo_resume_tokens (scope, token)
            VALUES (%s, %s)
            ON CONFLICT (scope) DO UPDATE
              SET token = EXCLUDED.token, updated_at = now()
            """,
            (scope, psycopg2.Binary(token_bytes)),
        )
    conn.commit()


def prepare_collection_state(conn, collection_name: str) -> CollectionState:
    pg_table = get_or_create_table_name(conn, collection_name)
    ensure_table(conn, pg_table)
    columns = load_columns(conn, collection_name)
    column_order = sorted(columns.values(), key=lambda c: c.pg_column)
    upsert_sql = build_upsert_sql(conn, pg_table, column_order)
    return CollectionState(
        collection_name=collection_name,
        pg_table=pg_table,
        columns=columns,
        column_order=column_order,
        upsert_sql=upsert_sql,
    )


def backfill_collection(
    conn,
    mongo_collection,
    state: CollectionState,
    batch_size: int,
    settings: SyncSettings,
) -> CollectionState:
    logging.info("Backfilling collection %s", state.collection_name)
    batch_docs: List[Dict[str, Any]] = []
    for doc in mongo_collection.find({}):
        state = ensure_columns_for_document(conn, state, doc)
        batch_docs.append(doc)
        if len(batch_docs) >= batch_size:
            upsert_batch(conn, state, batch_docs, settings)
            batch_docs.clear()
    if batch_docs:
        upsert_batch(conn, state, batch_docs, settings)
    return state


def process_change(
    conn,
    change: Dict[str, Any],
    states: Dict[str, CollectionState],
    settings: SyncSettings,
) -> None:
    ns = change.get("ns") or {}
    coll = ns.get("coll")
    if not coll:
        return
    state = states.get(coll)
    if state is None:
        state = prepare_collection_state(conn, coll)
        states[coll] = state

    op = change.get("operationType")
    if op in {"insert", "replace", "update"}:
        doc = change.get("fullDocument")
        if not doc:
            return
        state = ensure_columns_for_document(conn, state, doc)
        states[coll] = state
        upsert_batch(conn, state, [doc], settings)
    elif op == "delete":
        doc_key = change.get("documentKey", {})
        _id = doc_key.get("_id")
        if _id is None:
            return
        delete_row(conn, state.pg_table, _id)
    else:
        # drop, rename, invalidate, etc.
        logging.info("Skipping op type %s for collection %s", op, coll)


def watch_changes(
    conn,
    mongo_db,
    collections: Optional[List[str]],
    settings: SyncSettings,
) -> None:
    scope = "db:{}:{}".format(
        mongo_db.name,
        "all" if not collections else short_hash(",".join(sorted(collections))),
    )
    resume_token = load_resume_token(conn, scope)
    pipeline = []
    if collections:
        pipeline = [{"$match": {"ns.coll": {"$in": collections}}}]

    states: Dict[str, CollectionState] = {}

    while True:
        try:
            watch_kwargs = {
                "pipeline": pipeline,
                "full_document": "updateLookup",
            }
            if resume_token:
                watch_kwargs["resume_after"] = resume_token
            with mongo_db.watch(**watch_kwargs) as stream:
                for change in stream:
                    process_change(conn, change, states, settings)
                    resume_token = change.get("_id")
                    if resume_token:
                        save_resume_token(conn, scope, resume_token)
        except PyMongoError as exc:
            logging.exception("Mongo watch error: %s", exc)
            resume_token = None
            time.sleep(2)
        except psycopg2.Error as exc:
            logging.exception("Postgres error during watch: %s", exc)
            time.sleep(2)


def parse_collections(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    items = [c.strip() for c in raw.split(",")]
    items = [c for c in items if c]
    return items or None


def filter_collections(
    collections: List[str], exclude: Optional[List[str]]
) -> List[str]:
    if not exclude:
        return collections
    exclude_set = {c for c in exclude if c}
    if not exclude_set:
        return collections
    filtered = [c for c in collections if c not in exclude_set]
    skipped = [c for c in collections if c in exclude_set]
    if skipped:
        logging.info("Excluding collections: %s", ", ".join(skipped))
    return filtered


def main() -> int:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    mongo_uri = os.getenv("MONGO_DETAILS") or os.getenv("MONGO_URI") or "mongodb://localhost:27017"
    mongo_db_name = os.getenv("DB_NAME") or os.getenv("MONGO_DB")
    if not mongo_db_name:
        logging.error("DB_NAME (or MONGO_DB) is required")
        return 1

    collections = parse_collections(os.getenv("COLLECTIONS"))
    exclude_collections = parse_collections(os.getenv("EXCLUDE_COLLECTIONS"))
    backfill_enabled = env_flag("BACKFILL", True)
    watch_enabled = env_flag("WATCH", True)
    batch_size = int(os.getenv("BATCH_SIZE", "500"))
    copy_enabled = env_flag("COPY_ENABLED", True)
    copy_min_rows = int(os.getenv("COPY_MIN_ROWS", "200"))
    if copy_min_rows < 1:
        copy_min_rows = 1
    settings = SyncSettings(copy_enabled=copy_enabled, copy_min_rows=copy_min_rows)

    pg_dsn = build_pg_dsn()

    try:
        pg_conn = psycopg2.connect(pg_dsn)
    except psycopg2.Error as exc:
        logging.exception("Failed to connect to Postgres: %s", exc)
        return 1

    try:
        mongo_client = MongoClient(mongo_uri)
        mongo_db = mongo_client[mongo_db_name]
    except PyMongoError as exc:
        logging.exception("Failed to connect to MongoDB: %s", exc)
        return 1

    ensure_registry_tables(pg_conn)

    if collections is None:
        collections = mongo_db.list_collection_names()
    collections = filter_collections(collections, exclude_collections)
    if not collections:
        logging.warning("No collections to sync after applying exclusions.")
        return 0

    states: Dict[str, CollectionState] = {}
    for coll in collections:
        state = prepare_collection_state(pg_conn, coll)
        states[coll] = state
        if backfill_enabled:
            state = backfill_collection(pg_conn, mongo_db[coll], state, batch_size, settings)
            states[coll] = state

    if watch_enabled:
        watch_changes(pg_conn, mongo_db, collections, settings)

    return 0


if __name__ == "__main__":
    sys.exit(main())
