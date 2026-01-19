import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import typer
from dotenv import load_dotenv

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
MOTHERDUCK_DB_NAME = os.getenv("MOTHERDUCK_DB_NAME")
MOTHERDUCK_TABLE_NAME = os.getenv("MOTHERDUCK_TABLE_NAME")

BACKUP_NAME_RE = re.compile(r"^airgradient_full_\d{8}T\d{6}Z\.parquet$")
DEFAULT_MIN_NEW_ROWS = 2000
DEFAULT_NULL_CHECK_COLUMNS = (
    "locationId",
    "timestamp",
    "serialno",
    "updated_at",
)

app = typer.Typer()


def require_table_name() -> str:
    if not MOTHERDUCK_TABLE_NAME:
        raise ValueError("MOTHERDUCK_TABLE_NAME must be set in the .env file.")
    return MOTHERDUCK_TABLE_NAME


def connect_motherduck() -> duckdb.DuckDBPyConnection:
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN must be set in the .env file.")
    db_string = f"md:{MOTHERDUCK_DB_NAME}" if MOTHERDUCK_DB_NAME else "md:"
    return duckdb.connect(f"{db_string}?motherduck_token={MOTHERDUCK_TOKEN}")


def s3_endpoint_url() -> str | None:
    endpoint_url = os.getenv("R2_ENDPOINT_URL")
    account_id = os.getenv("R2_ACCOUNT_ID")
    if endpoint_url:
        return endpoint_url
    if account_id:
        return f"https://{account_id}.r2.cloudflarestorage.com"
    return None


def configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    access_key = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("R2_REGION") or os.getenv("AWS_DEFAULT_REGION") or "auto"

    if not access_key or not secret_key:
        raise ValueError(
            "R2/AWS credentials not set. Provide R2_ACCESS_KEY_ID and "
            "R2_SECRET_ACCESS_KEY (or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)."
        )

    try:
        con.execute("INSTALL httpfs")
    except duckdb.Error:
        pass
    con.execute("LOAD httpfs")
    con.execute("SET s3_access_key_id = ?", [access_key])
    con.execute("SET s3_secret_access_key = ?", [secret_key])
    if session_token:
        con.execute("SET s3_session_token = ?", [session_token])
    con.execute("SET s3_region = ?", [region])
    endpoint = s3_endpoint_url()
    if endpoint:
        parsed = urlparse(endpoint)
        endpoint_host = parsed.netloc if parsed.scheme else endpoint
        con.execute("SET s3_endpoint = ?", [endpoint_host])
        con.execute("SET s3_url_style = 'path'")


def s3_env() -> dict[str, str]:
    access_key = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("R2_REGION") or os.getenv("AWS_DEFAULT_REGION") or "auto"
    env = dict(os.environ)
    env["AWS_ACCESS_KEY_ID"] = access_key or ""
    env["AWS_SECRET_ACCESS_KEY"] = secret_key or ""
    env["AWS_DEFAULT_REGION"] = region
    if session_token:
        env["AWS_SESSION_TOKEN"] = session_token
    return env


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def parse_s3_root(root: str) -> tuple[str, str]:
    parsed = urlparse(root)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid s3 path: {root}")
    bucket: str = parsed.netloc
    prefix: str = parsed.path.lstrip("/")
    return bucket, prefix


def backup_filename(timestamp: datetime) -> str:
    ts: str = timestamp.strftime("%Y%m%dT%H%M%SZ")
    return f"airgradient_full_{ts}.parquet"


def backup_path(root: str, filename: str) -> str:
    normalized = root.rstrip("/")
    return f"{normalized}/{filename}"


def ensure_local_root(root: str) -> Path:
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_local_backups(root: str) -> list[Path]:
    root_path = Path(root)
    if not root_path.exists():
        return []
    backups: list[Path] = [
        p
        for p in root_path.glob("airgradient_full_*.parquet")
        if BACKUP_NAME_RE.match(p.name)
    ]
    return sorted(backups, key=lambda p: p.name)


def cleanup_local_backups(root: str, retain: int) -> None:
    if retain < 1:
        return
    backups = list_local_backups(root)
    for path in backups[:-retain]:
        path.unlink()


def list_s3_backups(root: str) -> tuple[str, list[str]]:
    bucket, prefix = parse_s3_root(root)
    base = f"s3://{bucket}/{prefix}/" if prefix else f"s3://{bucket}/"
    cmd = ["aws", "s3", "ls", base]
    endpoint = s3_endpoint_url()
    if endpoint:
        cmd.extend(["--endpoint-url", endpoint])
    result = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        env=s3_env(),
    )
    keys: list[str] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        key = parts[3]
        if BACKUP_NAME_RE.match(key):
            full_key = f"{prefix.rstrip('/')}/{key}" if prefix else key
            keys.append(full_key)
    keys.sort()
    return bucket, keys


def cleanup_s3_backups(root: str, retain: int) -> None:
    if retain < 1:
        return
    bucket, keys = list_s3_backups(root)
    if len(keys) <= retain:
        return
    endpoint = s3_endpoint_url()
    for key in keys[:-retain]:
        uri = f"s3://{bucket}/{key}"
        cmd = ["aws", "s3", "rm", uri, "--only-show-errors"]
        if endpoint:
            cmd.extend(["--endpoint-url", endpoint])
        subprocess.run(cmd, check=True, env=s3_env())


def list_backup_uris(root: str) -> list[str]:
    if is_s3_path(root):
        bucket, keys = list_s3_backups(root)
        return [f"s3://{bucket}/{key}" for key in keys]
    return [str(path) for path in list_local_backups(root)]


def latest_backups(root: str) -> tuple[str, str | None]:
    backups = list_backup_uris(root)
    if not backups:
        raise ValueError(f"No backups found at {root}")
    latest = backups[-1]
    previous = backups[-2] if len(backups) > 1 else None
    return latest, previous


def quoted_columns(columns: list[str]) -> str:
    return ", ".join([f'"{col}"' for col in columns])


def parquet_columns(con: duckdb.DuckDBPyConnection, parquet_path: str) -> list[str]:
    escaped = parquet_path.replace("'", "''")
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
    ).fetchall()
    return [row[0] for row in rows]


def parquet_stats(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    null_check_columns: list[str],
) -> tuple[int, dict[str, int]]:
    escaped = parquet_path.replace("'", "''")
    select_parts = ["COUNT(*) AS row_count"]
    for col in null_check_columns:
        select_parts.append(
            f"SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) AS \"{col}_nulls\""
        )
    select_sql = ",\n            ".join(select_parts)
    query = f"""
        SELECT {select_sql}
        FROM read_parquet('{escaped}')
    """
    row = con.execute(query).fetchone()
    if row is None:
        raise ValueError(f"No rows returned while reading {parquet_path}")
    row_count = int(row[0])
    nulls: dict[str, int] = {}
    for idx, col in enumerate(null_check_columns, start=1):
        nulls[col] = int(row[idx] or 0)
    return row_count, nulls


@app.command()
def backup(
    parquet_root: str = typer.Option(
        "data/backups",
        "--parquet-root",
        help="Destination root for the backup (local path or s3://...).",
    ),
    retain: int = typer.Option(
        4,
        "--retain",
        help="Number of backups to keep (older ones are deleted).",
    ),
) -> None:
    table_name = require_table_name()
    con = connect_motherduck()
    if is_s3_path(parquet_root):
        configure_s3(con)
    else:
        ensure_local_root(parquet_root)

    timestamp: datetime = datetime.now(timezone.utc)
    filename: str = backup_filename(timestamp)
    output_path: str = backup_path(parquet_root, filename)
    escaped_path: str = output_path.replace("'", "''")
    query = f"""
        COPY (SELECT * FROM {table_name})
        TO '{escaped_path}'
        (FORMAT 'parquet', CODEC 'ZSTD')
    """
    con.execute(query)
    print(f"Wrote backup to {output_path}")

    if is_s3_path(parquet_root):
        cleanup_s3_backups(parquet_root, retain)
    else:
        cleanup_local_backups(parquet_root, retain)


@app.command()
def validate(
    parquet_root: str = typer.Option(
        "data/backups",
        "--parquet-root",
        help="Backup root for validation (local path or s3://...).",
    ),
    min_new_rows: int = typer.Option(
        DEFAULT_MIN_NEW_ROWS,
        "--min-new-rows",
        help="Minimum expected new rows compared to the previous backup.",
    ),
    null_check_columns: list[str] = typer.Option(
        list(DEFAULT_NULL_CHECK_COLUMNS),
        "--null-check-column",
        help="Columns that must not contain NULL values. Repeatable.",
    ),
) -> None:
    latest, previous = latest_backups(parquet_root)
    con = duckdb.connect()
    if is_s3_path(parquet_root):
        configure_s3(con)

    parquet_cols = parquet_columns(con, latest)
    missing = [col for col in null_check_columns if col not in parquet_cols]
    if missing:
        raise ValueError(f"Missing columns in latest backup: {missing}")

    latest_count, latest_nulls = parquet_stats(con, latest, null_check_columns)
    print(f"Latest backup: {latest}")
    print(f"Row count: {latest_count}")

    if previous:
        previous_count, _ = parquet_stats(con, previous, [])
        print(f"Previous backup: {previous}")
        print(f"Previous row count: {previous_count}")
        if latest_count < previous_count + min_new_rows:
            raise ValueError(
                f"Row count {latest_count} is less than "
                f"{previous_count} + {min_new_rows}"
            )

    null_failures = {col: count for col, count in latest_nulls.items() if count > 0}
    if null_failures:
        details = ", ".join([f"{col}={count}" for col, count in null_failures.items()])
        raise ValueError(f"NULL checks failed: {details}")

    print("Validation passed.")


if __name__ == "__main__":
    app()
