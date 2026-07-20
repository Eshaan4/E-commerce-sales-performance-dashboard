"""
init_pipeline.py
Verify that all PostgreSQL schemas were created by the DB init scripts.
This runs inside the pipeline-init Docker container after postgres is healthy.
If schemas are missing (e.g. re-run on existing volume), creates them.
"""
import os
import sys
import time
from pathlib import Path

# Ensure src/ is importable
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, str(Path(__file__).parent.parent))


def wait_for_postgres(max_retries: int = 30, delay: int = 3) -> bool:
    """Poll until PostgreSQL accepts connections."""
    import psycopg2
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db   = os.getenv("POSTGRES_DB",   "de_poc")
    user = os.getenv("POSTGRES_USER", "de_user")
    pwd  = os.getenv("POSTGRES_PASSWORD", "de_password123")

    print(f"\nWaiting for PostgreSQL at {host}:{port}/{db} …")
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname=db, user=user, password=pwd,
                connect_timeout=5,
            )
            conn.close()
            print(f"  ✔ PostgreSQL ready (attempt {attempt})")
            return True
        except Exception as exc:
            print(f"  [{attempt}/{max_retries}] Not ready: {exc}  – retrying in {delay}s …")
            time.sleep(delay)
    return False


def get_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB",   "de_poc"),
        user=os.getenv("POSTGRES_USER",   "de_user"),
        password=os.getenv("POSTGRES_PASSWORD", "de_password123"),
    )


def schemas_exist(conn) -> bool:
    """Return True if all 4 required schemas exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.schemata
            WHERE schema_name IN ('bronze','silver','gold','metadata')
        """)
        count = cur.fetchone()[0]
    return count == 4


def run_sql_file(path: Path, conn) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"  ✔ {path.name}")


def create_schemas(conn) -> None:
    """Run all DDL files to create schemas."""
    sql_dir   = Path(__file__).parent.parent / "sql"
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        print(f"  ⚠  No SQL files found in {sql_dir}")
        return
    print(f"\nRunning {len(sql_files)} DDL script(s):")
    for f in sql_files:
        try:
            run_sql_file(f, conn)
        except Exception as exc:
            conn.rollback()
            print(f"  ✗ {f.name} → {exc}")


def print_schema_summary(conn) -> None:
    """Print table counts per schema."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze','silver','gold','metadata')
            ORDER BY table_schema, table_name
        """)
        rows = cur.fetchall()

    by_schema: dict = {}
    for schema, table in rows:
        by_schema.setdefault(schema, []).append(table)

    print("\n  Schema summary:")
    for schema in sorted(by_schema):
        tables = by_schema[schema]
        print(f"    {schema:<12} → {len(tables)} table(s): {', '.join(tables)}")


def main():
    print("\n" + "═" * 60)
    print("  DE PoC – Pipeline Initialisation")
    print("═" * 60)

    if not wait_for_postgres():
        print("\n✗ Could not connect to PostgreSQL. Exiting.")
        sys.exit(1)

    conn = get_conn()
    conn.autocommit = False

    if schemas_exist(conn):
        print("\n  ✔ All schemas already exist (created by postgres init scripts).")
    else:
        print("\n  ⚠ Schemas missing – running DDL scripts …")
        create_schemas(conn)

    print_schema_summary(conn)
    conn.close()

    print("\n" + "═" * 60)
    print("  ✔ Initialisation complete!")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
