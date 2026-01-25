# scripts/check_neon.py
import os
from dotenv import load_dotenv
import psycopg

def main():
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set. Add it to .env in the repo root.")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select schemaname, tablename
                from pg_catalog.pg_tables
                where schemaname = 'm2mr'
                order by tablename;
                """
            )
            rows = cur.fetchall()

    print("Connected.")
    if not rows:
        print("No tables found in schema m2mr.")
    else:
        print("Tables in schema m2mr:")
        for schema, table in rows:
            print(f"- {schema}.{table}")

if __name__ == "__main__":
    main()
