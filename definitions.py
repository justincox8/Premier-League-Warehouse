import os
from datetime import date
from pathlib import Path

import dagster as dg
import duckdb
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from ingest_matches import run_ingest

ROOT = Path(__file__).resolve().parent

# Absolute paths so nothing depends on where Dagster is launched from.
os.environ.setdefault("DUCKDB_PATH", str(ROOT / "pl_dbt" / "dev.duckdb"))
os.environ.setdefault("RAW_DIR", str(ROOT / "raw"))


def current_season() -> int:
    """Seasons are named by start year and begin in August."""
    today = date.today()
    return today.year if today.month >= 8 else today.year - 1


# ---- Asset 1: raw matches (the dlt pipeline) ----
class IngestConfig(dg.Config):
    seasons: list[int] | None = None  # None means the current season


@dg.asset(key=["football_data", "pl_matches"], group_name="raw")
def pl_matches_raw(context: dg.AssetExecutionContext, config: IngestConfig):
    seasons = config.seasons or [current_season()]
    context.log.info(f"Ingesting seasons {seasons}")
    info = run_ingest(seasons)
    info.raise_on_failed_jobs()
    return dg.MaterializeResult(metadata={"seasons": seasons})


# ---- Assets 2+: every dbt model (staging, marts) ----
dbt_project = DbtProject(project_dir=ROOT / "pl_dbt")
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def pl_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ---- Serving layer: publish the marts to Postgres ----
class PostgresServing(dg.ConfigurableResource):
    host: str = "127.0.0.1"
    port: int = 5432
    dbname: str
    user: str
    password: str

    def conn_string(self) -> str:
        return (
            f"dbname={self.dbname} user={self.user} "
            f"password={self.password} host={self.host} port={self.port}"
        )


SERVED_TABLES = ["league_table", "fact_team_matches"]


@dg.asset(
    group_name="serving",
    deps=[dg.AssetKey("league_table"), dg.AssetKey("fact_team_matches")],
)
def postgres_serving_layer(
    context: dg.AssetExecutionContext, postgres: PostgresServing
):
    con = duckdb.connect()  # in-memory scratch session
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{os.environ['DUCKDB_PATH']}' AS wh (READ_ONLY)")
    con.execute(f"ATTACH '{postgres.conn_string()}' AS pg (TYPE postgres)")
    con.execute("CALL postgres_execute('pg', 'CREATE SCHEMA IF NOT EXISTS serving')")

    row_counts = {}
    for table in SERVED_TABLES:
        con.execute("BEGIN")
        con.execute(f"DROP TABLE IF EXISTS pg.serving.{table}")
        con.execute(f"CREATE TABLE pg.serving.{table} AS SELECT * FROM wh.main.{table}")
        con.execute("COMMIT")

        src = con.execute(f"SELECT count(*) FROM wh.main.{table}").fetchone()[0]
        dst = con.execute(f"SELECT count(*) FROM pg.serving.{table}").fetchone()[0]
        if src != dst:
            raise ValueError(f"{table}: {src} rows in DuckDB but {dst} in Postgres")
        row_counts[table] = dst
        context.log.info(f"Published {table}: {dst} rows")

    return dg.MaterializeResult(metadata=row_counts)


# ---- Automation ----
refresh_job = dg.define_asset_job(
    "refresh_pl_warehouse", selection=dg.AssetSelection.all()
)

daily_refresh = dg.ScheduleDefinition(
    job=refresh_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/Los_Angeles",
)

# ---- Registration: must come LAST, after everything it refers to ----
defs = dg.Definitions(
    assets=[pl_matches_raw, pl_dbt_assets, postgres_serving_layer],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
        "postgres": PostgresServing(
            dbname=dg.EnvVar("POSTGRES_DB"),
            user=dg.EnvVar("POSTGRES_USER"),
            password=dg.EnvVar("POSTGRES_PASSWORD"),
        ),
    },
    jobs=[refresh_job],
    schedules=[daily_refresh],
)
