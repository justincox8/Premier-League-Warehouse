import duckdb

P="raw/football_data/pl_matches/*.parquet"

print(duckdb.sql(f"""
    WITH latest AS (
        SELECT * FROM read_parquet('{P}')
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingestion_timestamp DESC) = 1
    )
    SELECT
        season,
        count(*)                                           AS matches,
        count(*) FILTER (status = 'FINISHED')              AS finished,
        count(*) FILTER (status IN ('SCHEDULED', 'TIMED')) AS upcoming,
        max(utc_date) FILTER (status = 'FINISHED')         AS last_played
    FROM latest
    GROUP BY season ORDER BY season
"""))

print(duckdb.sql(f"""
    SELECT count(*) AS snapshots, count(DISTINCT ingestion_timestamp) AS runs
    FROM read_parquet('{P}')
"""))