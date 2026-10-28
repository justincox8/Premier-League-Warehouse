with source as (

    select * from {{ source('football_data', 'pl_matches') }}

),

latest as (

    -- raw is append-only, so keep only the newest version of each match
    select *
    from source
    qualify row_number() over (
        partition by id
        order by ingestion_timestamp desc, _dlt_load_id desc
    ) = 1

)

select
    id                       as match_id,
    season,
    matchday,
    stage,
    status,
    utc_date                 as kickoff_utc,

    home_team__id            as home_team_id,
    home_team__name          as home_team_name,
    away_team__id            as away_team_id,
    away_team__name          as away_team_name,

    score__winner            as winner,
    score__half_time__home   as home_goals_ht,
    score__half_time__away   as away_goals_ht,
    score__full_time__home   as home_goals_ft,
    score__full_time__away   as away_goals_ft,

    last_updated             as source_updated_at,
    ingestion_timestamp      as ingested_at

from latest
