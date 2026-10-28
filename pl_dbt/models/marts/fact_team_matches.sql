with finished as (

    select * from {{ ref('stg_pl_matches') }}
    where status = 'FINISHED'

),

home_side as (

    select
        match_id, season, matchday, kickoff_utc,
        home_team_id   as team_id,
        home_team_name as team_name,
        away_team_id   as opponent_id,
        'home'         as venue,
        home_goals_ft  as goals_for,
        away_goals_ft  as goals_against
    from finished

),

away_side as (

    select
        match_id, season, matchday, kickoff_utc,
        away_team_id   as team_id,
        away_team_name as team_name,
        home_team_id   as opponent_id,
        'away'         as venue,
        away_goals_ft  as goals_for,
        home_goals_ft  as goals_against
    from finished

),

unioned as (

    select * from home_side
    union all
    select * from away_side

)

select
    cast(match_id as varchar) || '-' || cast(team_id as varchar) as team_match_id,
    *,
    case
        when goals_for > goals_against then 'W'
        when goals_for = goals_against then 'D'
        else 'L'
    end as result,
    case
        when goals_for > goals_against then 3
        when goals_for = goals_against then 1
        else 0
    end as points
from unioned
