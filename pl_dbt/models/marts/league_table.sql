select
    season,
    team_id,
    team_name,
    count(*)                                             as played,
    count(*) filter (where result = 'W')                 as wins,
    count(*) filter (where result = 'D')                 as draws,
    count(*) filter (where result = 'L')                 as losses,
    cast(sum(goals_for) as integer)                      as goals_for,
    cast(sum(goals_against) as integer)                  as goals_against,
    cast(sum(goals_for) - sum(goals_against) as integer) as goal_difference,
    cast(sum(points) as integer)                         as points,
    rank() over (
        partition by season
        order by sum(points) desc,
                 sum(goals_for) - sum(goals_against) desc,
                 sum(goals_for) desc
    )                                                    as position
from {{ ref('fact_team_matches') }}
group by season, team_id, team_name
