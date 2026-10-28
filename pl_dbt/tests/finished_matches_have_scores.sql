select * from {{ ref('stg_pl_matches') }}
where status = 'FINISHED'
  and (home_goals_ft is null or away_goals_ft is null)
