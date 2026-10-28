select season
from {{ ref('league_table') }}
group by season
having sum(goals_for) != sum(goals_against)
