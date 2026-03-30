-- fct_league_table.sql
-- Answers: Points per team, rank per season, win rate, goals scored/conceded
 
with team_stats as (
    select * from {{ ref('int_team_stats') }}
),
 
totals as (
    select
        season,
        season_start_year,
        team,
 
        -- matches
        sum(matches_played)                             as matches_played,
        sum(wins)                                       as wins,
        sum(draws)                                      as draws,
        sum(losses)                                     as losses,
 
        -- goals
        sum(goals_scored)                               as goals_scored,
        sum(goals_conceded)                             as goals_conceded,
        sum(goal_difference)                            as goal_difference,
 
        -- points
        sum(points)                                     as points,
 
        -- rates
        round(sum(wins) / nullif(sum(matches_played), 0) * 100, 1)
                                                        as win_rate_pct,
        round(sum(draws) / nullif(sum(matches_played), 0) * 100, 1)
                                                        as draw_rate_pct,
        round(sum(losses) / nullif(sum(matches_played), 0) * 100, 1)
                                                        as loss_rate_pct,
 
        -- averages
        round(sum(goals_scored) / nullif(sum(matches_played), 0), 2)
                                                        as avg_goals_scored,
        round(sum(goals_conceded) / nullif(sum(matches_played), 0), 2)
                                                        as avg_goals_conceded,
 
        -- discipline
        sum(yellow_cards)                               as yellow_cards,
        sum(red_cards)                                  as red_cards,
 
        -- shots
        sum(shots)                                      as shots,
        sum(shots_on_target)                            as shots_on_target,
        round(sum(shots_on_target) / nullif(sum(shots), 0) * 100, 1)
                                                        as shot_accuracy_pct
 
    from team_stats
    group by 1, 2, 3
),
 
ranked as (
    select
        *,
        -- rank within each season by points, then goal diff, then goals scored
        row_number() over (
            partition by season
            order by points desc, goal_difference desc, goals_scored desc
        )                                               as season_rank,
 
        -- is this team in top 4 (Champions League)?
        case when row_number() over (
            partition by season
            order by points desc, goal_difference desc, goals_scored desc
        ) <= 4 then true else false end                 as top_4,
 
        -- is this team in bottom 3 (relegated)?
        case when row_number() over (
            partition by season
            order by points desc, goal_difference desc, goals_scored desc
        ) >= 18 then true else false end                as relegated
 
    from totals
)
 
select * from ranked
order by season, season_rank