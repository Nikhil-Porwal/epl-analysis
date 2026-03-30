-- int_team_stats.sql
-- Aggregates match-level data into per-team per-season stats
-- Each match contributes TWO rows (home team + away team)
 
with matches as (
    select * from {{ ref('stg_matches') }}
),
 
home_stats as (
    select
        season,
        season_start_year,
        home_team                           as team,
        'home'                              as venue,
        count(*)                            as matches_played,
        sum(home_win)                       as wins,
        sum(draw)                           as draws,
        sum(away_win)                       as losses,
        sum(home_goals)                     as goals_scored,
        sum(away_goals)                     as goals_conceded,
        sum(home_goals) - sum(away_goals)   as goal_difference,
        sum(home_points)                    as points,
        sum(home_shots)                     as shots,
        sum(home_shots_on_target)           as shots_on_target,
        sum(home_yellow_cards)              as yellow_cards,
        sum(home_red_cards)                 as red_cards,
        sum(home_corners)                   as corners,
        sum(home_fouls)                     as fouls
    from matches
    group by 1, 2, 3, 4
),
 
away_stats as (
    select
        season,
        season_start_year,
        away_team                           as team,
        'away'                              as venue,
        count(*)                            as matches_played,
        sum(away_win)                       as wins,
        sum(draw)                           as draws,
        sum(home_win)                       as losses,
        sum(away_goals)                     as goals_scored,
        sum(home_goals)                     as goals_conceded,
        sum(away_goals) - sum(home_goals)   as goal_difference,
        sum(away_points)                    as points,
        sum(away_shots)                     as shots,
        sum(away_shots_on_target)           as shots_on_target,
        sum(away_yellow_cards)              as yellow_cards,
        sum(away_red_cards)                 as red_cards,
        sum(away_corners)                   as corners,
        sum(away_fouls)                     as fouls
    from matches
    group by 1, 2, 3, 4
),
 
combined as (
    select * from home_stats
    union all
    select * from away_stats
)
 
select * from combined