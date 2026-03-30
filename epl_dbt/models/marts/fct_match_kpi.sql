-- fct_match_kpi.sql
-- Answers: Home vs away advantage, avg goals per match, trends over time
 
with matches as (
    select * from {{ ref('stg_matches') }}
),
 
match_level as (
    select
        season,
        season_start_year,
        match_date,
        home_team,
        away_team,
        home_goals,
        away_goals,
        total_goals,
        result,
        home_win,
        away_win,
        draw,
        ht_home_goals,
        ht_away_goals,
        ht_result,
        home_shots,
        away_shots,
        home_shots_on_target,
        away_shots_on_target,
        home_corners,
        away_corners,
 
        -- second half goals
        (home_goals - ht_home_goals)        as home_sh_goals,
        (away_goals - ht_away_goals)        as away_sh_goals,
 
        -- comeback flag (losing at HT but won FT)
        case
            when ht_result = 'A' and result = 'H' then true
            when ht_result = 'H' and result = 'A' then true
            else false
        end                                 as comeback_win,
 
        -- high scoring match (5+ goals)
        case when total_goals >= 5
            then true else false
        end                                 as high_scoring
 
    from matches
),
 
season_kpi as (
    select
        season,
        season_start_year,
 
        -- volume
        count(*)                                        as total_matches,
        sum(total_goals)                                as total_goals,
 
        -- avg goals
        round(avg(total_goals), 2)                      as avg_goals_per_match,
        round(avg(home_goals), 2)                       as avg_home_goals,
        round(avg(away_goals), 2)                       as avg_away_goals,
        round(avg(ht_home_goals + ht_away_goals), 2)    as avg_ht_goals,
 
        -- home vs away advantage
        sum(home_win)                                   as home_wins,
        sum(away_win)                                   as away_wins,
        sum(draw)                                       as draws,
        round(sum(home_win) / count(*) * 100, 1)        as home_win_pct,
        round(sum(away_win) / count(*) * 100, 1)        as away_win_pct,
        round(sum(draw) / count(*) * 100, 1)            as draw_pct,
 
        -- home advantage index (>1 means home teams win more)
        round(
            sum(home_win) / nullif(sum(away_win), 0), 2
        )                                               as home_advantage_index,
 
        -- shots
        round(avg(home_shots), 1)                       as avg_home_shots,
        round(avg(away_shots), 1)                       as avg_away_shots,
        round(avg(home_shots_on_target), 1)             as avg_home_sot,
        round(avg(away_shots_on_target), 1)             as avg_away_sot,
 
        -- corners
        round(avg(home_corners), 1)                     as avg_home_corners,
        round(avg(away_corners), 1)                     as avg_away_corners,
 
        -- entertaining matches
        sum(case when high_scoring then 1 else 0 end)   as high_scoring_matches,
        round(
            sum(case when high_scoring then 1 else 0 end) / count(*) * 100, 1
        )                                               as high_scoring_pct,
 
        -- comebacks
        sum(case when comeback_win then 1 else 0 end)   as comeback_wins
 
    from match_level
    group by 1, 2
)
 
select * from season_kpi
order by season_start_year