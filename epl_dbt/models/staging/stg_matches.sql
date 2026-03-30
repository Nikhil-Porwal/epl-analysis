-- stg_matches.sql
-- Cleans and standardizes RAW.MATCHES for downstream models
 
with source as (
    select * from {{ source('epl_raw', 'matches') }}
),
 
cleaned as (
    select
        -- identifiers
        season,
        season_start_year,
        try_to_date(match_date)         as match_date,
 
        -- teams
        trim(home_team)                 as home_team,
        trim(away_team)                 as away_team,
 
        -- goals
        home_goals,
        away_goals,
        total_goals,
 
        -- results
        result,                          -- H / A / D
        home_win,
        away_win,
        draw,
 
        -- points
        home_points,
        away_points,
 
        -- half time
        ht_home_goals,
        ht_away_goals,
        ht_result,
 
        -- shots
        home_shots,
        away_shots,
        home_shots_on_target,
        away_shots_on_target,
 
        -- discipline
        home_yellow_cards,
        away_yellow_cards,
        home_red_cards,
        away_red_cards,
 
        -- set pieces
        home_corners,
        away_corners,
        home_fouls,
        away_fouls,
 
        -- metadata
        ingested_at
 
    from source
    where
        match_date is not null
        and home_team is not null
        and away_team is not null
        and home_goals is not null
        and away_goals is not null
)
 
select * from cleaned
