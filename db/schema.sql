CREATE DATABASE IF NOT EXISTS nfl_dw;
USE nfl_dw;

-- date dim
CREATE TABLE IF NOT EXISTS dim_date (
  date_key INT PRIMARY KEY,           -- YYYYMMDD e.g. 20240908
  date DATE NOT NULL,
  year SMALLINT,
  month TINYINT,
  day TINYINT,
  weekday TINYINT,
  is_weekend TINYINT,
  season VARCHAR(10),
  is_playoffs TINYINT
);

-- teams
CREATE TABLE IF NOT EXISTS dim_team (
  team_id INT PRIMARY KEY,
  team_name VARCHAR(100),
  city VARCHAR(100),
  abbreviation VARCHAR(10),
  conference VARCHAR(10),
  division VARCHAR(50),
  active TINYINT DEFAULT 1
);

-- players
CREATE TABLE IF NOT EXISTS dim_player (
  player_id INT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  full_name VARCHAR(200),
  position VARCHAR(10),
  birth_date DATE,
  height_cm INT,
  weight_kg INT,
  active TINYINT DEFAULT 1,
  current_team_id INT,
  INDEX (last_name),
  FOREIGN KEY (current_team_id) REFERENCES dim_team(team_id)
);

-- games
CREATE TABLE IF NOT EXISTS dim_game (
  game_id BIGINT PRIMARY KEY,
  season SMALLINT,
  week TINYINT,
  game_date DATE,
  home_team_id INT,
  away_team_id INT,
  home_score INT,
  away_score INT,
  status VARCHAR(50),
  venue VARCHAR(128),
  FOREIGN KEY (home_team_id) REFERENCES dim_team(team_id),
  FOREIGN KEY (away_team_id) REFERENCES dim_team(team_id),
  INDEX(season, game_date)
);

-- team game facts
CREATE TABLE IF NOT EXISTS fact_team_game (
  game_id BIGINT,
  team_id INT,
  opponent_team_id INT,
  date_key INT,
  total_yards INT,
  passing_yards INT,
  rushing_yards INT,
  points_scored INT,
  points_allowed INT,
  turnovers INT,
  time_of_possession_sec INT,
  win_flag TINYINT,
  PRIMARY KEY (game_id, team_id),
  INDEX(team_id, date_key)
);

-- player game facts
CREATE TABLE IF NOT EXISTS fact_player_game (
  game_id BIGINT,
  player_id INT,
  team_id INT,
  opponent_team_id INT,
  date_key INT,
  snaps INT,
  pass_attempts INT,
  pass_completions INT,
  pass_yards INT,
  pass_tds INT,
  rush_attempts INT,
  rush_yards INT,
  rush_tds INT,
  receptions INT,
  rec_yards INT,
  rec_tds INT,
  fumbles INT,
  fantasy_points FLOAT,
  PRIMARY KEY (game_id, player_id),
  INDEX(player_id, date_key)
);

-- staging raw pbp (store raw JSON or parquet meta)
CREATE TABLE IF NOT EXISTS stg_raw_pbp (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  year SMALLINT,
  raw_json JSON,
  fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- pipeline metadata table
CREATE TABLE IF NOT EXISTS meta_run_state (
  pipeline_name VARCHAR(100) PRIMARY KEY,
  last_run_at DATETIME,
  last_game_date DATE,
  notes TEXT
);
