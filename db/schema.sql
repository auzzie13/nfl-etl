-- NFL Data Warehouse Schema
-- Complete schema with staging tables and proper data types

-- Drop database and recreate to avoid foreign key conflicts
DROP DATABASE IF EXISTS nfl_dw;
CREATE DATABASE nfl_dw;
USE nfl_dw;

-- Date dimension
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

-- Teams dimension
CREATE TABLE IF NOT EXISTS dim_team (
  team_id INT PRIMARY KEY,
  team_name VARCHAR(100),
  city VARCHAR(100),
  abbreviation VARCHAR(10),
  conference VARCHAR(10),
  division VARCHAR(50),
  active TINYINT DEFAULT 1
);

-- Players dimension
CREATE TABLE dim_player (
  player_id VARCHAR(20) PRIMARY KEY,  -- Changed to VARCHAR to match NFL data
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  full_name VARCHAR(200),
  position VARCHAR(10),
  birth_date DATE,
  height_cm INT,
  weight_kg INT,
  active TINYINT DEFAULT 1,
  current_team_id INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX (last_name),
  INDEX (position)
);

-- Games dimension
CREATE TABLE dim_game (
  game_id VARCHAR(20) PRIMARY KEY,    -- Changed to VARCHAR to match NFL data
  season SMALLINT,
  week TINYINT,
  game_date DATE,
  home_team_id INT,
  away_team_id INT,
  home_score INT,
  away_score INT,
  status VARCHAR(50),
  venue VARCHAR(128),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX(season, game_date)
);

-- Team game facts
CREATE TABLE fact_team_game (
  game_id VARCHAR(20),
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
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (game_id, team_id),
  INDEX(team_id, date_key)
);

-- Player game facts
CREATE TABLE fact_player_game (
  game_id VARCHAR(20),
  player_id VARCHAR(20),
  team_id INT,
  opponent_team_id INT,
  date_key INT,
  snaps INT DEFAULT 0,
  pass_attempts INT DEFAULT 0,
  pass_completions INT DEFAULT 0,
  pass_yards INT DEFAULT 0,
  pass_tds INT DEFAULT 0,
  interception INT DEFAULT 0,
  rush_attempts INT DEFAULT 0,
  rush_yards INT DEFAULT 0,
  rush_tds INT DEFAULT 0,
  receptions INT DEFAULT 0,
  rec_yards INT DEFAULT 0,
  rec_tds INT DEFAULT 0,
  total_tds INT DEFAULT 0,
  fumbles INT DEFAULT 0,
  fantasy_points DECIMAL(6,2) DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (game_id, player_id),
  INDEX(player_id, date_key),
  INDEX(game_id)
);

-- Player injury facts (optional)
CREATE TABLE fact_player_injury (
  player_id VARCHAR(20),
  season SMALLINT,
  week TINYINT,
  injury_type VARCHAR(100),
  status VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX(player_id, season, week)
);

-- Staging table for players
CREATE TABLE stg_players (
  player_id VARCHAR(20),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  full_name VARCHAR(200),
  position VARCHAR(10),
  birth_date DATE
);

-- Staging table for player game stats
CREATE TABLE stg_player_game (
  game_id VARCHAR(20),
  player_id VARCHAR(20),
  pass_attempts INT DEFAULT 0,
  completions INT DEFAULT 0,
  pass_yards INT DEFAULT 0,
  pass_tds INT DEFAULT 0,
  interception INT DEFAULT 0,
  rush_attempts INT DEFAULT 0,
  rush_yards INT DEFAULT 0,
  rush_tds INT DEFAULT 0,
  receptions INT DEFAULT 0,
  rec_yards INT DEFAULT 0,
  rec_tds INT DEFAULT 0,
  total_tds INT DEFAULT 0,
  fumbles INT DEFAULT 0
);

-- Staging table for injuries
CREATE TABLE stg_injuries (
  player_id VARCHAR(20),
  season SMALLINT,
  week TINYINT,
  injury_type VARCHAR(100),
  status VARCHAR(50)
);

-- Pipeline metadata table
CREATE TABLE meta_run_state (
  pipeline_name VARCHAR(100) PRIMARY KEY,
  last_run_at DATETIME,
  last_game_date DATE,
  notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Performance indexes
CREATE INDEX idx_fact_player_game_stats ON fact_player_game(pass_yards DESC, rush_yards DESC, rec_yards DESC);
CREATE INDEX idx_fact_player_game_season ON fact_player_game(date_key);
CREATE INDEX idx_player_position_active ON dim_player(position, active);

-- Show all created tables
SELECT 'Tables created successfully!' as status;
SHOW TABLES;