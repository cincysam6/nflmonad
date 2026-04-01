-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================
-- These are written as CREATE TABLE statements (not views).
-- They are populated by the R transform pipeline via DuckDB CTAS or inserts.
-- =============================================================================

-- ---- dim_season -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_season (
  season          INTEGER PRIMARY KEY,
  season_type     VARCHAR,   -- 'regular' | 'postseason'
  league_year     INTEGER,
  season_start_date DATE,
  season_end_date   DATE,
  playoffs_start_week INTEGER,
  super_bowl_week     INTEGER
);

-- ---- dim_week ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_week (
  season          INTEGER,
  week            INTEGER,
  season_type     VARCHAR,
  week_start_date DATE,
  week_end_date   DATE,
  PRIMARY KEY (season, week)
);

-- ---- dim_team ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_team (
  team_abbr       VARCHAR PRIMARY KEY,
  full_name       VARCHAR,
  short_name      VARCHAR,
  conference      VARCHAR,   -- AFC | NFC
  division        VARCHAR,   -- e.g. AFC North
  team_color      VARCHAR,
  team_color2     VARCHAR,
  team_logo_url   VARCHAR,
  -- Relocation handling: a team may have multiple historical abbrs
  current_abbr    VARCHAR,   -- canonical current abbreviation
  active_flag     INTEGER DEFAULT 1
);

-- ---- dim_player -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_player (
  player_id       VARCHAR PRIMARY KEY,  -- gsis_id
  full_name       VARCHAR,
  first_name      VARCHAR,
  last_name       VARCHAR,
  position        VARCHAR,
  position_group  VARCHAR,
  birth_date      DATE,
  college         VARCHAR,
  height          INTEGER,   -- inches
  weight          INTEGER,   -- pounds
  entry_year      INTEGER,
  rookie_year     INTEGER,
  draft_club      VARCHAR,
  draft_number    INTEGER,
  active_flag     INTEGER DEFAULT 1,
  headshot_url    VARCHAR,
  -- Cross-reference IDs
  pfr_id          VARCHAR,
  espn_id         VARCHAR,
  sleeper_id      VARCHAR
);

-- ---- dim_game ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_game (
  game_id         VARCHAR PRIMARY KEY,
  season          INTEGER,
  week            INTEGER,
  season_type     VARCHAR,
  game_date       DATE,
  kickoff_time    VARCHAR,
  home_team       VARCHAR,
  away_team       VARCHAR,
  stadium         VARCHAR,
  surface         VARCHAR,
  roof            VARCHAR,
  FOREIGN KEY (home_team) REFERENCES dim_team(team_abbr),
  FOREIGN KEY (away_team) REFERENCES dim_team(team_abbr)
);

-- ---- dim_position_group -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_position_group (
  position        VARCHAR PRIMARY KEY,
  position_group  VARCHAR,   -- QB RB WR TE OL DL LB CB S K P DST
  side_of_ball    VARCHAR    -- offense | defense | special_teams
);

-- Seed data
INSERT OR REPLACE INTO dim_position_group VALUES
  ('QB',  'QB',  'offense'),
  ('RB',  'RB',  'offense'), ('HB', 'RB', 'offense'), ('FB', 'RB', 'offense'),
  ('WR',  'WR',  'offense'),
  ('TE',  'TE',  'offense'),
  ('C',   'OL',  'offense'), ('G',  'OL', 'offense'), ('T',  'OL', 'offense'),
  ('DE',  'DL',  'defense'), ('DT', 'DL', 'defense'), ('NT', 'DL', 'defense'),
  ('ILB','LB',  'defense'), ('OLB','LB', 'defense'), ('MLB','LB', 'defense'),
  ('CB',  'CB',  'defense'),
  ('FS',  'S',   'defense'), ('SS', 'S',  'defense'), ('DB', 'S',  'defense'),
  ('K',   'K',   'special_teams'),
  ('P',   'P',   'special_teams'),
  ('LS',  'LS',  'special_teams')
;


-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ---- fact_play --------------------------------------------------------------
-- Grain: game_id + play_id
CREATE TABLE IF NOT EXISTS fact_play (
  game_id                 VARCHAR,
  play_id                 INTEGER,
  season                  INTEGER,
  week                    INTEGER,
  posteam                 VARCHAR,
  defteam                 VARCHAR,
  play_type               VARCHAR,
  down                    INTEGER,
  ydstogo                 INTEGER,
  yardline_100            INTEGER,
  yards_gained            DOUBLE,
  epa                     DOUBLE,
  wpa                     DOUBLE,
  success                 INTEGER,
  pass_attempt            INTEGER,
  rush_attempt            INTEGER,
  touchdown               INTEGER,
  interception            INTEGER,
  fumble_lost             INTEGER,
  sack                    INTEGER,
  air_yards               DOUBLE,
  yards_after_catch       DOUBLE,
  passer_id               VARCHAR,
  rusher_id               VARCHAR,
  receiver_id             VARCHAR,
  score_differential      DOUBLE,
  game_seconds_remaining  DOUBLE,
  shotgun                 INTEGER,
  no_huddle               INTEGER,
  red_zone_flag           INTEGER,
  goal_line_flag          INTEGER,
  PRIMARY KEY (game_id, play_id)
);

-- ---- fact_drive -------------------------------------------------------------
-- Grain: game_id + drive_id
CREATE TABLE IF NOT EXISTS fact_drive (
  game_id         VARCHAR,
  drive_id        INTEGER,
  season          INTEGER,
  posteam         VARCHAR,
  defteam         VARCHAR,
  plays           INTEGER,
  yards_gained    DOUBLE,
  touchdowns      INTEGER,
  turnovers       INTEGER,
  reached_red_zone INTEGER,
  drive_result    VARCHAR,
  PRIMARY KEY (game_id, drive_id)
);

-- ---- fact_team_game ---------------------------------------------------------
-- Grain: game_id + team
CREATE TABLE IF NOT EXISTS fact_team_game (
  game_id         VARCHAR,
  team            VARCHAR,
  opponent        VARCHAR,
  season          INTEGER,
  week            INTEGER,
  home_flag       INTEGER,
  points_for      INTEGER,
  points_against  INTEGER,
  margin          INTEGER,
  win_flag        INTEGER,
  epa_per_play    DOUBLE,
  pass_epa_per_dropback DOUBLE,
  rush_epa_per_carry    DOUBLE,
  def_epa_per_play      DOUBLE,
  success_rate    DOUBLE,
  pass_rate       DOUBLE,
  turnovers       INTEGER,
  sacks_allowed   INTEGER,
  red_zone_td_rate DOUBLE,
  PRIMARY KEY (game_id, team)
);

-- ---- fact_player_game -------------------------------------------------------
-- Grain: game_id + player_id
CREATE TABLE IF NOT EXISTS fact_player_game (
  game_id         VARCHAR,
  player_id       VARCHAR,
  season          INTEGER,
  week            INTEGER,
  team            VARCHAR,
  opponent        VARCHAR,
  position        VARCHAR,
  -- Stats stored as NULLable doubles for flexibility across positions
  completions     INTEGER,
  attempts        INTEGER,
  passing_yards   DOUBLE,
  passing_tds     INTEGER,
  interceptions   INTEGER,
  carries         INTEGER,
  rushing_yards   DOUBLE,
  rushing_tds     INTEGER,
  targets         INTEGER,
  receptions      INTEGER,
  receiving_yards DOUBLE,
  receiving_tds   INTEGER,
  offense_snaps   INTEGER,
  offense_snap_pct DOUBLE,
  fantasy_points_ppr DOUBLE,
  PRIMARY KEY (game_id, player_id)
);

-- ---- fact_team_week ---------------------------------------------------------
-- Grain: season + week + team
CREATE TABLE IF NOT EXISTS fact_team_week (
  season          INTEGER,
  week            INTEGER,
  team            VARCHAR,
  game_id         VARCHAR,
  opponent        VARCHAR,
  home_flag       INTEGER,
  points_for      INTEGER,
  points_against  INTEGER,
  win_flag        INTEGER,
  PRIMARY KEY (season, week, team)
);

-- ---- fact_player_week -------------------------------------------------------
-- Grain: season + week + player_id
CREATE TABLE IF NOT EXISTS fact_player_week (
  season          INTEGER,
  week            INTEGER,
  player_id       VARCHAR,
  team            VARCHAR,
  position        VARCHAR,
  targets         INTEGER,
  receptions      INTEGER,
  receiving_yards DOUBLE,
  carries         INTEGER,
  rushing_yards   DOUBLE,
  attempts        INTEGER,
  passing_yards   DOUBLE,
  fantasy_points_ppr DOUBLE,
  PRIMARY KEY (season, week, player_id)
);

-- ---- fact_injury_status -----------------------------------------------------
-- Grain: season + week + player_id + report_date
CREATE TABLE IF NOT EXISTS fact_injury_status (
  season          INTEGER,
  week            INTEGER,
  player_id       VARCHAR,
  team            VARCHAR,
  report_date     DATE,
  position        VARCHAR,
  report_status   VARCHAR,
  injury_type     VARCHAR,
  injury_severity_score INTEGER,
  PRIMARY KEY (season, week, player_id, report_date)
);

-- ---- fact_depth_chart -------------------------------------------------------
-- Grain: season + week + team + player_id + position + depth_team + formation
CREATE TABLE IF NOT EXISTS fact_depth_chart (
  season          INTEGER,
  week            INTEGER,
  team            VARCHAR,
  player_id       VARCHAR,
  position        VARCHAR,
  depth_team      INTEGER,  -- 1=starter, 2=backup, etc.
  formation       VARCHAR,
  PRIMARY KEY (season, week, team, player_id, position, depth_team, formation)
);

-- ---- fact_snap_count --------------------------------------------------------
-- Grain: season + week + pfr_player_id
CREATE TABLE IF NOT EXISTS fact_snap_count (
  season          INTEGER,
  week            INTEGER,
  pfr_player_id   VARCHAR,
  team            VARCHAR,
  position        VARCHAR,
  offense_snaps   INTEGER,
  offense_snap_pct DOUBLE,
  defense_snaps   INTEGER,
  defense_snap_pct DOUBLE,
  st_snaps        INTEGER,
  st_snap_pct     DOUBLE,
  PRIMARY KEY (season, week, pfr_player_id)
);

-- ---- fact_market_game -------------------------------------------------------
-- Grain: game_id + market_timestamp + sportsbook
-- Placeholder — populated when external odds are available
CREATE TABLE IF NOT EXISTS fact_market_game (
  game_id           VARCHAR,
  market_timestamp  TIMESTAMP,
  sportsbook        VARCHAR,
  market_type       VARCHAR,
  home_spread       DOUBLE,
  total_line        DOUBLE,
  home_ml           INTEGER,
  away_ml           INTEGER,
  over_juice        DOUBLE,
  under_juice       DOUBLE,
  PRIMARY KEY (game_id, market_timestamp, sportsbook, market_type)
);
