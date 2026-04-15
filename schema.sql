CREATE TABLE IF NOT EXISTS players (
  rsn TEXT NOT NULL,
  league TEXT NOT NULL,
  added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  active INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (rsn, league)
);

CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rsn TEXT NOT NULL,
  league TEXT NOT NULL,
  fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  league_rank INTEGER,
  league_points INTEGER,
  total_rank INTEGER,
  total_level INTEGER,
  total_xp INTEGER
);

CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
  ON snapshots(league, rsn, fetched_at DESC);
