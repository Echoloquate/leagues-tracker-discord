import sqlite3
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "leagues.db"
SCHEMA_PATH = ROOT / "schema.sql"


def init_db(db_path: Path = DB_PATH) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_PATH.read_text())


@contextmanager
def connect(db_path: Path = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def sync_roster(conn, league: str, rsns: list[str]) -> None:
    roster = {r.strip() for r in rsns if r.strip()}
    roster_lower = {r.lower(): r for r in roster}

    existing = {
        row["rsn"]: bool(row["active"])
        for row in conn.execute(
            "SELECT rsn, active FROM players WHERE league = ?", (league,)
        )
    }
    existing_lower = {rsn.lower(): rsn for rsn in existing}

    for key, original in roster_lower.items():
        if key in existing_lower:
            stored = existing_lower[key]
            if not existing[stored]:
                conn.execute(
                    "UPDATE players SET active = 1 WHERE league = ? AND rsn = ?",
                    (league, stored),
                )
        else:
            conn.execute(
                "INSERT INTO players (rsn, league) VALUES (?, ?)",
                (original, league),
            )

    for key, stored in existing_lower.items():
        if key not in roster_lower and existing[stored]:
            conn.execute(
                "UPDATE players SET active = 0 WHERE league = ? AND rsn = ?",
                (league, stored),
            )


def active_players(conn, league: str) -> list[str]:
    return [
        row["rsn"]
        for row in conn.execute(
            "SELECT rsn FROM players WHERE league = ? AND active = 1 ORDER BY rsn COLLATE NOCASE",
            (league,),
        )
    ]


def insert_snapshot(conn, league: str, row: dict) -> None:
    conn.execute(
        """INSERT INTO snapshots
           (rsn, league, league_rank, league_points, total_rank, total_level, total_xp)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            row["rsn"],
            league,
            row["league_rank"],
            row["league_points"],
            row["total_rank"],
            row["total_level"],
            row["total_xp"],
        ),
    )


def latest_two_snapshots(conn, league: str) -> dict[str, list[dict]]:
    rows = conn.execute(
        """SELECT rsn, fetched_at, league_rank, league_points,
                  total_rank, total_level, total_xp,
                  ROW_NUMBER() OVER (
                    PARTITION BY rsn ORDER BY fetched_at DESC, id DESC
                  ) AS rn
           FROM snapshots
           WHERE league = ?""",
        (league,),
    ).fetchall()
    out: dict[str, list[dict]] = {}
    for row in rows:
        if row["rn"] > 2:
            continue
        out.setdefault(row["rsn"], []).append(dict(row))
    return out
