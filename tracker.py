import argparse
import asyncio
import os
import sys
from pathlib import Path

import yaml

from src import db, discord, fetcher

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yaml"
LEAGUES_DIR = ROOT / "leagues"


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text())


def read_roster(league: str) -> list[str]:
    path = LEAGUES_DIR / f"{league}.txt"
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def write_roster(league: str, rsns: list[str]) -> None:
    LEAGUES_DIR.mkdir(exist_ok=True)
    path = LEAGUES_DIR / f"{league}.txt"
    body = "\n".join(sorted(set(rsns), key=str.lower))
    path.write_text(body + ("\n" if body else ""))


def cmd_run(args) -> int:
    config = load_config()
    if args.league not in config["leagues"]:
        print(f"Unknown league: {args.league}", file=sys.stderr)
        return 2
    lc = config["leagues"][args.league]

    webhook = os.environ.get(lc["webhook_env"])
    if not webhook and not args.dry_run:
        print(f"Missing env var: {lc['webhook_env']}", file=sys.stderr)
        return 2

    db.init_db()
    with db.connect() as conn:
        db.sync_roster(conn, args.league, read_roster(args.league))
        rsns = db.active_players(conn, args.league)

    if not rsns:
        print(f"No active players in roster for '{args.league}'.")
        return 0

    print(f"Fetching {len(rsns)} players for '{args.league}'...")
    fetched = asyncio.run(
        fetcher.fetch_all(lc["hiscore_url"], rsns, lc["league_row_index"])
    )
    print(f"Received {len(fetched)}/{len(rsns)} successful responses.")

    with db.connect() as conn:
        for row in fetched:
            db.insert_snapshot(conn, args.league, row)
        history = db.latest_two_snapshots(conn, args.league)

    rows = []
    for rsn, snaps in history.items():
        latest = snaps[0]
        prev = snaps[1] if len(snaps) > 1 else None
        rows.append({
            "rsn": rsn,
            "league_rank": latest["league_rank"],
            "league_points": latest["league_points"],
            "total_level": latest["total_level"],
            "rank_delta": (prev["league_rank"] - latest["league_rank"]) if prev else 0,
            "points_delta": (latest["league_points"] - prev["league_points"]) if prev else 0,
        })
    rows.sort(key=lambda r: (-r["league_points"], r["league_rank"]))

    messages = discord.build_messages(rows, config["tiers"])
    if args.dry_run:
        print("\n".join(messages))
    else:
        discord.send(webhook, messages, lc["display_name"], lc["avatar_url"])
        print(f"Posted {len(messages)} message(s) to Discord.")
    return 0


def cmd_add(args) -> int:
    roster = read_roster(args.league)
    lower = {r.lower() for r in roster}
    added = []
    for rsn in args.rsns:
        if rsn.lower() not in lower:
            roster.append(rsn)
            lower.add(rsn.lower())
            added.append(rsn)
    write_roster(args.league, roster)
    print(f"Added: {added or '(all already present)'}")
    return 0


def cmd_remove(args) -> int:
    roster = read_roster(args.league)
    targets = {r.lower() for r in args.rsns}
    kept = [r for r in roster if r.lower() not in targets]
    removed = [r for r in roster if r.lower() in targets]
    write_roster(args.league, kept)
    print(f"Removed: {removed or '(none matched)'}")
    return 0


def cmd_list(args) -> int:
    roster = read_roster(args.league)
    for rsn in roster:
        print(rsn)
    print(f"\n{len(roster)} player(s)", file=sys.stderr)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="tracker")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="Fetch, snapshot, and post to Discord")
    r.add_argument("league")
    r.add_argument("--dry-run", action="store_true", help="Print messages instead of posting")
    r.set_defaults(func=cmd_run)

    a = sub.add_parser("add", help="Add RSN(s) to a league roster")
    a.add_argument("league")
    a.add_argument("rsns", nargs="+")
    a.set_defaults(func=cmd_add)

    rm = sub.add_parser("remove", help="Remove RSN(s) from a league roster")
    rm.add_argument("league")
    rm.add_argument("rsns", nargs="+")
    rm.set_defaults(func=cmd_remove)

    ls = sub.add_parser("list", help="List RSNs in a league roster")
    ls.add_argument("league")
    ls.set_defaults(func=cmd_list)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
