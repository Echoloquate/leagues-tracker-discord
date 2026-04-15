import httpx


def tier_for(points: int, tiers: list[dict]) -> int:
    for t in tiers:
        if points > t["min"]:
            return t["tier"]
    return tiers[-1]["tier"]


def _fmt_delta(n: int) -> str:
    if n > 0:
        return f"+{n}"
    return str(n)


def build_messages(rows: list[dict], tiers: list[dict], chunk_size: int = 35) -> list[str]:
    pad_rank = len(str(max(len(rows), 1))) + 2
    header = (
        "```"
        + "#".ljust(pad_rank)
        + "Rank".ljust(7)
        + "ΔRank".ljust(7)
        + "RSN".ljust(14)
        + "Pts".ljust(7)
        + "ΔPts".ljust(7)
        + "T".ljust(3)
        + "Lvl".ljust(5)
        + "\n"
    )
    messages: list[str] = []
    buf = header
    for i, row in enumerate(rows, 1):
        buf += (
            str(i).ljust(pad_rank)
            + str(row["league_rank"]).ljust(7)
            + _fmt_delta(row["rank_delta"]).ljust(7)
            + str(row["rsn"])[:13].ljust(14)
            + str(row["league_points"]).ljust(7)
            + _fmt_delta(row["points_delta"]).ljust(7)
            + str(tier_for(row["league_points"], tiers)).ljust(3)
            + str(row["total_level"]).ljust(5)
            + "\n"
        )
        if i % chunk_size == 0 and i != len(rows):
            buf += "```"
            messages.append(buf)
            buf = header
    buf += "```"
    messages.append(buf)
    return messages


def send(webhook_url: str, messages: list[str], username: str, avatar_url: str) -> None:
    with httpx.Client(timeout=20.0) as client:
        for m in messages:
            r = client.post(
                webhook_url,
                json={"content": m, "username": username, "avatar_url": avatar_url},
            )
            r.raise_for_status()
