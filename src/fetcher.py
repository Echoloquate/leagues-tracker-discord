import asyncio

import httpx


async def _fetch_one(
    client: httpx.AsyncClient,
    base_url: str,
    rsn: str,
    league_row: int,
    retries: int = 3,
) -> dict | None:
    for attempt in range(retries):
        try:
            r = await client.get(base_url, params={"player": rsn}, timeout=15.0)
            if r.status_code == 404:
                return None
            if r.status_code != 200:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            lines = r.text.strip().split("\n")
            total = lines[0].split(",")
            league = lines[league_row].split(",")
            return {
                "rsn": rsn,
                "total_rank": int(total[0]),
                "total_level": int(total[1]),
                "total_xp": int(total[2]),
                "league_rank": int(league[0]),
                "league_points": int(league[1]),
            }
        except (httpx.HTTPError, ValueError, IndexError):
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def fetch_all(
    base_url: str,
    rsns: list[str],
    league_row: int,
    concurrency: int = 8,
) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        async def one(rsn: str) -> dict | None:
            async with sem:
                return await _fetch_one(client, base_url, rsn, league_row)

        results = await asyncio.gather(*(one(r) for r in rsns))
    return [r for r in results if r is not None]
