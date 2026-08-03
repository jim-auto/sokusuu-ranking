"""Batch-dig missing monthly recaps for July 5+ streak gaps via SearchTimeline."""
from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import monthly_collect
from monthly_collect import extract_monthly_count
from playwright.async_api import async_playwright

# username -> months to dig in 2026 (priority first)
TARGETS: dict[str, list[int]] = {
    "tora_maru005": [4, 2, 1],
    "kukuru_nanpa": [3, 1],
    "tomu_riddle": [6, 5, 4, 3],
    "nakayamasoku": [6, 5, 4, 3],
    "Tinder_god_2": [5, 4, 3, 1],
    "anshin_pua": [5, 4, 3, 1],
    "makoto__pua": [6, 5, 4, 3, 2, 1],
    "socool55555": [6, 5, 4, 3, 2, 1],
    "kimu__himitsu2": [6, 5, 4, 3, 2, 1],
    "147asdf764": [6, 5, 4, 3, 2, 1],
    "MSKsecond": [6, 5, 4, 3, 2, 1],
    "cx_lm5": [6],
    "tsutsumi_ye4pe": [5, 4, 3],
    "ururunpua": [4, 3, 2],
    "ak1_pua": [4, 3, 2, 1],
    "17go_pua": [3, 2, 1],
    "okarun_pua": [2, 1],
    "dick_duck_swing": [5],  # under 5, check if wrong
    "Nano486273": [6],  # under 5
}

YEAR = 2026


def month_window(month: int):
    # reporting window: last ~5 days of month through first ~10 of next
    if month == 12:
        since = f"{YEAR}-12-25"
        until = f"{YEAR + 1}-01-12"
    else:
        since = f"{YEAR}-{month:02d}-25"
        until = f"{YEAR}-{month + 1:02d}-12"
    # also allow mid-month late recaps via broader
    since_broad = f"{YEAR}-{month:02d}-20"
    until_broad = f"{YEAR}-{month + 1:02d}-15" if month < 12 else f"{YEAR + 1}-01-15"
    return since, until, since_broad, until_broad


def build_queries(username: str, month: int) -> list[str]:
    since, until, since_b, until_b = month_window(month)
    m = month
    return [
        f'from:{username} ("{m}月総括" OR "{m}月 総括" OR "{m}月統括" OR "{m}月まとめ" OR "{m}月 まとめ") since:{since_b} until:{until_b}',
        f'from:{username} ("{m}月" OR 今月) (総括 OR 統括 OR 実績 OR 結果 OR まとめ OR 着地 OR 戦績 OR 月間 OR 振り返り) (即 OR 節 OR そ OR そく OR get) since:{since_b} until:{until_b}',
        f'from:{username} "{m}月" (即 OR 節 OR そ OR そく OR まとめ) since:{since} until:{until}',
    ]


async def dig():
    sets = monthly_collect.load_playwright_cookie_sets()
    if not sets:
        raise SystemExit("no playwright cookies")
    print(f"cookie sets: {len(sets)}")

    results: dict[str, dict] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 720},
        )
        await context.add_cookies(sets[0]["cookies"])

        for username, months in TARGETS.items():
            print(f"\n===== @{username} months={months} =====")
            best: dict[int, dict] = {}
            for month in months:
                for qi, query in enumerate(build_queries(username, month)):
                    print(f"  M{month} q{qi+1}: {query[:90]}...")
                    try:
                        tweets = await monthly_collect.search_query_tweets(
                            context, query, scrolls=4
                        )
                    except Exception as e:
                        print(f"    search fail: {e}")
                        await asyncio.sleep(3)
                        continue
                    print(f"    hits={len(tweets)}")
                    for t in tweets:
                        u = (t.get("username") or "").lower()
                        if u and u != username.lower():
                            continue
                        text = t.get("text") or t.get("full_text") or ""
                        if text.startswith("RT @"):
                            continue
                        count = extract_monthly_count(text, YEAR, month)
                        # also try without requiring month if clear recap
                        if not count and re.search(
                            rf"(?<!\d){month}\s*月.{{0,30}}(\d{{1,3}})\s*(即|節|そ|get)",
                            text,
                            re.I,
                        ):
                            count = extract_monthly_count(text, YEAR, month)
                        if not count:
                            # show candidates with month mention
                            if re.search(rf"(?<!\d){month}\s*月", text) and re.search(
                                r"総括|実績|結果|まとめ|着地|月間|節|即", text
                            ):
                                print(
                                    f"    (no extract) {t.get('created_at')} "
                                    f"{(t.get('url') or t.get('tweet_url') or '')}"
                                )
                                print(f"      {text[:160].replace(chr(10), ' / ')}")
                            continue
                        url = (
                            t.get("url")
                            or t.get("tweet_url")
                            or t.get("source_url")
                            or ""
                        )
                        tid = t.get("id") or t.get("rest_id") or ""
                        if not url and tid:
                            url = f"https://x.com/{username}/status/{tid}"
                        prev = best.get(month)
                        if prev is None or count > prev["count"]:
                            best[month] = {
                                "count": count,
                                "url": url,
                                "created_at": t.get("created_at"),
                                "text": text[:500],
                                "id": tid,
                            }
                            print(
                                f"    BEST M{month}={count} {url}"
                            )
                            print(f"      {text[:140].replace(chr(10), ' / ')}")
                    await asyncio.sleep(1.2)
                    # if we already have a solid hit, skip remaining queries for month
                    if month in best and best[month]["count"] >= 5:
                        break
                await asyncio.sleep(0.8)

            results[username] = best
            print(f"  => { {m: v['count'] for m,v in sorted(best.items())} }")

        await browser.close()

    out = Path("data/streak_batch_dig.json")
    # serialize
    serial = {
        u: {str(m): v for m, v in best.items()} for u, best in results.items()
    }
    out.write_text(json.dumps(serial, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nsaved", out)

    # diff vs existing
    import glob

    existing: dict[str, dict[int, int]] = {}
    for path in glob.glob("data/monthly_2026_*.json"):
        m = int(Path(path).stem.split("_")[-1])
        for r in json.loads(Path(path).read_text(encoding="utf-8")):
            u = str(r.get("username") or "").lower()
            c = int(r.get("monthly_count") or 0)
            if c > 0:
                existing.setdefault(u, {})[m] = max(existing.get(u, {}).get(m, 0), c)

    print("\n=== NEW / HIGHER ===")
    for u, best in results.items():
        for m, info in sorted(best.items()):
            old = existing.get(u.lower(), {}).get(m, 0)
            if info["count"] > old:
                print(f"  @{u} 2026-{m:02d}: {old} -> {info['count']}  {info['url']}")


if __name__ == "__main__":
    asyncio.run(dig())
