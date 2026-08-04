"""
週間即数ランキング（お試し）

7月最終週の例:
  python weekly_collect.py --year 2026 --month 7 --week last
  python weekly_collect.py --start 2026-07-27 --end 2026-08-02

方針（trial）:
- 明示的な「今週N即 / 週N節」や期間ラベル付き報告を優先
- 月次・年間総括は弾く
- 最低掲載しきい値はデフォルト 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from monthly_collect import (
    build_period_result,
    clean_tweet_text,
    create_playwright_contexts,
    create_sessions,
    get_user_id,
    get_user_tweets,
    load_json,
    load_playwright_cookie_sets,
    normalize_period_row,
    save_json,
    search_query_tweets,
    should_replace_result,
)

OUTPUT_ACCOUNTS = "data/sokusuu_accounts.json"


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def last_week_of_month(year: int, month: int) -> tuple[date, date]:
    """その月の最終日を含む ISO 週（月〜日）を返す。

    2026-07 なら 2026-07-27(月)〜2026-08-02(日)。
    """
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    # Monday=0
    start = last_day - timedelta(days=last_day.weekday())
    end = start + timedelta(days=6)
    return start, end


def week_label(start: date, end: date) -> str:
    return f"{start.isoformat()}_{end.isoformat()}"


def output_path(start: date, end: date) -> str:
    return f"data/weekly_{week_label(start, end)}.json"


def reporting_window(start: date, end: date) -> tuple[date, date]:
    # 週内投稿 + 終了後2日の振り返り
    return start, end + timedelta(days=2)


def parse_tweet_datetime(created_at: str) -> date | None:
    if not created_at:
        return None
    try:
        return datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").date()
    except Exception:
        return None


def extract_weekly_count(text: str, start: date, end: date) -> int | None:
    """本文から対象週の週間即数を抽出。"""
    cleaned = clean_tweet_text(text)
    if cleaned.startswith("RT @"):
        return None

    # 月次・年間は除外
    if re.search(r"(?:1[0-2]|[1-9])\s*月\s*総括|年間|年総括|年報", cleaned):
        if not re.search(r"今週|今週の|週次|週間|今週", cleaned):
            return None
    if re.search(r"(?:目標|予定|目指す|したい|します[!！])", cleaned):
        return None

    patterns = [
        r"今週\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"今週\s*(?:は|の)?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"今週\s*(?:は|の結果|の実績|の総括)?\s*[=:：]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"週間\s*(?:は|で|計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"週次\s*(?:は|で|計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"(?:週総括|週まとめ|週振り返り)\s*[=:：]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"\[週\]\s*(\d+)\s*(?:即|節)",
        # 7/27-8/2 や 7月最終週
        rf"{start.month}/{start.day}\s*[-〜~～]\s*{end.month}/{end.day}.{{0,20}}?(\d+)\s*(?:即|節|get|g\b)",
        r"最終週\s*(?:は|で)?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"(?:計|合計)\s*(\d+)\s*(?:即|節)\s*(?:/|\(|（)?\s*週",
        # 「今週末N即」
        r"今週末?\s*(?:で|に|は)?\s*(\d+)\s*(?:即|節)",
    ]
    for pat in patterns:
        m = re.search(pat, cleaned, re.IGNORECASE)
        if not m:
            continue
        value = int(m.group(1))
        if 0 < value <= 200:
            return value

    # 「今週 総括 5即」ゆるめ
    if re.search(r"今週|今週|週総括|週まとめ", cleaned):
        m = re.search(
            r"(?:総括|結果|実績|まとめ|着地)\s*[=:：]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b)",
            cleaned,
        )
        if m:
            value = int(m.group(1))
            if 0 < value <= 200:
                return value
    return None


def pick_best_weekly_hit(tweets, username: str, start: date, end: date):
    rep_start, rep_end = reporting_window(start, end)
    best = None
    for tweet in tweets:
        created = parse_tweet_datetime(tweet.get("created_at", ""))
        if created and not (rep_start <= created <= rep_end):
            continue
        count = extract_weekly_count(tweet.get("text", ""), start, end)
        if not count:
            continue
        hit = {
            "count": count,
            "url": f"https://x.com/{username}/status/{tweet['id']}",
            "text": (tweet.get("text") or "")[:500],
            "created_at": tweet.get("created_at", ""),
        }
        if best is None or count > best["count"]:
            best = hit
    return best


async def seed_weekly_search(
    context, usernames: list[str], start: date, end: date, scrolls: int = 4
):
    """シード向け from:user Search（グローバルはノイズが多いので使わない）。"""
    rep_start, rep_end = reporting_window(start, end)
    until = rep_end + timedelta(days=1)
    best_by_user: dict[str, dict] = {}
    for i, username in enumerate(usernames, 1):
        q = (
            f"from:{username} (今週 OR 週総括 OR 週まとめ OR 週間 OR 最終週 OR 節 OR 即) "
            f"since:{rep_start.isoformat()} until:{until.isoformat()}"
        )
        try:
            tweets = await search_query_tweets(context, q, scrolls=scrolls)
        except Exception as e:
            print(f"  [seed-search] @{username} ERR {e}")
            continue
        hit = pick_best_weekly_hit(tweets, username, start, end)
        if hit:
            hit["username"] = username
            best_by_user[username.lower()] = hit
            print(f"  [seed-search] @{username}: {hit['count']}")
        if i % 15 == 0:
            print(f"  [seed-search] {i}/{len(usernames)} hits={len(best_by_user)}")
    return best_by_user


def load_seed_usernames() -> list[str]:
    usernames: list[str] = []
    seen: set[str] = set()

    def add(u: str) -> None:
        key = u.lower()
        if not u or key in seen:
            return
        seen.add(key)
        usernames.append(u)

    # July monthly seeds if present
    july = load_json("data/monthly_2026_07.json", [])
    for row in july:
        add(row.get("username", ""))

    accounts = load_json(OUTPUT_ACCOUNTS, [])
    # prioritize higher sokusuu
    accounts = sorted(
        accounts, key=lambda r: -int(r.get("sokusuu") or r.get("followers_count") or 0)
    )
    for row in accounts[:250]:
        add(row.get("username", ""))

    return usernames


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="週間即数ランキング（お試し）")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--month", type=int, default=7)
    parser.add_argument(
        "--week",
        choices=["last"],
        default="last",
        help="last=その月の最終日を含む ISO 週",
    )
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--min-count", type=int, default=3)
    parser.add_argument("--limit", type=int, default=0, help="TL走査の最大人数（0=全seed）")
    parser.add_argument("--skip-timeline", action="store_true")
    parser.add_argument("--headful", action="store_true")
    args = parser.parse_args()

    if args.start and args.end:
        start, end = parse_iso_date(args.start), parse_iso_date(args.end)
    else:
        start, end = last_week_of_month(args.year, args.month)

    out = output_path(start, end)
    print("=" * 60)
    print(f"週間即数ランキング（trial） {start} 〜 {end}")
    print(f"報告窓: {reporting_window(start, end)[0]} 〜 {reporting_window(start, end)[1]}")
    print(f"出力: {out}")
    print("=" * 60)

    accounts = load_json(OUTPUT_ACCOUNTS, [])
    accounts_map = {a["username"].lower(): a for a in accounts}
    seeds = load_seed_usernames()
    if args.limit > 0:
        seeds = seeds[: args.limit]
    print(f"seed: {len(seeds)}")

    results_map: dict[str, dict] = {}
    sessions = create_sessions()
    session_idx = [0]
    cookie_sets = load_playwright_cookie_sets()

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser, contexts = await create_playwright_contexts(
            p, cookie_sets, headless=not args.headful
        )
        ctx = contexts[0]["context"]

        # 1) seed-focused Search（グローバルは一般TLに埋もれるので使わない）
        # trial: 上位 seed を厚めに
        search_seeds = seeds[:80]
        print(f"\n[1/2] seed weekly search ({len(search_seeds)})")
        seed_hits = await seed_weekly_search(ctx, search_seeds, start, end, scrolls=3)
        print(f"  seed-search users={len(seed_hits)}")
        for key, hit in seed_hits.items():
            account = dict(accounts_map.get(key, {}))
            account.setdefault("username", hit.get("username") or key)
            if hit.get("display_name") and not account.get("display_name"):
                account["display_name"] = hit["display_name"]
            row = build_period_result(account, hit, "weekly_count", "search")
            results_map[key] = row

        # 2) seed timeline dig（残りの取りこぼし）
        if not args.skip_timeline and sessions:
            print("\n[2/2] seed timeline dig")
            for i, username in enumerate(seeds[:120], 1):
                key = username.lower()
                if key in results_map:
                    continue
                uid = get_user_id(sessions, session_idx, username)
                if not uid:
                    continue
                tweets = get_user_tweets(
                    sessions, session_idx, uid, count=40, max_pages=2
                )
                hit = pick_best_weekly_hit(tweets, username, start, end)
                if not hit:
                    if i % 30 == 0:
                        print(f"  [{i}] hits={len(results_map)}")
                    continue
                account = dict(accounts_map.get(key, {}))
                account.setdefault("username", username)
                row = build_period_result(account, hit, "weekly_count", "timeline")
                cur = results_map.get(key)
                if should_replace_result(cur, row, "weekly_count"):
                    results_map[key] = row
                    print(f"  [timeline] @{username}: {hit['count']}")
        elif args.skip_timeline:
            print("\n[2/2] seed timeline dig: skipped")

        await browser.close()

    rows = [normalize_period_row(r) for r in results_map.values()]
    rows = [r for r in rows if int(r.get("weekly_count") or 0) >= args.min_count]
    rows.sort(
        key=lambda r: (
            -int(r.get("weekly_count") or 0),
            -(r.get("followers_count") or 0),
            (r.get("username") or "").lower(),
        )
    )
    # annotate period
    for r in rows:
        r["period_start"] = start.isoformat()
        r["period_end"] = end.isoformat()
        r["period_label"] = f"{start.month}/{start.day}〜{end.month}/{end.day}"

    save_json(out, rows)
    print(f"\n{len(rows)}件 (>= {args.min_count}) -> {out}")
    for i, r in enumerate(rows[:20], 1):
        print(f"  {i}. @{r['username']}: {r['weekly_count']}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
