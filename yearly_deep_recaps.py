# -*- coding: utf-8 -*-
"""Deep-find 2025 yearly recap tweets for target users and merge into yearly_2025.json."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from monthly_collect import (
    build_period_result,
    create_playwright_contexts,
    create_sessions,
    extract_yearly_count,
    extract_yearly_profile_count,
    get_user_id,
    get_user_tweets,
    load_json,
    load_playwright_cookie_sets,
    normalize_period_row,
    save_json,
    search_query_tweets,
    should_replace_result,
)

YEAR = 2025

def tweet_in_year_window(created_at: str, year: int = YEAR) -> bool:
    """Accept recap posts from year-12-01 through year+1-02-15, or created year == year."""
    if not created_at:
        return False
    try:
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return False
    start = datetime(year, 12, 1, tzinfo=dt.tzinfo)
    end = datetime(year + 1, 2, 15, tzinfo=dt.tzinfo)
    # also allow any tweet in calendar year (rare mid-year annual recaps)
    if dt.year == year:
        return True
    return start <= dt <= end

YEARLY_PATH = "data/yearly_2025.json"
ACCOUNTS = "data/sokusuu_accounts.json"
CUTOFF = 10

PROFILE_PRIORITY = [
    "nakayamasoku",
    "PUAINOKI",
    "socool55555",
    "dick_duck_swing",
    "namenayone",
    "RobertPowerJ",
    "hirohirorenai",
    "1jEvvc",
    "yszk1624",
    "bYiNieJVI17Hk2M",
    "allen_pua_",
    "yutayuta_pua",
    "ak166121",
    "gofugofu5252",
    "komanyan28",
]

MISSING_REGULARS = [
    "17go_pua",
    "kimu__himitsu2",
    "makoto__pua",
    "Lattie_pua",
    "kukuru_nanpa",
    "nanpasei",
    "tonpamiso",
    "sub_chilll",
    "mic_pua",
    "okarun_pua",
    "shingen_pua",
    "ryepua",
    "kannen170",
    "suto_komari",
    "kuroiwa_45",
    "Niko_PUA",
    "tsutsumi_ye4pe",
]


async def find_yearly_for_user(context, sessions, session_idx, username: str) -> dict | None:
    """Return best yearly hit dict or None."""
    best = None

    queries = [
        f"from:{username} since:2025-12-01 until:2026-02-15 (総括 OR 統括 OR 振り返り OR まとめ OR 年報 OR 着地)",
        f"from:{username} since:2025-12-01 until:2026-02-15 (2025年 OR 25年 OR 年間)",
        f"from:{username} since:2025-01-01 until:2026-01-31 2025年",
        f"from:{username} (2025年総括 OR 25年総括 OR 年末総括 OR 年間総括)",
    ]
    seen = set()
    tweets = []
    for q in queries:
        try:
            got = await search_query_tweets(context, q, scrolls=5)
        except Exception as e:
            print(f"  [search] @{username} {e}")
            continue
        for t in got:
            tid = t.get("id")
            if tid in seen:
                continue
            seen.add(tid)
            tweets.append(t)

    # TL supplement
    if sessions:
        try:
            uid = get_user_id(sessions, session_idx, username)
            if uid:
                tl = get_user_tweets(sessions, session_idx, uid, count=50, max_pages=5)
                for t in tl:
                    tid = t.get("id")
                    if tid in seen:
                        continue
                    seen.add(tid)
                    tweets.append(t)
        except Exception as e:
            print(f"  [tl] @{username} {e}")

    for t in tweets:
        text = t.get("text") or ""
        # skip pure RT
        if text.startswith("RT @") or text.startswith("RT@"):
            continue
        if not tweet_in_year_window(t.get("created_at") or "", YEAR):
            continue
        count = extract_yearly_count(text, YEAR, strict=True)
        if not count:
            count = extract_yearly_count(text, YEAR, strict=False)
        if not count:
            continue
        # prefer higher count; require year-ish context for large numbers
        if count < 10:
            continue
        hit = {
            "count": count,
            "url": f"https://x.com/{username}/status/{t.get('id')}",
            "text": text[:500],
            "created_at": t.get("created_at") or "",
            "method": "search",
        }
        if best is None or count > best["count"]:
            best = hit

    return best


def write_results_md(rows: list[dict]) -> None:
    tweet_n = sum(
        1
        for r in rows
        if (r.get("match_source") or "") in {"search", "global_search", "timeline"}
    )
    prof_n = sum(1 for r in rows if "profile" in str(r.get("match_source") or ""))

    def src_label(s: str) -> str:
        if s in {"search", "global_search", "timeline"}:
            return "ツイート"
        if "profile" in str(s):
            return "プロフィール（要確認）"
        return s or "?"

    lines = [
        "# 2025 年間ランキング結果",
        "",
        "PR 確認用スナップショット。根拠はリンクのみ（ツイート本文は載せない）。",
        "",
        "## 方針メモ",
        "",
        "- **年間10即以上のみ掲載**",
        "- 探索: ツイート由来（総括 Search / timeline）+ プロフィール由来",
        "- 同一アカウントはツイート証拠を優先。プロフィールのみは要確認",
        "- 除外: 月次総括・遠征短期合計・目標ツイート・即目カウント・応援リプ等",
        "- プロフィール: ライブbio再取得。節/g 単位も年間として採用",
        f"- ツイート由来 {tweet_n} / プロフィール由来 {prof_n}",
        "",
        "## 2025年（10即以上）",
        "",
        "| # | account | 表示名 | 即数 | 由来 | 根拠 |",
        "|---|---------|--------|-----:|------|------|",
    ]
    for i, r in enumerate(rows, 1):
        u = r.get("username") or ""
        name = (r.get("display_name") or "").replace("|", "\\|")
        c = int(r.get("yearly_count") or 0)
        src = src_label(r.get("match_source") or "")
        url = (
            r.get("source_url")
            or r.get("evidence_url")
            or r.get("tweet_url")
            or f"https://x.com/{u}"
        )
        lines.append(f"| {i} | @{u} | {name} | {c} | {src} | [link]({url}) |")
    Path("docs/results_2025.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote docs/results_2025.md", len(rows))


async def main_async() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--set",
        choices=["profile", "missing", "all"],
        default="all",
    )
    parser.add_argument("--only", default="")
    args = parser.parse_args()

    if args.only.strip():
        users = [u.strip().lstrip("@") for u in args.only.split(",") if u.strip()]
    elif args.set == "profile":
        users = PROFILE_PRIORITY
    elif args.set == "missing":
        users = MISSING_REGULARS
    else:
        # unique preserve order
        seen = set()
        users = []
        for u in PROFILE_PRIORITY + MISSING_REGULARS:
            k = u.lower()
            if k not in seen:
                seen.add(k)
                users.append(u)

    existing = load_json(YEARLY_PATH, [])
    results_map = {
        (r.get("username") or "").lower(): r for r in existing if r.get("username")
    }
    accounts = {
        (a.get("username") or "").lower(): a
        for a in load_json(ACCOUNTS, [])
        if a.get("username")
    }

    sessions = create_sessions()
    session_idx = [0]
    cookie_sets = load_playwright_cookie_sets()
    from playwright.async_api import async_playwright

    print(f"targets: {len(users)}")
    async with async_playwright() as p:
        browser, contexts = await create_playwright_contexts(
            p, cookie_sets, headless=True
        )
        ctx = contexts[0]["context"]
        for i, username in enumerate(users, 1):
            print(f"[{i}/{len(users)}] @{username}")
            hit = await find_yearly_for_user(
                ctx, sessions, session_idx, username
            )
            if not hit:
                print("  no yearly tweet hit")
                continue
            account = dict(accounts.get(username.lower(), {}))
            account.setdefault("username", username)
            if not account.get("display_name"):
                cur = results_map.get(username.lower()) or {}
                account["display_name"] = cur.get("display_name") or username
            row = build_period_result(
                account, hit, "yearly_count", hit.get("method") or "search"
            )
            key = username.lower()
            old = results_map.get(key)
            # prefer tweet over profile even if count slightly lower? No - prefer higher quality:
            # if old is profile and new is tweet with count>=10, prefer tweet if count within 30% or higher
            replace = should_replace_result(old, row, "yearly_count")
            if old and "profile" in str(old.get("match_source") or "") and hit["count"] >= 10:
                # upgrade to tweet evidence if new count is at least 50% of profile claim
                old_c = int(old.get("yearly_count") or 0)
                if hit["count"] >= max(10, int(old_c * 0.5)):
                    replace = True
                    print(
                        f"  upgrade profile->{hit['count']} tweet "
                        f"(was {old_c} profile)"
                    )
            if replace:
                results_map[key] = row
                print(f"  HIT {hit['count']} {hit['url']}")
            else:
                print(
                    f"  skip keep {old.get('yearly_count') if old else None} "
                    f"vs new {hit['count']}"
                )
        await browser.close()

    all_rows = [normalize_period_row(r) for r in results_map.values()]
    all_rows.sort(
        key=lambda r: (
            -int(r.get("yearly_count") or 0),
            (r.get("username") or "").lower(),
        )
    )
    save_json(YEARLY_PATH, all_rows)
    ranked = [r for r in all_rows if int(r.get("yearly_count") or 0) >= CUTOFF]
    write_results_md(ranked)
    print(f"done all={len(all_rows)} ge10={len(ranked)}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()