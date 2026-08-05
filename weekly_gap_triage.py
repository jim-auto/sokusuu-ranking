# -*- coding: utf-8 -*-
"""ギャップ候補の簡易仕分け（週内ツイ + stack 結果）。"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path

from monthly_collect import (
    create_playwright_contexts,
    create_sessions,
    load_json,
    load_playwright_cookie_sets,
)
from weekly_collect import (
    collect_user_tweets_for_week,
    count_stack_units,
    extract_day_recap_n,
    is_goal_or_progress_tracker,
    is_meta_or_third_party_soku_talk,
    is_period_recap_tweet,
    last_week_of_month,
    parse_tweet_datetime,
    stack_weekly_from_tweets,
)

DEFAULT_USERS = [
    "tora_maru005",
    "kuroiwa_45",
    "Lattie_pua",
    "PUAINOKI",
    "socool55555",
    "mic_pua",
    "nakayamasoku",
    "tomu_riddle",
    "greed_pua",
    "bookmaker_2015",
    "rei_app_pua",
    "cx_lm5",
]


def classify(tweets, hit, start: date, end: date) -> tuple[str, str]:
    week_tweets = []
    for t in tweets:
        d = parse_tweet_datetime(t.get("created_at", ""))
        if d and start <= d <= end:
            week_tweets.append(t)

    texts = [(t.get("text") or "") for t in week_tweets]
    samples = []
    for t in texts[:12]:
        one = t.replace("\n", " ")[:80]
        u = count_stack_units(t)
        samples.append(f"[{u}] {one}")

    count = int((hit or {}).get("count") or 0)
    if not week_tweets:
        return "fetch_gap", "週内ツイ取得0件（鍵/検索漏れの可能性）"

    # any hard case signal ignored by rules?
    signals = 0
    excludeish = 0
    for t in texts:
        if is_period_recap_tweet(t) or is_goal_or_progress_tracker(t) or is_meta_or_third_party_soku_talk(t):
            excludeish += 1
        if count_stack_units(t) > 0 or extract_day_recap_n(t):
            signals += 1
        # heuristic real case words without count
        if any(k in t for k in ("即‼️", "準即/", "即/", "パス即", "満即", "即った", "しょのち", "そー")):
            if count_stack_units(t) <= 0 and not is_period_recap_tweet(t):
                return "rule_gap", "ケースっぽい語があるが stack=0 → 判定不足の可能性; " + " / ".join(samples[:3])

    if count >= 2:
        return "in_weekly_ok", f"stack={count}"
    if count == 1:
        return "week_low", f"週1のみ stack=1; " + " / ".join(samples[:3])
    if signals == 0 and excludeish >= max(1, len(texts) // 3):
        return "exclude", "進捗・理論・総括寄り; " + " / ".join(samples[:3])
    if signals == 0:
        return "week_low", "週内に即語ほぼなし; " + " / ".join(samples[:3])
    return "rule_gap", f"signals={signals} stack={count}; " + " / ".join(samples[:3])


async def main_async() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", default=",".join(DEFAULT_USERS))
    parser.add_argument("--deep", action="store_true", default=True)
    args = parser.parse_args()
    users = [u.strip().lstrip("@") for u in args.users.split(",") if u.strip()]
    start, end = last_week_of_month(2026, 7)
    july = {
        (r.get("username") or "").lower(): int(r.get("monthly_count") or 0)
        for r in load_json("data/monthly_2026_07.json", [])
    }

    sessions = create_sessions()
    session_idx = [0]
    cookie_sets = load_playwright_cookie_sets()
    from playwright.async_api import async_playwright

    rows = []
    async with async_playwright() as p:
        browser, contexts = await create_playwright_contexts(
            p, cookie_sets, headless=True
        )
        ctx = contexts[0]["context"]
        for i, username in enumerate(users, 1):
            print(f"[{i}/{len(users)}] @{username}")
            tweets = await collect_user_tweets_for_week(
                ctx, sessions, session_idx, username, start, end, deep=args.deep
            )
            hit = stack_weekly_from_tweets(tweets, username, start, end)
            label, note = classify(tweets, hit, start, end)
            rows.append(
                {
                    "username": username,
                    "july": july.get(username.lower()),
                    "stack": int((hit or {}).get("count") or 0),
                    "tweets": len(tweets),
                    "label": label,
                    "note": note,
                }
            )
            print(f"  -> {label} stack={rows[-1]['stack']} tweets={len(tweets)}")
        await browser.close()

    lines = [
        "# 週間ギャップ仕分け（7/27〜8/2）",
        "",
        "ラベル: `fetch_gap` / `rule_gap` / `week_low` / `exclude` / `in_weekly_ok`",
        "",
        "| account | 7月 | stack | 取得 | label | note |",
        "|---------|----:|------:|-----:|--------|------|",
    ]
    for r in rows:
        note = (r["note"] or "").replace("|", "/")[:120]
        lines.append(
            f"| @{r['username']} | {r['july'] if r['july'] is not None else '-'} | "
            f"{r['stack']} | {r['tweets']} | {r['label']} | {note} |"
        )
    lines += ["", "## 次アクション要約", ""]
    by = {}
    for r in rows:
        by.setdefault(r["label"], []).append(r["username"])
    for k, vs in sorted(by.items()):
        lines.append(f"- **{k}**: " + ", ".join(f"@{u}" for u in vs))
    out = Path("docs/weekly_gap_triage.md")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote", out)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()