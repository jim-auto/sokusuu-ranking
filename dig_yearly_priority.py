# -*- coding: utf-8 -*-
"""API-only 2025 yearly deep dig for high-priority missing regulars.

Does not use 即目. Logs keyword candidates even when extract fails.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from monthly_collect import (
    build_period_result,
    create_sessions,
    extract_yearly_count,
    get_user_id,
    get_user_tweets,
    load_json,
    normalize_period_row,
    save_json,
    should_replace_result,
)
from yearly_deep_recaps import (
    CUTOFF,
    YEAR,
    YEARLY_PATH,
    tweet_in_year_window,
    write_results_md,
)

TARGETS = [
    "kukuru_nanpa",
    "Tt2tb",
    "makoto__pua",
]
ACCOUNTS = "data/sokusuu_accounts.json"
OUT_LOG = Path("data/yearly_priority_dig.json")
KEYWORD = re.compile(
    r"2025|25年|今年|本年|年間|年末|年報|総括|統括|着地|振り返り|まとめ"
)


def parse_dt(created_at: str) -> datetime | None:
    if not created_at:
        return None
    try:
        return datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return None


def dig_user(sessions, session_idx, username: str) -> dict:
    uid = get_user_id(sessions, session_idx, username)
    if not uid:
        print(f"  @{username} uid=None")
        return {"username": username, "uid": None, "tweets": 0, "best": None, "cands": []}

    tweets = get_user_tweets(sessions, session_idx, uid, count=50, max_pages=70)
    print(f"  @{username} uid={uid} tweets={len(tweets)}")
    if tweets:
        first = parse_dt(tweets[0].get("created_at") or "")
        last = parse_dt(tweets[-1].get("created_at") or "")
        print(f"    range {first} -> {last}")

    best = None
    cands = []
    for t in tweets:
        text = t.get("text") or ""
        if text.startswith("RT @") or text.startswith("RT@"):
            continue
        created = t.get("created_at") or ""
        if not KEYWORD.search(text):
            continue
        if not tweet_in_year_window(created, YEAR, text):
            continue
        # 即目は方針どおり使わない
        if re.search(r"\d+\s*即目", text) and not re.search(
            r"(?:総括|着地|年間|年末)", text
        ):
            cands.append(
                {
                    "id": t.get("id"),
                    "created_at": created,
                    "count": None,
                    "reason": "soku_me_skip",
                    "text": text[:240],
                }
            )
            continue
        count = extract_yearly_count(text, YEAR, strict=True)
        if not count:
            count = extract_yearly_count(text, YEAR, strict=False)
        url = f"https://x.com/{username}/status/{t.get('id')}"
        entry = {
            "id": t.get("id"),
            "url": url,
            "created_at": created,
            "count": count,
            "text": text[:280],
        }
        cands.append(entry)
        if not count or count < CUTOFF:
            continue
        if best is None or count > best["count"]:
            best = {
                "count": count,
                "url": url,
                "text": text[:500],
                "created_at": created,
                "method": "timeline",
            }
    return {
        "username": username,
        "uid": uid,
        "tweets": len(tweets),
        "best": best,
        "cands": cands,
    }


def main() -> None:
    sessions = create_sessions()
    print("sessions", len(sessions))
    if not sessions:
        raise SystemExit("no cookie sessions")
    session_idx = [0]
    accounts = {
        (a.get("username") or "").lower(): a
        for a in load_json(ACCOUNTS, [])
        if a.get("username")
    }
    existing = load_json(YEARLY_PATH, [])
    results_map = {
        (r.get("username") or "").lower(): r for r in existing if r.get("username")
    }

    report = []
    for username in TARGETS:
        print(f"[{username}]")
        dug = dig_user(sessions, session_idx, username)
        report.append(dug)
        best = dug.get("best")
        print(f"    cands={len(dug.get('cands') or [])} best={best}")
        if not best:
            continue
        account = dict(accounts.get(username.lower(), {}))
        account.setdefault("username", username)
        row = build_period_result(account, best, "yearly_count", "timeline")
        old = results_map.get(username.lower())
        if old and not should_replace_result(old, row, "yearly_count"):
            old_c = int(old.get("yearly_count") or 0)
            if best["count"] <= old_c:
                print(f"    keep {old_c}")
                continue
        results_map[username.lower()] = row
        print(f"    ADD {best['count']} {best['url']}")

    OUT_LOG.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
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
    print(f"wrote {OUT_LOG} all={len(all_rows)} ge10={len(ranked)}")


if __name__ == "__main__":
    main()
