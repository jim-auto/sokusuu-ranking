# -*- coding: utf-8 -*-
"""Merge verified 2025 yearly recaps and refresh docs/results_2025.md."""
from __future__ import annotations

import json
from pathlib import Path

from monthly_collect import (
    build_period_result,
    extract_yearly_count,
    load_json,
    normalize_period_row,
    save_json,
    should_replace_result,
)
from yearly_deep_recaps import CUTOFF, YEARLY_PATH, write_results_md

ACCOUNTS = "data/sokusuu_accounts.json"

VERIFIED = [
    {
        "username": "sugi_ichiban",
        "display_name": "杉山先生@一番星プロジェクト",
        "count": 92,
        "url": "https://x.com/sugi_ichiban/status/2006286163762679905",
        "text": "【2025年総括】92即\n🔥46 🌹15🥥13 🍐8🍎8 🟥1📦1\n1月15即\n2月9即→講習期間終了\n3月10即",
        "created_at": "Wed Dec 31 08:47:51 +0000 2025",
    },
    {
        "username": "shin9suke",
        "display_name": "しんすけ@侍長期(受講済)×wing中期(受講済）",
        "count": 57,
        "url": "https://x.com/shin9suke/status/2062574409320374313",
        "text": "【2025年総括】\n57即(🐶19🦁8🦉3📦17遠征10(パス1))\n※今更ですが",
        "created_at": "Thu Jun 04 16:37:14 +0000 2026",
    },
    {
        "username": "okarun_pua",
        "display_name": "おかるん@りお講習",
        "count": 155,
        "url": "https://x.com/okarun_pua/status/2006307712574070954",
        "text": "【2025年総括】\n\nスト　🦉🦁　64即\nネト　🍎🔥　10即\n箱　　🧚‍♂️🦾　81即\n計　　　　　　155即",
        "created_at": "Wed Dec 31 10:13:28 +0000 2025",
    },
    {
        "username": "makoto__pua",
        "display_name": "伊藤誠@講習、note始めました",
        "count": 30,
        "url": "https://x.com/makoto__pua/status/2006386354196971778",
        "text": "2025年スト開始からのトータル\n3月 2即\n4月 2即\n5月 0即\n6月 0即\n7月 3即\n8月 4即\n9月 2即\n10月 5即\n11月 2即\n12月 10即",
        "created_at": "Wed Dec 31 15:25:58 +0000 2025",
    },
]


def main() -> None:
    existing = load_json(YEARLY_PATH, [])
    results_map = {
        (r.get("username") or "").lower(): r for r in existing if r.get("username")
    }
    accounts = {
        (a.get("username") or "").lower(): a
        for a in load_json(ACCOUNTS, [])
        if a.get("username")
    }

    for item in VERIFIED:
        username = item["username"]
        parsed = extract_yearly_count(item["text"], 2025, strict=False)
        if parsed != item["count"]:
            raise SystemExit(
                f"extractor mismatch @{username}: {parsed} != {item['count']}"
            )
        account = dict(accounts.get(username.lower(), {}))
        account.setdefault("username", username)
        account.setdefault("display_name", item["display_name"])
        hit = {
            "count": item["count"],
            "url": item["url"],
            "text": item["text"],
            "created_at": item["created_at"],
        }
        row = build_period_result(
            account, hit, "yearly_count", item.get("match_source") or "search"
        )
        old = results_map.get(username.lower())
        if old and not should_replace_result(old, row, "yearly_count"):
            # still upgrade if old is lower or profile-only
            old_c = int(old.get("yearly_count") or 0)
            if item["count"] <= old_c and "profile" not in str(old.get("match_source") or ""):
                print(f"  keep @{username} {old_c}")
                continue
        results_map[username.lower()] = row
        print(
            f"  @{username}: {old.get('yearly_count') if old else None} -> {item['count']}"
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
    print(f"done all={len(all_rows)} ge10={len(ranked)}")


if __name__ == "__main__":
    main()
