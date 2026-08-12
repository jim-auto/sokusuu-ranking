# -*- coding: utf-8 -*-
"""Merge manual yearly hits into yearly_2025.json and refresh docs."""
from __future__ import annotations

import json
from pathlib import Path

from monthly_collect import (
    build_period_result,
    load_json,
    normalize_period_row,
    save_json,
    should_replace_result,
)

YEARLY_PATH = "data/yearly_2025.json"
ACCOUNTS = "data/sokusuu_accounts.json"
CUTOFF = 10

# Verified hits from X search / collect (2025 yearly)
MANUAL_HITS = [
    {
        "username": "tora_maru005",
        "count": 331,
        "url": "https://x.com/tora_maru005/status/2006631147825147967",
        "text": "2025年総括　331即🐯 🐶スト130 🦾🧚‍♀️100 その他遠征、🦁、アプリ、飲み100程",
        "created_at": "Thu Jan 01 07:38:41 +0000 2026",
        "method": "search",
        "display_name": "とらまる@ナンパ",
    },
    {
        "username": "greed_pua",
        "count": 22,
        "url": "https://x.com/greed_pua/status/1995317035895849127",
        "text": "",  # keep existing if better
        "method": "search",
    },
    {
        "username": "komanyan28",
        "count": 10,
        "url": "https://x.com/komanyan28",
        "method": "profile_bio",
    },
]


def source_label(src: str) -> str:
    if src in {"search", "global_search", "timeline", "stack"}:
        return "ツイート"
    if "profile" in str(src):
        return "プロフィール（要確認）"
    return src or "?"


def write_results_md(rows: list[dict]) -> None:
    tweet_n = sum(
        1
        for r in rows
        if (r.get("match_source") or "")
        in {"search", "global_search", "timeline"}
    )
    prof_n = sum(1 for r in rows if "profile" in str(r.get("match_source") or ""))
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
        "| # | account | 表示名 | 即数 | カテゴリ | 由来 | 根拠 |",
        "|---|---------|--------|-----:|----------|------|------|",
    ]
    for i, r in enumerate(rows, 1):
        u = r.get("username") or ""
        name = (r.get("display_name") or "").replace("|", "\\|")
        c = int(r.get("yearly_count") or 0)
        # channel category heuristic from existing fields
        ch = r.get("channel") or r.get("category") or "未分類"
        if isinstance(ch, list):
            ch = "/".join(ch) if ch else "未分類"
        src = source_label(r.get("match_source") or "")
        url = (
            r.get("source_url")
            or r.get("evidence_url")
            or r.get("tweet_url")
            or f"https://x.com/{u}"
        )
        lines.append(
            f"| {i} | @{u} | {name} | {c} | {ch} | {src} | [link]({url}) |"
        )
    Path("docs/results_2025.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    print("wrote docs/results_2025.md", len(rows))


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

    for hit in MANUAL_HITS:
        u = hit["username"]
        key = u.lower()
        account = dict(accounts.get(key, {}))
        account.setdefault("username", u)
        if hit.get("display_name"):
            account["display_name"] = hit["display_name"]
        result = {
            "count": hit["count"],
            "url": hit["url"],
            "text": hit.get("text") or "",
            "created_at": hit.get("created_at") or "",
        }
        row = build_period_result(
            account, result, "yearly_count", hit.get("method") or "search"
        )
        if should_replace_result(results_map.get(key), row, "yearly_count"):
            results_map[key] = row
            print(f"  merge @{u}: {hit['count']}")

    rows = [normalize_period_row(r) for r in results_map.values()]
    rows = [r for r in rows if int(r.get("yearly_count") or 0) >= CUTOFF]
    rows.sort(
        key=lambda r: (
            -int(r.get("yearly_count") or 0),
            -(r.get("followers_count") or 0),
            (r.get("username") or "").lower(),
        )
    )
    # keep full json with sub-cutoff too for debugging - write full map then filtered for ranking
    all_rows = [normalize_period_row(r) for r in results_map.values()]
    all_rows.sort(
        key=lambda r: (
            -int(r.get("yearly_count") or 0),
            (r.get("username") or "").lower(),
        )
    )
    save_json(YEARLY_PATH, all_rows)
    write_results_md(rows)
    print(f"yearly_2025.json all={len(all_rows)} ge10={len(rows)}")


if __name__ == "__main__":
    main()