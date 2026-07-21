"""
月次ランキングを合算して上半期（1-6月）ランキングを作る。

例:
  python build_halfyear_ranking.py --year 2026
  -> data/yearly_2026.json
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict


def load_monthly(year: int, month: int) -> list[dict]:
    path = f"data/monthly_{year}_{month:02d}.json"
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def build_halfyear_ranking(year: int, months: list[int] | None = None) -> list[dict]:
    if months is None:
        months = list(range(1, 7))

    by_user: dict[str, dict] = defaultdict(
        lambda: {
            "months": {},
            "total": 0,
            "meta": None,
            "best_evidence": None,
            "best_month_count": -1,
            "month_rows": {},  # month -> row (for channel inference)
        }
    )

    used_months = []
    for month in months:
        rows = load_monthly(year, month)
        if not rows:
            print(f"  skip: monthly_{year}_{month:02d}.json なし")
            continue
        used_months.append(month)
        for row in rows:
            username = row.get("username")
            if not username:
                continue
            count = int(row.get("monthly_count") or 0)
            if count <= 0:
                continue

            entry = by_user[username]
            prev = entry["months"].get(month)
            if prev is not None and count <= prev:
                continue
            if prev is not None:
                entry["total"] -= prev
            entry["months"][month] = count
            entry["total"] += count
            entry["month_rows"][month] = row

            followers = int(row.get("followers_count") or 0)
            meta = entry["meta"]
            if meta is None or followers >= int(meta.get("followers_count") or 0):
                entry["meta"] = row

            if count > entry["best_month_count"]:
                entry["best_month_count"] = count
                entry["best_evidence"] = (
                    row.get("evidence_url")
                    or row.get("tweet_url")
                    or row.get("source_url")
                    or ""
                )

    # 月次本文からチャネルを合算（generate_html.infer_channels と揃える）
    try:
        from generate_html import infer_channels
    except Exception:
        infer_channels = None

    ranking = []
    for username, entry in by_user.items():
        if entry["total"] <= 0:
            continue
        meta = entry["meta"] or {}
        breakdown = entry["months"]
        parts = [f"{m}月{breakdown[m]}即" for m in sorted(breakdown)]
        text = f"{year}年上半期 合算{entry['total']}即（" + " / ".join(parts) + "）"

        # 各月の総括本文を連結してチャネル判定用にする
        evidence_parts = []
        channel_set: list[str] = []
        for m in sorted(entry["month_rows"]):
            row = entry["month_rows"][m]
            t = (row.get("tweet_text") or "").strip()
            if t:
                evidence_parts.append(t)
            if infer_channels is not None:
                for ch in infer_channels(row):
                    if ch not in channel_set and ch != "unknown":
                        channel_set.append(ch)
        channel_evidence = "\n".join(evidence_parts)
        if not channel_set and infer_channels is not None:
            channel_set = [
                c
                for c in infer_channels(
                    {
                        "tweet_text": channel_evidence,
                        "categories": meta.get("categories", ""),
                        "bio": meta.get("bio", ""),
                        "display_name": meta.get("display_name", ""),
                    }
                )
            ]

        ranking.append(
            {
                "username": username,
                "display_name": meta.get("display_name", ""),
                "yearly_count": entry["total"],
                "tweet_url": entry["best_evidence"] or f"https://x.com/{username}",
                "source_url": entry["best_evidence"] or f"https://x.com/{username}",
                "evidence_url": entry["best_evidence"] or "",
                "tweet_text": text,
                "channel_evidence": channel_evidence,
                "channels": channel_set,
                "tweet_created_at": "",
                "followers_count": meta.get("followers_count", 0),
                "categories": meta.get("categories", ""),
                "profile_image_url": meta.get("profile_image_url", ""),
                "match_source": "monthly_sum_h1",
                "period": "h1",
                "period_label": "上半期",
                "month_breakdown": {str(k): v for k, v in sorted(breakdown.items())},
                "source_months": used_months,
                "needs_review": False,
                "source_type": "monthly_aggregate",
            }
        )

    ranking.sort(key=lambda r: (-r["yearly_count"], r["username"].lower()))
    return ranking


def main() -> None:
    parser = argparse.ArgumentParser(description="上半期ランキング（月次合算）を生成")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--months",
        default="1-6",
        help="合算する月（例: 1-6）。既定は上半期",
    )
    parser.add_argument(
        "--output",
        help="出力先（既定: data/yearly_YYYY.json）",
    )
    args = parser.parse_args()

    if "-" in args.months:
        start_s, end_s = args.months.split("-", 1)
        months = list(range(int(start_s), int(end_s) + 1))
    else:
        months = [int(x) for x in args.months.split(",") if x.strip()]

    print(f"{args.year}年 上半期ランキング生成（月次合算: {months}）")
    ranking = build_halfyear_ranking(args.year, months)
    out = args.output or f"data/yearly_{args.year}.json"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(ranking, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"{len(ranking)}件 -> {out}")
    for i, row in enumerate(ranking[:15], 1):
        name = row.get("display_name") or "-"
        print(f"  {i}. @{row['username']} ({name}): {row['yearly_count']}即")
    if len(ranking) > 15:
        print(f"  ... +{len(ranking) - 15} more")


if __name__ == "__main__":
    main()
