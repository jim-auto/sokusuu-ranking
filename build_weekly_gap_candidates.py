"""週間ランキングの取りこぼし候補台帳を作る。"""
from __future__ import annotations

import argparse
from pathlib import Path

from monthly_collect import load_json, save_json

EXTRA_REGULARS = [
    "ot_aza", "taruchan100", "sugi_ichiban", "puro_nanpa", "training_pua",
    "shime_pua", "sandorafc", "oyasugaoo", "mostkkweek", "tora_maru005",
    "bookmaker_2015", "taka_DTnmp", "atannon_nampa", "sub_chilll",
]


def load_seeds() -> set[str]:
    path = Path("seed_accounts.txt")
    if not path.exists():
        return set()
    out = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line.lstrip("@").lower())
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weekly", default="data/weekly_2026-07-27_2026-08-02.json")
    parser.add_argument("--july-min", type=int, default=3)
    args = parser.parse_args()

    weekly = load_json(args.weekly, [])
    july = load_json("data/monthly_2026_07.json", [])
    seeds = load_seeds()
    weekly_map = {(r.get("username") or "").lower(): r for r in weekly if r.get("username")}
    candidates: dict[str, dict] = {}

    def ensure(u: str) -> dict:
        key = u.lower()
        if key not in candidates:
            candidates[key] = {
                "username": u,
                "july_count": None,
                "weekly_count": int(weekly_map[key].get("weekly_count") or 0) if key in weekly_map else 0,
                "display_name": (weekly_map.get(key) or {}).get("display_name") or "",
                "in_seed": key in seeds,
                "in_weekly": key in weekly_map,
                "sources": [],
                "priority": 0,
                "status": "unknown",
            }
        return candidates[key]

    for r in july:
        u = r.get("username") or ""
        if not u:
            continue
        c = int(r.get("monthly_count") or 0)
        if c < args.july_min:
            continue
        row = ensure(u)
        row["july_count"] = c
        row["display_name"] = row["display_name"] or (r.get("display_name") or "")
        row["sources"].append("july_monthly")
        row["priority"] += c

    for u in EXTRA_REGULARS:
        row = ensure(u)
        if "extra_regular" not in row["sources"]:
            row["sources"].append("extra_regular")
        row["priority"] += 5

    for key, row in candidates.items():
        wc = int(row.get("weekly_count") or 0)
        jc = row.get("july_count")
        if row["in_weekly"] and wc >= 2:
            row["status"] = "in_weekly_ok"
            row["priority"] = -1
        elif row["in_weekly"] and wc == 1:
            row["status"] = "hit_under_min"
        elif not row["in_seed"]:
            row["status"] = "not_seeded"
        elif jc and jc >= 5:
            row["status"] = "seeded_no_or_low_hit"
        else:
            row["status"] = "seeded_gap"

    gap = [r for r in candidates.values() if r["status"] != "in_weekly_ok"]
    gap.sort(key=lambda r: (-int(r.get("july_count") or 0), -int(r.get("priority") or 0), (r.get("username") or "").lower()))

    out_json = Path("data/weekly_gap_candidates.json")
    out_md = Path("docs/weekly_gap_candidates.md")
    save_json(str(out_json), gap)

    lines = [
        "# 週間ランキング取りこぼし候補",
        "",
        f"生成元 weekly: `{args.weekly}`",
        f"7月しきい値: monthly_count >= {args.july_min}",
        f"候補数: **{len(gap)}**（週間 min>=2 以外）",
        "",
        "## 優先トップ30",
        "",
        "| # | account | 7月 | 週間 | seed | status | 表示名 |",
        "|---|---------|----:|-----:|:----:|--------|--------|",
    ]
    for i, r in enumerate(gap[:30], 1):
        jc = r.get("july_count") if r.get("july_count") is not None else "-"
        lines.append(
            f"| {i} | @{r['username']} | {jc} | {r.get('weekly_count') or 0} | "
            f"{'Y' if r.get('in_seed') else 'N'} | {r.get('status')} | {(r.get('display_name') or '')[:24]} |"
        )
    lines += [
        "",
        "## only 用コピペ（優先20）",
        "",
        "```",
        ",".join(r["username"] for r in gap[:20]),
        "```",
        "",
    ]
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out_json} ({len(gap)})")
    print(f"wrote {out_md}")
    print("top20:", ",".join(r["username"] for r in gap[:20]))


if __name__ == "__main__":
    main()
