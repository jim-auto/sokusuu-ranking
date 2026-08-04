"""Offline re-score weekly stack results after count_stack_units rule changes."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from weekly_collect import count_stack_units, write_weekly_markdown

PATH = Path("data/weekly_2026-07-27_2026-08-02.json")
START, END = date(2026, 7, 27), date(2026, 8, 2)
MIN_COUNT = 2


def main() -> None:
    rows = json.loads(PATH.read_text(encoding="utf-8"))
    new_rows: list[dict] = []

    for r in rows:
        method = r.get("count_method") or r.get("match_source")
        if method == "explicit":
            new_rows.append(r)
            continue

        items = r.get("evidence_items") or []
        kept = []
        total = 0
        username = r.get("username")
        for e in items:
            text = e.get("text") or ""
            units = count_stack_units(text)
            if units <= 0:
                preview = text.replace("\n", " ")[:40]
                print(f"  drop @{username}: {preview!r}")
                continue
            e2 = dict(e)
            e2["units"] = units
            kept.append(e2)
            total += units

        if total < MIN_COUNT:
            print(f"  OUT @{username}: {r.get('weekly_count')} -> {total} (posts {len(kept)})")
            continue

        r2 = dict(r)
        old = r2.get("weekly_count")
        r2["weekly_count"] = total
        r2["count"] = total
        r2["stack_posts"] = len(kept)
        r2["evidence_items"] = kept
        r2["stack_summary"] = f"積み上げ{total}（報告{len(kept)}投稿）"
        r2["count_method"] = "stack"
        if old != total:
            print(f"  @{username}: {old} -> {total} (posts {len(kept)})")
        new_rows.append(r2)

    new_rows.sort(
        key=lambda x: (
            -int(x.get("weekly_count") or 0),
            -(x.get("followers_count") or 0),
            (x.get("username") or "").lower(),
        )
    )
    PATH.write_text(json.dumps(new_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_weekly_markdown(new_rows, START, END)
    print("---")
    print(f"{len(new_rows)} rows -> {PATH}")
    for i, r in enumerate(new_rows[:25], 1):
        print(f"  {i}. @{r['username']}: {r['weekly_count']} [{r.get('count_method')}]")


if __name__ == "__main__":
    main()
