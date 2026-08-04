"""Offline re-score weekly stack results after count_stack_units rule changes."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from weekly_collect import stack_weekly_from_tweets, write_weekly_markdown

PATH = Path("data/weekly_2026-07-27_2026-08-02.json")
START, END = date(2026, 7, 27), date(2026, 8, 2)
MIN_COUNT = 2


def main() -> None:
    rows = json.loads(PATH.read_text(encoding="utf-8"))
    new_rows: list[dict] = []

    for r in rows:
        method = r.get("count_method") or r.get("match_source")
        username = r.get("username") or ""
        if method == "explicit":
            new_rows.append(r)
            continue

        items = r.get("evidence_items") or []
        tweets = [
            {
                "id": e.get("tweet_id") or e.get("url", "").rsplit("/", 1)[-1],
                "text": e.get("text") or "",
                "created_at": e.get("created_at") or "",
                "is_quote": e.get("is_quote", False),
            }
            for e in items
            if e.get("text") or e.get("role") == "explicit_total"
        ]
        if not tweets:
            continue

        hit = stack_weekly_from_tweets(tweets, username, START, END)
        if not hit or int(hit.get("count") or 0) < MIN_COUNT:
            print(f"  OUT @{username}: {r.get('weekly_count')} -> {hit.get('count') if hit else 0}")
            continue

        r2 = dict(r)
        old = r2.get("weekly_count")
        r2["weekly_count"] = hit["count"]
        r2["count"] = hit["count"]
        r2["stack_posts"] = hit.get("stack_posts")
        r2["evidence_items"] = hit.get("evidence_items") or []
        r2["stack_summary"] = hit.get("stack_summary")
        r2["count_method"] = "stack"
        if old != hit["count"]:
            print(f"  @{username}: {old} -> {hit['count']}")
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
