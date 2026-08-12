# -*- coding: utf-8 -*-
"""Merge verified in-week case tweets, restack, and refresh weekly docs."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from weekly_collect import (
    dedupe_weekly_rows,
    stack_weekly_from_tweets,
    write_weekly_markdown,
)

PATH = Path("data/weekly_2026-07-27_2026-08-02.json")
ACCOUNTS = Path("data/sokusuu_accounts.json")
START, END = date(2026, 7, 27), date(2026, 8, 2)
MIN_COUNT = 1

# 人手確認済みの週内ケース（検出漏れパターン）
EXTRA_TWEETS = {
    "greed_pua": [
        {
            "id": "2081700412382720358",
            "text": "🍐そ19🦏夜職📮狂\nカルバンクラインえろかった",
            "created_at": "Mon Jul 27 11:17:09 +0000 2026",
        },
        {
            "id": "2081777852773765367",
            "text": "🪩そ21🦏タトゥーネキ\nセク値過去一ぐらい高くて大満足",
            "created_at": "Mon Jul 27 16:24:52 +0000 2026",
        },
        {
            "id": "2082065296500949214",
            "text": "🍐そ21🦏🎯🍾店員\nリーセグダあったけどなんとかいけた",
            "created_at": "Tue Jul 28 11:27:04 +0000 2026",
        },
    ],
    "mic_pua": [
        {
            "id": "2082118244111155425",
            "text": "久々復帰🎤\n本日🗼🍛即×2‼️\n\n2人ともシコい\nマインド回復❤️‍🩹",
            "created_at": "Tue Jul 28 14:57:27 +0000 2026",
        },
    ],
    "PUAINOKI": [
        {
            "id": "2082450543893708925",
            "text": "TAKAMIさんパスそあざす‼️",
            "created_at": "Wed Jul 29 12:57:54 +0000 2026",
        },
    ],
    "kent_o_o": [
        {
            "id": "2081561013099598103",
            "text": "KBK某ちょんそ\n\n某るんさんあざす\n\nKBKちょんそしかしてない\nガチ弱い",
            "created_at": "Mon Jul 27 02:03:13 +0000 2026",
        },
        {
            "id": "2084003852031033565",
            "text": "🧚乞食ちょんそ\n流石に2即目いく",
            "created_at": "Sun Aug 02 19:50:11 +0000 2026",
        },
    ],
    "yomaru_street": [
        {
            "id": "2083929728067756212",
            "text": "39🦏/来多ー/タイプ値5.5/🦐\n\n駅前でシコスレンダー🉐発見→ダッシュkkし飲み行こ！で🐶🚕搬したら搬まさかの39\nどこいくの言われたので下心開示→ユーモアと引きで直🏩成功🩸グダ破壊NS即",
            "created_at": "Sun Aug 02 14:55:39 +0000 2026",
        },
    ],
    "nakayamasoku": [
        {
            "id": "2083906930377170998",
            "text": "ひっさびさにapp即\n新規即気持ちええんじゃあ",
            "created_at": "Sun Aug 02 13:25:03 +0000 2026",
        },
    ],
    "kannen170": [
        {
            "id": "2083244499749523628",
            "text": "本日2即！ 🎩いくぜ！",
            "created_at": "Fri Jul 31 17:32:00 +0000 2026",
        },
    ],
}


def account_meta(accounts: list[dict], username: str) -> dict:
    key = username.lower()
    for a in accounts:
        if (a.get("username") or "").lower() == key:
            return a
    return {"username": username}


def tweets_from_row(row: dict) -> list[dict]:
    seen = set()
    tweets = []
    for e in row.get("evidence_items") or []:
        tid = str(e.get("tweet_id") or "")
        if not tid and e.get("url"):
            tid = str(e["url"]).rstrip("/").rsplit("/", 1)[-1]
        if not tid or tid in seen:
            continue
        seen.add(tid)
        tweets.append(
            {
                "id": tid,
                "text": e.get("text") or "",
                "created_at": e.get("created_at") or "",
                "is_quote": e.get("is_quote", False),
            }
        )
    if row.get("tweet_text") and row.get("tweet_url"):
        tid = str(row["tweet_url"]).rstrip("/").rsplit("/", 1)[-1]
        if tid and tid not in seen:
            tweets.append(
                {
                    "id": tid,
                    "text": row.get("tweet_text") or "",
                    "created_at": row.get("tweet_created_at") or "",
                }
            )
    return tweets


def apply_hit(row: dict, hit: dict) -> dict:
    row = dict(row)
    ev = (hit.get("evidence_items") or [{}])[0]
    row["weekly_count"] = hit["count"]
    row["count"] = hit["count"]
    row["count_method"] = hit.get("method") or "stack"
    row["match_source"] = hit.get("method") or "stack"
    row["stack_posts"] = hit.get("stack_posts")
    row["stack_summary"] = hit.get("stack_summary")
    row["evidence_items"] = hit.get("evidence_items") or []
    row["tweet_url"] = hit.get("url") or ev.get("url") or row.get("tweet_url")
    row["source_url"] = row["tweet_url"]
    row["evidence_url"] = row["tweet_url"]
    row["tweet_text"] = hit.get("text") or ev.get("text") or row.get("tweet_text")
    row["tweet_created_at"] = hit.get("created_at") or ev.get("created_at") or ""
    row["source_type"] = "tweet_evidence"
    row["needs_review"] = False
    row["period_start"] = START.isoformat()
    row["period_end"] = END.isoformat()
    row["period_label"] = "7/27〜8/2"
    return row


def main() -> None:
    rows = json.loads(PATH.read_text(encoding="utf-8"))
    accounts = json.loads(ACCOUNTS.read_text(encoding="utf-8")) if ACCOUNTS.exists() else []
    by_user = {}
    for r in rows:
        username = r.get("username") or ""
        key = username.lower()
        if (r.get("count_method") or r.get("match_source")) == "explicit":
            by_user[key] = r
            continue
        tweets = tweets_from_row(r)
        if not tweets:
            by_user[key] = r
            continue
        hit = stack_weekly_from_tweets(tweets, username, START, END)
        if hit and int(hit.get("count") or 0) >= MIN_COUNT:
            new_row = apply_hit(r, hit)
            if new_row["weekly_count"] != r.get("weekly_count"):
                print(f"  restack @{username}: {r.get('weekly_count')} -> {new_row['weekly_count']}")
            by_user[key] = new_row
        else:
            by_user[key] = r

    for username, extras in EXTRA_TWEETS.items():
        key = username.lower()
        row = by_user.get(key) or {
            **account_meta(accounts, username),
            "username": username,
        }
        tweets = tweets_from_row(row)
        seen = {str(t.get("id")) for t in tweets}
        for t in extras:
            if str(t["id"]) not in seen:
                tweets.append(t)
                seen.add(str(t["id"]))
        hit = stack_weekly_from_tweets(tweets, username, START, END)
        if not hit or int(hit.get("count") or 0) < MIN_COUNT:
            print(f"  skip @{username}: {hit}")
            continue
        new_row = apply_hit(row, hit)
        old = by_user.get(key, {}).get("weekly_count")
        print(f"  @{username}: {old} -> {new_row['weekly_count']}")
        by_user[key] = new_row

    new_rows = list(by_user.values())
    new_rows = dedupe_weekly_rows(new_rows)
    new_rows = [r for r in new_rows if int(r.get("weekly_count") or 0) >= MIN_COUNT]
    new_rows.sort(
        key=lambda x: (
            -int(x.get("weekly_count") or 0),
            -(x.get("followers_count") or 0),
            (x.get("username") or "").lower(),
        )
    )
    PATH.write_text(json.dumps(new_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_weekly_markdown(new_rows, START, END)
    ge2 = sum(1 for r in new_rows if int(r.get("weekly_count") or 0) >= 2)
    print(f"done rows={len(new_rows)} ge2={ge2} -> {PATH}")


if __name__ == "__main__":
    main()
