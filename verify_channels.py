#!/usr/bin/env python3
"""月次ランキングのチャネル（スト/ネト/箱/その他/謎）を検証する。

総括ツイート本文から拾えるヒントと、JSON に保存された channels /
generate_html.infer_channels の結果を突き合わせて、
過大付与・取りこぼし・根拠なし を列挙する。

Usage:
  python verify_channels.py --file data/monthly_2026_07.json
  python verify_channels.py --year 2026 --month 7 --min-count 5
  python verify_channels.py --file data/monthly_2026_07.json --json-out data/channel_verify_2026_07.json
  python verify_channels.py --file data/monthly_2026_07.json --strict
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

from generate_html import (
    CATEGORY_LABELS,
    CHANNEL_ORDER,
    CHANNEL_OVERRIDES,
    CLUB_HINTS,
    ONLINE_HINTS,
    OTHER_HINTS,
    STREET_HINTS,
    _scan_channel_text,
    infer_channels,
    split_csv,
)

HINT_PATTERNS: dict[str, re.Pattern[str]] = {
    "street": STREET_HINTS,
    "online": ONLINE_HINTS,
    "club": CLUB_HINTS,
    "other": OTHER_HINTS,
}

# 人手で見ると「チャネル語」だが既存 regex が拾えない・拾いすぎる例をレビュー補助用に出す
SOFT_HINTS: dict[str, re.Pattern[str]] = {
    "street": re.compile(
        r"弾丸|弾(?:×|ｘ|x|/|即|\d)|準(?:即|×|ｘ|x|/|\d)|店連れ|路上|出撃|"
        r"声かけ|スト準|丘スト|完ソロ|SGT|MGT|脚馬|"
        r"[🐶🦁🦉🏪]",
        re.IGNORECASE,
    ),
    "online": re.compile(
        r"(?<![a-z])(?:APP|app)(?![a-z])|ネト新規|ネト改善|マチアプ|アプリ|"
        r"タプ|Tin|tinder|with|ペアーズ|東カレ|某app|"
        r"[🗼🍛🍎🔥🍐]",
        re.IGNORECASE,
    ),
    "club": re.compile(
        r"箱|クラ(?:ブ|ナン)|はこ|相席|オリラジ|"
        r"[🦾🧚📦🟦⬛⬜◼◾▪◻]|Ⓜ",
        re.IGNORECASE,
    ),
    "other": re.compile(r"パス|代打|アテンド|くるくる|ハイエナ|指名|カップル", re.IGNORECASE),
}


def label(channels: list[str]) -> str:
    if not channels:
        return "-"
    return "/".join(CATEGORY_LABELS.get(c, c) for c in CHANNEL_ORDER if c in channels)


def normalize_channels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = [str(x).strip() for x in value if str(x).strip()]
    elif isinstance(value, str):
        raw = split_csv(value)
    else:
        return []
    out: list[str] = []
    for item in raw:
        key = item.lower()
        # 日本語ラベルも受ける
        for en, ja in CATEGORY_LABELS.items():
            if en == "all":
                continue
            if key in {en, ja}:
                key = en
                break
        if key in CHANNEL_ORDER and key not in out:
            out.append(key)
    return [c for c in CHANNEL_ORDER if c in out]


def find_match_snippets(text: str, pattern: re.Pattern[str], limit: int = 5) -> list[str]:
    if not text:
        return []
    snippets: list[str] = []
    for m in pattern.finditer(text):
        start = max(0, m.start() - 8)
        end = min(len(text), m.end() + 8)
        snip = text[start:end].replace("\n", " ")
        snippets.append(snip)
        if len(snippets) >= limit:
            break
    return snippets


def soft_scan(text: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for channel, pattern in SOFT_HINTS.items():
        snips = find_match_snippets(text, pattern)
        if snips:
            hits[channel] = snips
    return hits


def hard_scan_details(text: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for channel, pattern in HINT_PATTERNS.items():
        snips = find_match_snippets(text, pattern)
        if snips:
            hits[channel] = snips
    return hits


def without_explicit_channels(row: dict) -> dict:
    bare = dict(row)
    bare.pop("channels", None)
    bare.pop("channel", None)
    return bare


def classify_row(row: dict, min_count: int) -> dict[str, Any] | None:
    count = int(row.get("monthly_count") or row.get("sokusuu") or 0)
    if count < min_count:
        return None

    username = str(row.get("username") or "")
    text = str(row.get("tweet_text") or row.get("channel_evidence") or "")
    stored = normalize_channels(row.get("channels") or row.get("channel"))
    text_channels = _scan_channel_text(text)
    pure = infer_channels(without_explicit_channels(row))
    effective = infer_channels(row)  # HTML が実際に出すもの
    override = CHANNEL_OVERRIDES.get(username.lower())
    hard = hard_scan_details(text)
    soft = soft_scan(text)

    # 根拠の土台: hard text + soft text + known override
    evidence_base = set(text_channels)
    for ch in soft:
        # soft は補助。stored がそれと一致しているかの判断材料
        pass
    if override:
        evidence_base.update(override)

    issues: list[str] = []
    notes: list[str] = []

    if not stored:
        notes.append("stored_empty: HTML は pure infer を使う")

    # stored がある場合の過大付与: hard/soft/override のどれにも無い
    support = set(text_channels) | set(soft.keys())
    if override:
        support |= set(override)

    overclaim = [c for c in stored if c not in support and c != "unknown"]
    if overclaim:
        issues.append("overclaim:" + ",".join(overclaim))

    # hard text にあるのに stored に無い（stored が明示されているとき）
    if stored and stored != ["unknown"]:
        missing = [c for c in text_channels if c not in stored and c != "other"]
        # other は main があると finalize で落ちるので missing 扱いしない
        if missing:
            issues.append("missing_from_stored:" + ",".join(missing))

    # stored と HTML 実効が違う（通常は stored 優先で一致するはず）
    if stored and stored != effective:
        issues.append(
            "stored_vs_html:" + label(stored) + "->" + label(effective)
        )

    # pure と stored が大きく違う
    if stored and pure != stored:
        notes.append("pure_differs:" + label(pure))

    # hard が空で soft だけ / override だけ
    if stored and not text_channels:
        if any(c in soft for c in stored if c != "unknown"):
            notes.append("soft_only_support")
        elif override and set(stored) <= set(override):
            notes.append("override_only")
        elif stored == ["unknown"]:
            notes.append("unknown_no_text_hint")
        else:
            issues.append("no_text_support")

    # 仮説・願望っぽい箱（実際に箱やってない）
    if re.search(r"箱に行きたく|箱行きたい|箱再開した[くいか]", text):
        if "club" in stored or "club" in effective:
            issues.append("wishful_club")
        else:
            notes.append("club_mentioned_as_wish")

    # ネト改善など未来形
    if re.search(r"ネト改善|ネト再開|ネトもやる", text) and "online" in stored:
        if "online" not in text_channels and "online" not in soft:
            pass
        elif not re.search(
            r"ネト(?:ナン|即|×|\d)|マチアプ|アプリ|タプ|APP", text, re.I
        ):
            notes.append("online_maybe_future_only")

    status = "ok"
    if issues:
        status = "review"
    elif notes:
        status = "check"

    return {
        "username": username,
        "display_name": row.get("display_name") or "",
        "monthly_count": count,
        "status": status,
        "stored": stored,
        "stored_label": label(stored) if stored else "(none)",
        "text_scan": text_channels,
        "text_scan_label": label(text_channels) if text_channels else "(none)",
        "pure_infer": pure,
        "pure_label": label(pure),
        "html_infer": effective,
        "html_label": label(effective),
        "override": override or [],
        "hard_matches": hard,
        "soft_matches": soft,
        "issues": issues,
        "notes": notes,
        "evidence_url": row.get("evidence_url")
        or row.get("tweet_url")
        or row.get("source_url")
        or "",
        "tweet_text": text,
    }


def print_report(results: list[dict], show_ok: bool) -> None:
    review = [r for r in results if r["status"] == "review"]
    check = [r for r in results if r["status"] == "check"]
    ok = [r for r in results if r["status"] == "ok"]

    print("=" * 72)
    print(f"チャネル検証: {len(results)}件  (review={len(review)} check={len(check)} ok={len(ok)})")
    print("=" * 72)

    def dump(rows: list[dict], title: str) -> None:
        if not rows:
            return
        print(f"\n## {title} ({len(rows)})\n")
        for r in rows:
            print(
                f"@{r['username']:20s} {r['monthly_count']:3d}  "
                f"stored={r['stored_label']:16s}  "
                f"text={r['text_scan_label']:16s}  "
                f"html={r['html_label']}"
            )
            if r["issues"]:
                print(f"  ISSUES: {', '.join(r['issues'])}")
            if r["notes"]:
                print(f"  notes:  {', '.join(r['notes'])}")
            if r["hard_matches"]:
                for ch, snips in r["hard_matches"].items():
                    print(f"  hard[{ch}]: {snips[0]!r}")
            if r["soft_matches"] and r["status"] != "ok":
                for ch, snips in r["soft_matches"].items():
                    print(f"  soft[{ch}]: {snips[0]!r}")
            text = (r["tweet_text"] or "").replace("\n", " / ")
            print(f"  text: {text[:160]}")
            if r["evidence_url"]:
                print(f"  url:  {r['evidence_url']}")
            print()

    dump(review, "REVIEW（要確認・たぶんズレ）")
    dump(check, "CHECK（根拠弱め・目視推奨）")
    if show_ok:
        dump(ok, "OK")
    else:
        print(f"\n## OK ({len(ok)})  … `--show-ok` で本文付き表示\n")
        for r in ok:
            print(
                f"  @{r['username']:20s} {r['monthly_count']:3d}  "
                f"{r['html_label']}"
            )


def load_rows(args: argparse.Namespace) -> list[dict]:
    if args.file:
        path = args.file
    else:
        if not args.year or not args.month:
            raise SystemExit("--file か --year/--month を指定してください")
        path = f"data/monthly_{args.year}_{args.month:02d}.json"
    if not os.path.exists(path):
        raise SystemExit(f"ファイルがありません: {path}")
    with open(path, "r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise SystemExit(f"配列 JSON ではありません: {path}")
    print(f"loaded {len(rows)} rows from {path}")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="月次チャネル検証")
    parser.add_argument("--file", help="monthly_YYYY_MM.json パス")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument(
        "--min-count",
        type=int,
        default=5,
        help="検証対象の最低即数（既定: 5）",
    )
    parser.add_argument(
        "--show-ok",
        action="store_true",
        help="OK 行も本文付きで表示",
    )
    parser.add_argument(
        "--json-out",
        help="詳細レポートを JSON で書き出すパス",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="review が1件でもあれば exit 1",
    )
    args = parser.parse_args()

    rows = load_rows(args)
    results: list[dict] = []
    for row in rows:
        item = classify_row(row, args.min_count)
        if item:
            results.append(item)

    results.sort(key=lambda r: (-r["monthly_count"], r["username"].lower()))
    print_report(results, show_ok=args.show_ok)

    if args.json_out:
        os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\njson -> {args.json_out}")

    review_n = sum(1 for r in results if r["status"] == "review")
    check_n = sum(1 for r in results if r["status"] == "check")
    print(
        f"\nsummary: review={review_n} check={check_n} "
        f"ok={len(results) - review_n - check_n}"
    )
    if args.strict and review_n:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
