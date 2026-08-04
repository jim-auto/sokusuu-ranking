"""
週間即数ランキング（お試し）

方針:
- 基本は **週内の即/節報告の積み上げ**（総括必須にしない）
- 明示の「今週N即」があればそれも採用し、積み上げと比較して大きい方
- 月次・年間総括ポストは積み上げに入れない
- 最低掲載しきい値デフォルト 3

例:
  python weekly_collect.py --year 2026 --month 7 --week last
  python weekly_collect.py --start 2026-07-27 --end 2026-08-02 --limit 60
"""

from __future__ import annotations

import argparse
import asyncio
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from monthly_collect import (
    build_period_result,
    clean_tweet_text,
    create_playwright_contexts,
    create_sessions,
    get_user_id,
    get_user_tweets,
    load_json,
    load_playwright_cookie_sets,
    normalize_period_row,
    save_json,
    search_query_tweets,
    should_replace_result,
)

OUTPUT_ACCOUNTS = "data/sokusuu_accounts.json"


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def last_week_of_month(year: int, month: int) -> tuple[date, date]:
    """その月の最終日を含む ISO 週（月〜日）。"""
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    start = last_day - timedelta(days=last_day.weekday())
    end = start + timedelta(days=6)
    return start, end


def week_label(start: date, end: date) -> str:
    return f"{start.isoformat()}_{end.isoformat()}"


def output_path(start: date, end: date) -> str:
    return f"data/weekly_{week_label(start, end)}.json"


def parse_tweet_dt(created_at: str) -> datetime | None:
    if not created_at:
        return None
    try:
        return datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return None


def parse_tweet_datetime(created_at: str) -> date | None:
    dt = parse_tweet_dt(created_at)
    return dt.date() if dt else None


# 週次で優先して拾う常連（表示名メモ）
WEEKLY_PRIORITY_SEEDS = [
    "tonpamiso",  # チャージマン研
    "nanpasei",  # ヤリっくま
    "River_p823",  # フェニックス
    "omamco_pua2",
    "17go_pua",
    "cx_lm5",
    "sub_chilll",
    "okarun_pua",
    "ryepua",
    "bookmaker_2015",
    "kimu__himitsu2",
    "shingen_pua",
    "Lattie_pua",
    "kukuru_nanpa",
    "PUAINOKI",
    "makoto__pua",
    "rei_app_pua",
    "greed_pua",
    "yomaru_street",
    "tora_maru005",
    "2BWwC9xP3Vw1MUO",
    "motebody_pua",
    "Tinder_god_2",
    "kuroiwa_45",
    "socool55555",
    "mic_pua",
    "tomu_riddle",
    "nakayamasoku",
    "147asdf764",
    "ot_aza",
    "taruchan100",
    "tsutsumi_ye4pe",
    "Niko_PUA",
    "knt17760",
]


def is_period_recap_tweet(text: str) -> bool:
    """月次・年間総括（積み上げに入れない）。"""
    cleaned = clean_tweet_text(text)
    raw = text or ""
    if re.search(
        r"(?:1[0-2]|[1-9])\s*月\s*(?:総括|統括|まとめ|振り返り)|"
        r"【\d{4}年\s*\d{1,2}月】",
        cleaned,
    ):
        return True
    if re.search(r"[０-９0-9]+\s*月\s*(?:総括|統括|まとめ)", raw):
        # 全角月など ７月統括
        return True
    if re.search(r"年間総括|年総括|年報|年末総括|月間総括|月間統括", cleaned):
        return True
    if re.search(r"\d{4}年\s*\d{1,2}月\s*(?:計|合計)?\s*\d+\s*(?:即|節)", cleaned):
        # 【2026年7月】10節 など
        return True
    # 合計16即 + チャネル内訳の月報
    if re.search(r"合計\s*\d+\s*(?:即|節)", cleaned) and re.search(
        r"(?:総括|統括|パス含めず|月間)", cleaned + raw
    ):
        return True
    return False


def extract_weekly_total(text: str, start: date, end: date) -> int | None:
    """明示の週間合計（今週N即など）。"""
    cleaned = clean_tweet_text(text)
    if cleaned.startswith("RT @"):
        return None
    if is_period_recap_tweet(text) and not re.search(r"今週|週総括|週間", cleaned):
        return None
    if re.search(r"(?:目標|予定|目指す|したい|します[!！])", cleaned):
        return None

    patterns = [
        r"今週\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"今週\s*(?:は|の)?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"週間\s*(?:は|で|計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"(?:週総括|週まとめ|週振り返り)\s*[=:：]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        rf"{start.month}/{start.day}\s*[-〜~～]\s*{end.month}/{end.day}.{{0,20}}?(\d+)\s*(?:即|節|get|g\b)",
        r"最終週\s*(?:は|で)?\s*(?:計|合計)?\s*(\d+)\s*(?:即|節|get|g\b|そ)",
        r"今週末?\s*(?:で|に|は)?\s*(\d+)\s*(?:即|節)",
    ]
    for pat in patterns:
        m = re.search(pat, cleaned, re.IGNORECASE)
        if not m:
            continue
        value = int(m.group(1))
        if 0 < value <= 200:
            return value
    return None


def _strip_urls_emoji(text: str) -> str:
    t = re.sub(r"https?://\S+", " ", text or "")
    t = re.sub(r"[\U00010000-\U0010ffff]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def is_shousai_later_flag(text: str) -> bool:
    """「しょのち」「詳細後」— 詳細は後続投稿、それ自体は件数にしない。"""
    bare = _strip_urls_emoji(text or "")
    return bool(re.search(r"しょのち|詳細後|しょうさいあと|詳細あと", bare))


def is_live_soku_announce(text: str) -> bool:
    """ライブ即宣言（短文の 即/そ。詳細は後で出すパターン含む）。

    例:
      即 / 即‼️
      そ / そーーく / いーじーーそ
      2そ目 + しょのち
      🟦即NN体しこ
    """
    raw = text or ""
    if raw.startswith("RT @") or raw.startswith("RT@"):
        return False
    bare = _strip_urls_emoji(raw)
    if not bare or len(bare) > 90:
        return False
    # しょのち / 搬送だけの投稿はライブ即ではない
    if re.fullmatch(r"(?:しょのち|詳細後|はんそう|搬送|もどる)+", bare.replace(" ", "")):
        return False
    if re.search(
        r"表示|ランキング|今週\s*\d|月総括|理論|目標|したい",
        bare,
    ):
        return False

    # 即 / 節 短文
    if re.match(r"^(?:即|節)\b", bare):
        return True
    if re.search(r"(?:^|[\s　])(?:即|節)(?:\s|$|[！!‼️NＮ体])", bare) and len(bare) <= 40:
        return True

    # Nそ目（2そ目 = 2件目の即）
    if re.search(r"\d+\s*そ目", bare):
        return True

    # いーじーそ / いーじーーそ
    if re.search(r"い[ー〜～]*じ[ー〜～]*そ", bare):
        return True

    # そ / そーく / そーーく（それ・そんな 等を除外）
    if re.search(r"それ|そん|そう|そこ|そば|そろ|そそ|くそ", bare):
        return False
    if re.search(r"(?:^|[\s　])そ[ー〜～]*く?(?:\s|$|[！!‼️😄])", bare) and len(bare) <= 50:
        return True
    # 行頭のみの そ
    if re.match(r"^そ[ー〜～]*く?\s*$", bare.split("\n")[0].strip() if "\n" in raw else bare):
        return True

    return False


def is_case_detail_tweet(text: str) -> bool:
    """即の詳細っぽい現場報告（搬・合致・明細など）。"""
    raw = text or ""
    if not raw or raw.startswith("RT @"):
        return False
    if is_period_recap_tweet(raw):
        return False
    if is_live_soku_announce(raw) and len(_strip_urls_emoji(raw)) <= 20:
        return False
    if re.search(
        r"(?:パレ搬|ホテ搬|🏠搬|合致|値\d|杯|NS|瞳孔|押忍|"
        r"NN|直家|タクシー|搬送|はんそう|連れ|満即|準即|不満即)",
        raw,
    ):
        return True
    # 絵文字ケース行
    if re.search(r"[🐶🦁🦉🏪🦐🗼🍛🍎🔥🍐🪩🦾🧚📦🍺🥂🧙]", raw) and re.search(
        r"[/／]", raw
    ):
        return True
    return False


def is_multi_case_recap(text: str) -> bool:
    """昨日2節・2即 など複数まとめ。ライブ即の詳細としては潰さない。"""
    raw = text or ""
    if re.search(r"(?:昨日|今日|本日)\s*[2-6]\s*(?:即|節)", raw):
        return True
    if re.search(r"(?:^|[\s　])[2-6]\s*(?:即|節)\b", _strip_urls_emoji(raw)):
        return True
    if re.search(r"[①②③]", raw) and re.search(r"(?:即|節|満即)", raw):
        return True
    return False


def is_independent_case_report(text: str) -> bool:
    """単独で1件以上数えられる確定ケース（ライブ即の後続詳細に潰さない）。"""
    raw = text or ""
    if is_period_recap_tweet(raw):
        return False
    if re.search(
        r"(?:準即|パス即|満即|大満即|不満即|ノーグダ即)\s*/|"
        r"(?:即|節)\s*/\s*[^\s]|"
        r"(?:準即|パス即|満即|大満即|不満即|ノーグダ即)",
        raw,
    ):
        return True
    if is_multi_case_recap(raw):
        return True
    return False


def has_success_marker_in_raw(raw: str) -> bool:
    """本文そのものに即/節の成功確定語があるか。

    clean_tweet_text は絵文字行を「ネト1即」に合成するため、
    パレ搬・ホテ搬だけの投稿が偽陽性になる。合成語は使わない。
    パレ搬/ホテ搬だけでは即未確定。
    """
    if not raw:
        return False
    if is_live_soku_announce(raw):
        return True
    if re.search(
        r"(?:"
        r"準即|パス即|満即|大満即|不満即|ノーグダ即|"
        r"(?:即|節)\s*/|"
        r"(?:即|節)\s*[！!‼️]+|"
        r"今月\s*\d+\s*節目|"
        r"\d+\s*(?:即|節)\b|"
        r"即った|即れ[たとので]|即り"
        r")",
        raw,
    ):
        return True
    bare = _strip_urls_emoji(raw)
    # 単独の「即」「節」投稿（短い成功報告）
    if re.match(r"^(?:即|節)\b", bare) and len(bare) < 80:
        return True
    # 行頭・区切り後の 即/節（絵文字除去後）
    if re.search(r"(?:^|[\s　])(?:即|節)(?:\s|$|[！!/／])", bare):
        return True
    return False


def count_stack_units(text: str) -> int:
    """1ツイート内の即/節報告を何件として積むか。

    厳しめ: 実ケース報告っぽいものだけ。
    パレ搬・ホテ搬だけでは即未確定なので 0。
    「ノア（10節）」「表示はスト4即」みたいな言及は 0。
    """
    raw = text or ""
    if raw.startswith("RT @") or raw.startswith("RT@"):
        return 0
    if is_period_recap_tweet(raw):
        return 0

    cleaned = clean_tweet_text(raw)
    if not cleaned:
        return 0

    # 週間合計ツイートは積み上げ単位にせず、明示合計側で処理
    if re.search(r"今週\s*\d+\s*(?:即|節)|週総括|週まとめ|週間\s*\d+", cleaned):
        return 0

    # メタ会話・ランキング話は除外（根拠にならない）
    if re.search(
        r"表示|ランキング|位あざ|比較したく|難しいですか|どうですか|"
        r"風潮|予想|理論|マインド|界隈的",
        cleaned,
    ):
        return 0

    # 未確定・願望は除外
    if re.search(
        r"即れ[ただ]?ら|即りたい|即したい|即れそう|即れるか|即ろう|片方即|"
        r"即れたら|即れたらだいぶ|アツい$|締日なので",
        raw,
    ) and not re.search(r"(?:準即|満即|パス即|即/|即‼️|即った)", raw):
        # 条件・願望のみ。確定ラベルがあれば通す
        if not re.search(r"(?:準即|満即|パス即|ノーグダ即)\b|(?:即|節)\s*/", raw):
            return 0

    # パレ搬/ホテ搬のみ・搬送報告のみは数えない（即確定語が raw に必要）
    if not has_success_marker_in_raw(raw):
        return 0

    # 明細付きケース: 即/24 節/22（raw 基準・合成のネト1即は使わない）
    slash_n = len(re.findall(r"(?:即|節|準即)\s*/\s*[^\s]", raw))
    if slash_n:
        return min(slash_n, 8)

    def _multi_n() -> int | None:
        """2即/3節 などの複数報告。clean の絵文字変換に依存しない。"""
        bare = _strip_urls_emoji(raw)
        for src in (bare, cleaned):
            m = re.search(r"(?:^|[\s　])([2-6])\s*(?:即|節)\b", src)
            if m:
                return int(m.group(1))
        # 絵文字直後「🦾2即」
        m = re.search(r"([2-6])\s*(?:即|節)\b", raw[:50])
        if m:
            return int(m.group(1))
        return None

    multi_support = bool(
        re.search(
            r"(?:即|節)\s*/|NS|合致|満即|準即|パス即|値\d|杯|①|②",
            raw,
        )
    )

    # 即‼️ / 節‼️（成功報告）
    if re.search(r"(?:即|節)\s*[！!‼️]+", raw):
        n = _multi_n()
        if n and multi_support:
            return n
        return 1

    # 「2即」「3節」+ ケース語（搬/ホテだけでは不可）
    n = _multi_n()
    if n and multi_support:
        return n

    # 準即・パス即・満即など成功ラベル（現場語は不要。ラベル自体が確定）
    if re.search(r"(?:準即|パス即|満即|大満即|不満即|ノーグダ即)", raw):
        return 1

    # 今月N節目（成功後の節目報告）— 数値Nではなく 1 件
    if re.search(r"今月\s*\d+\s*節目", cleaned) and not re.search(
        r"表示|位|比較", cleaned
    ):
        return 1

    # 単独「即」「節」行 or 短い成功報告
    bare = _strip_urls_emoji(raw)
    if re.match(r"^(?:即|節)\b", bare) and len(bare) < 80:
        return 1

    # ライブ即（そ / そーく / Nそ目 / いーじーそ など）
    if is_live_soku_announce(raw):
        return 1

    # raw に即/節があるケース報告（即った、即/明細以外の短文など）
    # clean の「ネト1即」合成 + パレ搬 ではここに来ない（上で return 0 済み）
    if re.search(r"即った|即れ[たとので]|即り|\d+\s*(?:即|節)\b", raw):
        return 1
    if re.search(r"(?:^|[\s　])(?:即|節)(?:\s|$|[！!/／])", bare):
        return 1

    return 0


def extract_setsu_milestone(text: str) -> int | None:
    """今月N節目 の N。"""
    cleaned = clean_tweet_text(text)
    m = re.search(r"今月\s*(\d+)\s*節目", cleaned)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*節目", cleaned)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 80:
            return n
    return None


def stack_weekly_from_tweets(tweets, username: str, start: date, end: date) -> dict | None:
    """週内ツイートを積み上げて週間件数を作る。

    ライブ即 → 後続詳細（しょのち / 搬 / 明細）は 1 ケースとして二重計上しない。
    """
    stack_total = 0
    evidence_items: list[dict] = []
    milestones: list[int] = []
    explicit_best = None
    # ライブ即の直後に詳細が続く窓（時間）
    detail_window = timedelta(hours=12)
    pending_live_dt: datetime | None = None

    def _sort_key(t: dict):
        dt = parse_tweet_dt(t.get("created_at", ""))
        return dt or datetime(1970, 1, 1, tzinfo=timezone.utc)

    # 古い→新しいでペアリング
    ordered = sorted(tweets, key=_sort_key)

    for tweet in ordered:
        created_dt = parse_tweet_dt(tweet.get("created_at", ""))
        created = created_dt.date() if created_dt else None
        # 積み上げは週そのもの（start〜end）のみ
        if created and not (start <= created <= end):
            # 明示週間合計だけ報告窓+2日を許可
            if created <= end + timedelta(days=2):
                total = extract_weekly_total(tweet.get("text", ""), start, end)
                if total:
                    hit = {
                        "count": total,
                        "url": f"https://x.com/{username}/status/{tweet['id']}",
                        "text": (tweet.get("text") or "")[:500],
                        "created_at": tweet.get("created_at", ""),
                        "method": "explicit",
                    }
                    if explicit_best is None or total > explicit_best["count"]:
                        explicit_best = hit
            continue

        text = tweet.get("text", "") or ""
        total = extract_weekly_total(text, start, end)
        if total:
            hit = {
                "count": total,
                "url": f"https://x.com/{username}/status/{tweet['id']}",
                "text": text[:500],
                "created_at": tweet.get("created_at", ""),
                "method": "explicit",
            }
            if explicit_best is None or total > explicit_best["count"]:
                explicit_best = hit

        ms = extract_setsu_milestone(text)
        if ms is not None:
            milestones.append(ms)

        # しょのち単独は件数にしない（直前ライブのフラグ）
        if is_shousai_later_flag(text) and not has_success_marker_in_raw(text):
            if pending_live_dt and created_dt:
                tid = str(tweet.get("id") or "")
                evidence_items.append(
                    {
                        "units": 0,
                        "tweet_id": tid,
                        "url": f"https://x.com/{username}/status/{tid}",
                        "created_at": tweet.get("created_at", ""),
                        "text": text[:500],
                        "role": "case_detail",
                    }
                )
            continue

        units = count_stack_units(text)
        role = "case_report"
        live = is_live_soku_announce(text)

        # ライブ即の後続詳細 → 件数は足さず根拠だけ残す
        # （準即/・即/明細など独立ケースは潰さない）
        if (
            pending_live_dt
            and created_dt
            and created_dt - pending_live_dt <= detail_window
            and not live
            and not is_independent_case_report(text)
            and (is_case_detail_tweet(text) or (units > 0 and not is_live_soku_announce(text)))
        ):
            role = "case_detail"
            units = 0
            pending_live_dt = None
        elif units > 0:
            if live:
                pending_live_dt = created_dt
            else:
                # 独立ケース報告のあとは詳細待ちをクリア
                pending_live_dt = None
        elif is_case_detail_tweet(text) and pending_live_dt and created_dt:
            if (
                created_dt - pending_live_dt <= detail_window
                and not is_independent_case_report(text)
            ):
                role = "case_detail"
                units = 0
                pending_live_dt = None
            else:
                continue
        else:
            continue

        if units > 0 or role == "case_detail":
            stack_total += units
            tid = str(tweet.get("id") or "")
            evidence_items.append(
                {
                    "units": units,
                    "tweet_id": tid,
                    "url": f"https://x.com/{username}/status/{tid}",
                    "created_at": tweet.get("created_at", ""),
                    "text": text[:500],
                    "role": role,
                }
            )

    # 節目の差分（週内で今月N節目が伸びた分）
    milestone_delta = 0
    if len(milestones) >= 2:
        milestone_delta = max(milestones) - min(milestones)
        if milestone_delta < 0:
            milestone_delta = 0
    elif len(milestones) == 1:
        milestone_delta = 0

    stack_count = max(stack_total, milestone_delta)

    # 明示の「今週N即」があれば優先（積み上げは過大になりやすい）
    if explicit_best:
        explicit_best["evidence_items"] = [
            {
                "units": explicit_best["count"],
                "tweet_id": explicit_best["url"].rsplit("/", 1)[-1],
                "url": explicit_best["url"],
                "created_at": explicit_best.get("created_at", ""),
                "text": explicit_best.get("text", ""),
                "role": "explicit_total",
            }
        ]
        return explicit_best
    if stack_count <= 0:
        return None

    # 新しい順
    evidence_items.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    if not evidence_items:
        return None

    # 代表リンクは件数付きの最新ケース
    scored = [e for e in evidence_items if int(e.get("units") or 0) > 0]
    ev = scored[0] if scored else evidence_items[0]
    case_n = sum(1 for e in evidence_items if int(e.get("units") or 0) > 0)
    detail_n = sum(1 for e in evidence_items if e.get("role") == "case_detail")
    summary = f"積み上げ{stack_count}（報告{case_n}投稿"
    if detail_n:
        summary += f" / 詳細{detail_n}"
    if milestone_delta:
        summary += f" / 節目+{milestone_delta}"
    summary += "）"
    return {
        "count": stack_count,
        "url": ev["url"],
        "text": ev.get("text", ""),
        "created_at": ev.get("created_at", ""),
        "method": "stack",
        "stack_posts": case_n,
        "milestone_delta": milestone_delta,
        "stack_summary": summary,
        "evidence_items": evidence_items,
    }


def _format_created_at(created_at: str) -> str:
    if not created_at:
        return ""
    try:
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return created_at


def write_weekly_markdown(rows: list[dict], start: date, end: date) -> None:
    """結果表 + ユーザー別根拠Markdownを生成。"""
    label = f"{start.month}/{start.day}〜{end.month}/{end.day}"
    slug = week_label(start, end).replace(":", "")
    results_path = Path(f"docs/results_weekly_{start.year}_{start.month:02d}_last.md")
    evidence_path = Path(
        f"docs/evidence_weekly_{start.year}_{start.month:02d}_last.md"
    )

    stack_n = sum(1 for r in rows if r.get("count_method") == "stack")
    exp_n = sum(1 for r in rows if r.get("count_method") == "explicit")

    # --- 結果表 ---
    lines = [
        f"# 週間ランキング結果（お試し・積み上げ）",
        "",
        f"**期間:** {start.isoformat()} 〜 {end.isoformat()}（{label}）",
        "",
        "## 注意（trial）",
        "",
        "- あくまでお試し。本運用ではない",
        "- **方式: 週内の即/節ケース報告を積み上げ**（総括必須にしない）",
        "- ライブ即（即 / そ / そーく / Nそ目）→ 後続詳細（しょのち・搬）は 1 ケース扱い",
        "- パレ搬・ホテ搬だけでは即未確定（数えない）",
        "- 明示「今週N即」があれば優先",
        "- 月次・年間総括・ランキング雑談は除外",
        "- 掲載しきい値: デフォルト2〜3",
        f"- 件数: {len(rows)}（積み上げ {stack_n} / 明示 {exp_n}）",
        f"- **根拠ツイート一覧:** [`{evidence_path.name}`](./{evidence_path.name})",
        "",
        "## 結果",
        "",
        "| # | account | 表示名 | 即数 | 方式 | 報告数 | 根拠一覧 |",
        "|---|---------|--------|-----:|------|-------:|----------|",
    ]
    for i, r in enumerate(rows, 1):
        u = r.get("username") or ""
        name = (r.get("display_name") or "").replace("|", "\\|")
        c = int(r.get("weekly_count") or 0)
        method = r.get("count_method") or r.get("match_source") or ""
        method_label = (
            "積み上げ"
            if method == "stack"
            else ("明示" if method == "explicit" else method)
        )
        posts = r.get("stack_posts")
        posts_s = str(posts) if posts is not None else "-"
        anchor = u.lower()
        lines.append(
            f"| {i} | @{u} | {name} | {c} | {method_label} | {posts_s} | "
            f"[詳細](./{evidence_path.name}#{anchor}) |"
        )

    results_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # --- 根拠（ユーザー別セクション） ---
    elines = [
        f"# 週間ランキング根拠（{label}）",
        "",
        f"期間: {start.isoformat()} 〜 {end.isoformat()}",
        "",
        "各ユーザーの積み上げに使ったツイートです。本文は短縮あり。",
        "",
        f"結果表: [`{results_path.name}`](./{results_path.name})",
        "",
        "---",
        "",
    ]
    for i, r in enumerate(rows, 1):
        u = r.get("username") or ""
        name = r.get("display_name") or ""
        c = int(r.get("weekly_count") or 0)
        method = r.get("count_method") or ""
        method_label = (
            "積み上げ"
            if method == "stack"
            else ("明示" if method == "explicit" else method)
        )
        items = r.get("evidence_items") or []
        elines.append(f"## @{u}")
        elines.append("")
        elines.append(f"- 表示名: {name}")
        elines.append(f"- 週間: **{c}**（{method_label}）")
        if r.get("stack_summary"):
            elines.append(f"- サマリ: {r['stack_summary']}")
        elines.append(f"- プロフィール: https://x.com/{u}")
        elines.append("")
        if not items:
            # fallback single evidence
            url = r.get("evidence_url") or r.get("tweet_url") or ""
            text = (r.get("tweet_text") or "").replace("\n", " ")
            elines.append("### 根拠ツイート")
            elines.append("")
            if url:
                elines.append(f"1. {url}")
                if text:
                    elines.append(f"   - {text[:240]}")
            else:
                elines.append("_根拠ツイートなし_")
            elines.append("")
            elines.append("---")
            elines.append("")
            continue

        elines.append(f"### 根拠ツイート（{len(items)}件）")
        elines.append("")
        for j, item in enumerate(items, 1):
            url = item.get("url") or ""
            units = item.get("units", 1)
            created = _format_created_at(item.get("created_at", ""))
            text = (item.get("text") or "").replace("\n", " ").strip()
            role = item.get("role") or "case_report"
            if role == "explicit_total":
                role_ja = "週間合計"
            elif role == "case_detail":
                role_ja = "詳細"
            else:
                role_ja = "ケース"
            elines.append(f"{j}. **+{units}**（{role_ja}） {created}")
            elines.append(f"   - {url}")
            if text:
                elines.append(f"   - `{text[:280]}`")
            elines.append("")
        elines.append("---")
        elines.append("")

    evidence_path.write_text("\n".join(elines) + "\n", encoding="utf-8")
    print(f"docs: {results_path} / {evidence_path}")


def load_seed_usernames(limit: int = 0) -> list[str]:
    usernames: list[str] = []
    seen: set[str] = set()

    def add(u: str) -> None:
        u = (u or "").strip().lstrip("@")
        key = u.lower()
        if not u or key in seen:
            return
        seen.add(key)
        usernames.append(u)

    # 1) 優先常連（チャージマン研 / やりっくま / フェニックス など）
    for u in WEEKLY_PRIORITY_SEEDS:
        add(u)

    # 2) seed_accounts.txt
    seed_path = Path("seed_accounts.txt")
    if seed_path.exists():
        for line in seed_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            add(line)

    # 3) 7月月間実績順
    july = load_json("data/monthly_2026_07.json", [])
    july_sorted = sorted(
        july, key=lambda r: -int(r.get("monthly_count") or 0)
    )
    for row in july_sorted:
        add(row.get("username", ""))

    # 4) 総合アカウント上位
    accounts = load_json(OUTPUT_ACCOUNTS, [])
    accounts = sorted(
        accounts,
        key=lambda r: -int(r.get("sokusuu") or r.get("followers_count") or 0),
    )
    for row in accounts[:250]:
        add(row.get("username", ""))

    if limit > 0:
        return usernames[:limit]
    return usernames


async def collect_user_tweets_for_week(
    context,
    sessions,
    session_idx,
    username: str,
    start: date,
    end: date,
):
    """Search + API TL で週周辺の投稿を集める。"""
    until = end + timedelta(days=2)
    q = (
        f"from:{username} since:{start.isoformat()} until:{until.isoformat()}"
    )
    tweets = []
    try:
        tweets = await search_query_tweets(context, q, scrolls=4)
    except Exception as e:
        print(f"  [search] @{username} ERR {e}")

    if sessions:
        uid = get_user_id(sessions, session_idx, username)
        if uid:
            tl = get_user_tweets(sessions, session_idx, uid, count=40, max_pages=3)
            seen = {t.get("id") for t in tweets}
            for t in tl:
                if t.get("id") not in seen:
                    tweets.append(t)
    return tweets


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="週間即数ランキング（積み上げ trial）")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--month", type=int, default=7)
    parser.add_argument("--week", choices=["last"], default="last")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--min-count", type=int, default=2)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="走査人数（0=seed全件・上限300）",
    )
    parser.add_argument(
        "--only",
        default="",
        help="カンマ区切り username のみ収集（例: tonpamiso,nanpasei,River_p823）",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="既存 weekly JSON にマージ（--only と併用向き）",
    )
    parser.add_argument("--headful", action="store_true")
    args = parser.parse_args()

    if args.start and args.end:
        start, end = parse_iso_date(args.start), parse_iso_date(args.end)
    else:
        start, end = last_week_of_month(args.year, args.month)

    out = output_path(start, end)
    print("=" * 60)
    print(f"週間即数ランキング（積み上げ trial） {start} 〜 {end}")
    print("方式: 週内の即/節報告を積み上げ（ライブ即→詳細は二重計上しない）")
    print(f"出力: {out}")
    print("=" * 60)

    accounts = load_json(OUTPUT_ACCOUNTS, [])
    accounts_map = {a["username"].lower(): a for a in accounts}
    # 表示名だけ既知の優先ユーザー
    display_overrides = {
        "tonpamiso": "チャージマン研",
        "nanpasei": "ヤリっくま@味噌代表",
        "river_p823": "フェニックス@セクシーコマンドー部",
    }

    if args.only.strip():
        seeds = [
            u.strip().lstrip("@")
            for u in args.only.split(",")
            if u.strip()
        ]
    else:
        seed_limit = args.limit if args.limit > 0 else 300
        seeds = load_seed_usernames(limit=seed_limit)
    print(f"seed: {len(seeds)}")

    results_map: dict[str, dict] = {}
    if args.merge and Path(out).exists():
        for r in load_json(out, []):
            key = (r.get("username") or "").lower()
            if key:
                results_map[key] = r
        print(f"merge base: {len(results_map)} rows from {out}")

    sessions = create_sessions()
    session_idx = [0]
    cookie_sets = load_playwright_cookie_sets()

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser, contexts = await create_playwright_contexts(
            p, cookie_sets, headless=not args.headful
        )
        ctx = contexts[0]["context"]

        for i, username in enumerate(seeds, 1):
            tweets = await collect_user_tweets_for_week(
                ctx, sessions, session_idx, username, start, end
            )
            hit = stack_weekly_from_tweets(tweets, username, start, end)
            if not hit:
                # 再収集で0件なら merge からも落とす
                key = username.lower()
                if key in results_map:
                    print(f"  [drop] @{username} (recollect 0)")
                    results_map.pop(key, None)
                if i % 20 == 0:
                    print(f"  [{i}/{len(seeds)}] hits={len(results_map)}")
                continue

            account = dict(accounts_map.get(username.lower(), {}))
            account.setdefault("username", username)
            if username.lower() in display_overrides:
                account["display_name"] = display_overrides[username.lower()]
            elif not account.get("display_name"):
                account["display_name"] = username
            method = hit.get("method", "stack")
            match_source = "stack" if method == "stack" else "search"
            row = build_period_result(account, hit, "weekly_count", match_source)
            row["count_method"] = method
            if hit.get("stack_posts") is not None:
                row["stack_posts"] = hit["stack_posts"]
            if hit.get("milestone_delta"):
                row["milestone_delta"] = hit["milestone_delta"]
            if hit.get("stack_summary"):
                row["stack_summary"] = hit["stack_summary"]
            if hit.get("evidence_items"):
                row["evidence_items"] = hit["evidence_items"]

            key = username.lower()
            # 週次 trial は再収集で常に上書き（厳格化で件数が下がることも）
            results_map[key] = row
            print(
                f"  [{method}] @{username}: {hit['count']}"
                + (
                    f" (posts={hit.get('stack_posts')})"
                    if method == "stack"
                    else ""
                )
            )

        await browser.close()

    rows = [normalize_period_row(r) for r in results_map.values()]
    # evidence_items は normalize で落ちないよう再付与
    for r in rows:
        key = (r.get("username") or "").lower()
        src = results_map.get(key) or {}
        if src.get("evidence_items") and not r.get("evidence_items"):
            r["evidence_items"] = src["evidence_items"]
        if src.get("stack_summary"):
            r["stack_summary"] = src["stack_summary"]
        if src.get("count_method"):
            r["count_method"] = src["count_method"]
        if src.get("stack_posts") is not None:
            r["stack_posts"] = src["stack_posts"]
        if src.get("display_name") and not r.get("display_name"):
            r["display_name"] = src["display_name"]

    rows = [r for r in rows if int(r.get("weekly_count") or 0) >= args.min_count]
    rows.sort(
        key=lambda r: (
            -int(r.get("weekly_count") or 0),
            -(r.get("followers_count") or 0),
            (r.get("username") or "").lower(),
        )
    )
    for r in rows:
        r["period_start"] = start.isoformat()
        r["period_end"] = end.isoformat()
        r["period_label"] = f"{start.month}/{start.day}〜{end.month}/{end.day}"
        r.setdefault("count_method", r.get("match_source", "stack"))

    save_json(out, rows)
    write_weekly_markdown(rows, start, end)
    print(f"\n{len(rows)}件 (>= {args.min_count}) -> {out}")
    for i, r in enumerate(rows[:25], 1):
        print(
            f"  {i}. @{r['username']}: {r['weekly_count']} "
            f"[{r.get('count_method', '?')}]"
        )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
