"""
即数ランキング HTML レポート生成スクリプト

data/sokusuu_accounts.json から GitHub Pages 用の
index.html を docs/ に生成する。

ランキング4種:
  1. 総合（全員）
  2. ストリートナンパ
  3. クラブナンパ
  4. オンライン（マッチングアプリ等）
"""

import json
import os
import re
from datetime import datetime


def env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


INPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_DIR = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "index.html")
SHOW_PERIOD_TABS = env_flag("SHOW_PERIOD_TABS", default=True)
SHOW_PERIOD_DETAIL_TABS = env_flag("SHOW_PERIOD_DETAIL_TABS", default=False)
DEFAULT_TAB = os.getenv("DEFAULT_TAB", "all").strip() or "all"
DEFAULT_MONTH = os.getenv("DEFAULT_MONTH", "").strip()

# Public ranking should not double-count obvious sub/alt accounts that
# represent the same person and total.
DUPLICATE_ACCOUNT_CANONICALS = {
    "emuchi_pua": "puro_nanpa",
    "sub_chilll": "pua_chilll",
    "gureran_m3": "gureran_m",
    "inpsub": "ryepua",
}

# チャネル分類: ストナン / ネトナン / 箱 / その他 / 謎
CATEGORY_LABELS = {
    "all": "総合",
    "street": "ストナン",
    "online": "ネトナン",
    "club": "箱",
    "other": "その他",
    "unknown": "謎",
}
CHANNEL_ORDER = ["street", "online", "club", "other", "unknown"]
# 表ヘッダ: 複数チャネル時は総括内訳の件数が多い順
CHANNEL_COL_HEADER = "チャネル（即数が多い順）"
CHANNEL_COL_TH = (
    f'<th title="総括本文の内訳件数が多い順（同数はスト→ネト→箱）">'
    f"{CHANNEL_COL_HEADER}</th>"
)

# ツイート本文向けのチャネル推定（誤爆しにくいパターン）
# 「スト」は インスト 等に誤爆しやすいので前後を制限
#
# 界隈の絵文字チャネル（総括の内訳でよく使う）:
#   street: 🐶 ライオン 🦉 店舗連れ 🏪 / 🦐(エビス) / GT系 / Ⓜ(M)
#   online: 🗼(タプ) 🍛(ペアーズ) 🍎 🔥(with等) 🍐 東カレ系 / 🪩(ミラーボール=ネト)
#   club:   🦾 🧚 📦 / 会場色 🟦⬛️⬜ 等
STREET_HINTS = re.compile(
    r"(?<![イアウ])スト(?:ナン|即|準|コンビ|×|ｘ|x|\d|[\s　/／・・(（脚]|$)|"
    # ひらがな月報: 「すと こんび」「すと?」
    r"(?:^|[\s　/／|・])すと(?:ナン|即|コンビ|こんび|[\s　/／|・?？×ｘx\d]|$)|"
    r"丘スト|完ソロスト|ソロスト|地方スト|ストリート|"
    r"路上|[🐶🦁🦉🏪🦐]|(?:SGT|MGT)|GTスト|"
    r"味噌(?:スト|1日|遠征)?|明太子|"
    r"弾丸(?:即|×|ｘ|x|\d|[\s　/／(（]|$)|店連れ|"
    # イベント系ストリート（ひらがな総括向け）
    r"なつまつり|はなびたいかい|花火大会|夏祭り|うみ🌊|さわがに|"
    # Ⓜ / Ⓜ️ / 🅜 / キーキャップM はストナン
    r"[Mm]️\u20e3|Ⓜ|Ⓜ️|🅜",
    re.IGNORECASE,
)
ONLINE_HINTS = re.compile(
    r"(?:^|[^ア-ン])ネト(?:ナン|即|準|新規|ヘルプ|×|ｘ|x|\d|[\s　/／・(（]|$)|"
    r"マチアプ|マッチングアプリ|東カレ|"
    r"with|ｳｨｽﾞ|ウィズ|wiz|"
    r"タップル?|タプ|tin|tinder|pairs|ペアーズ|"
    # ひらがな/崩し字アプリ名
    r"てぃんだー|ティンダー|いんすた|インスタ|すれっず|スレッズ|threads?|"
    r"た[🍎🔥]|[🗼🍛🍎🔥🍐🪩]|"
    r"ワクメ|アプリ即|ネトナン|イン⭐|いん⭐|"
    r"某\s*app|使用\s*APP|TikTok|ティックトック",
    re.IGNORECASE,
)
CLUB_HINTS = re.compile(
    r"(?:^|[^ア-ン])箱(?:ナン|即|×|ｘ|x|\d|[\s　/／・(（]|$)|"
    r"クラブナン|クラナン|クラブ|"
    r"相席|オリラジ|ロマ絵|"
    # 🦾箱 / 📦 / 会場タグの色四角（CRUBD・コマン部系の内訳）
    # 🥂📦 はキャバ等のその他なので 📦 は🥂直後を除外
    r"[🦾🧚🟦⬛⬜◼◾▪◻◽]|(?<!🥂)📦",
    re.IGNORECASE,
)
# パス/代打/🥂系は「その他」（メインチャネルが無いときだけ other 単独表示）
OTHER_HINTS = re.compile(
    r"(?:パス(?!ワード)|代打|アテンド|くるくる|ハイエナ|指名)(?:即|×|\(|（|\d|[\s　]|$)|その他|オフライン|"
    r"🥂",  # キャバ等。箱(クラブ)とは別枠
    re.IGNORECASE,
)

# よく分かっている人のチャネル上書き（プロフィールカテゴリより優先）
CHANNEL_OVERRIDES = {
    "kent_o_o": ["street"],
    "taruchan100": ["street"],
    "daigakusei_pua": ["street"],
    "nakayamasoku": ["online"],
    "tinder_god_2": ["online"],
    "tomu_riddle": ["online"],
    "shime_pua": ["online"],  # ネト主、少量ストは本文があれば追加
    "motebody_pua": ["online"],  # マチアプ主戦（節報はapp中心）
    "river_p823": ["club"],
    "outlook_sabo_4": ["club"],
    "cx_lm5": ["club"],
    "sub_chilll": ["club"],
    "pua_chilll": ["club"],
    "bangedaisuki": ["street"],
    "pua_co": ["street"],
    "chiroru_pua": ["street"],
    "oyasugaoo": ["street"],
    "atannon_nampa": ["street"],
    "dick_duck_swing": ["street"],  # スト主（🐶🦁）+ パス多めでもその他にしない
    "yomaru_street": ["street", "club"],  # スト+箱メイン
    # 🥂📦 等は箱ではなくその他寄り（スト1 + 🥂📦4）
    "maya159r": ["other", "street"],
}


def active_class(tab_id: str) -> str:
    return " active" if tab_id == DEFAULT_TAB else ""


def normalize_month_id(value: str) -> str:
    value = value.strip().lower().replace("-", "_")
    if not value:
        return ""
    if value.startswith("m") and len(value) == 7 and value[1:].isdigit():
        return value
    compact = value.replace("_", "")
    if len(compact) == 6 and compact.isdigit():
        return "m" + compact
    parts = value.split("_")
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return "m" + parts[0] + parts[1].zfill(2)
    return ""


def load_data(filepath: str) -> list[dict]:
    """JSONデータを読み込む"""
    if not os.path.exists(filepath):
        print(f"[ERROR] {filepath} が見つかりません。先に scraper.py を実行してください。")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def split_csv(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def join_unique_csv(*values: str, exclude: set[str] | None = None) -> str:
    exclude = exclude or set()
    merged: list[str] = []
    for value in values:
        for item in split_csv(value):
            if item in exclude or item in merged:
                continue
            merged.append(item)
    return ", ".join(merged)


def _scan_channel_text(text: str) -> list[str]:
    """本文だけからチャネルを拾う。"""
    if not text or not str(text).strip():
        return []
    found: list[str] = []

    def add(channel: str) -> None:
        if channel not in found:
            found.append(channel)

    if STREET_HINTS.search(text):
        add("street")
    if ONLINE_HINTS.search(text):
        add("online")
    if CLUB_HINTS.search(text):
        add("club")
    # 🥂 / 🥂📦 は箱ではなくその他（キャバ等）
    if re.search(r"🥂", text) or OTHER_HINTS.search(text):
        if "other" not in found:
            # パス等の OTHER_HINTS はメインがあるとき後段で落とされることがある
            if re.search(r"🥂", text) or not any(
                c in found for c in ("street", "online", "club")
            ):
                add("other")
    return found


def estimate_channel_counts(text: str) -> dict[str, int]:
    """総括本文からチャネル別の件数をざっくり数える（表示順用）。

    絵文字1個・「パス」1回・「スト×N」などを加点する。
    """
    counts = {c: 0 for c in CHANNEL_ORDER}
    if not text or not str(text).strip():
        return counts
    raw = str(text)

    def _add_emoji_qty(channel: str, pattern: str) -> None:
        """🐶5 / 🔥10 / 🦾×3 のように絵文字+数量があれば数量を、なければ1を加点。"""
        for m in re.finditer(
            rf"(?:{pattern})\s*[:：/／×xｘ*]?\s*(\d+)?",
            raw,
        ):
            if m.group(1):
                counts[channel] += int(m.group(1))
            else:
                counts[channel] += 1

    # 絵文字（内訳リスト）— 数量付きを優先して合算
    # Ⓜ/Ⓜ️(M)→スト、🪩(ミラーボール)→ネト
    # 🥂 / 🥂📦 → その他（キャバ等。箱クラブとは別）
    _add_emoji_qty("street", r"[🐶🦁🦉🏪🦐]|Ⓜ\uFE0F?|🅜")
    _add_emoji_qty("online", r"[🍐🍎🔥🗼🍛🪩]")
    # 🥂📦 数量（📦 単体の箱カウントより先に拾う）
    for m in re.finditer(r"🥂\s*📦\s*[:：/／×xｘ*]?\s*(\d+)?", raw):
        counts["other"] += int(m.group(1)) if m.group(1) else 1
        # マッチ済み 📦 を潰して二重カウント防止
        raw = raw[: m.start()] + (" " * (m.end() - m.start())) + raw[m.end() :]
    _add_emoji_qty("club", r"[🦾🧚📦🟦⬛⬜◼◾▪◻◽]")
    _add_emoji_qty("other", r"[🥂]")

    # キーワード回数（数量なしの単なる言及は弱く1）
    # パスは「パス3」「パス×3」「(パス3)」を数量として
    for m in re.finditer(r"パス(?!ワード)\s*[×xｘ*]?\s*(\d+)?", raw):
        counts["other"] += int(m.group(1)) if m.group(1) else 1
    counts["street"] += len(re.findall(r"(?<![イアウ])スト(?:ナン|即|準|×|ｘ|x)", raw))
    counts["street"] += len(re.findall(r"弾丸|店連れ", raw))
    counts["online"] += len(re.findall(r"(?<![ア-ン])ネト(?:ナン|即|準|×|ｘ|x|新規)", raw))
    counts["online"] += len(re.findall(r"マチアプ|アプリ即|某\s*app|使用\s*APP", raw, re.I))
    counts["club"] += len(re.findall(r"(?<![ア-ン])箱(?:ナン|即|×|ｘ|x)|クラ(?:ブ|ナン)|相席", raw))

    # 「スト 10即」「ネト×6」など数量付きは max で上書き寄りに
    for ch, pats in (
        ("street", [r"スト[^\d]{0,6}(\d+)\s*(?:即|節)", r"弾丸[^\d]{0,4}(\d+)"]),
        ("online", [r"ネト[^\d]{0,6}(\d+)\s*(?:即|節)"]),
        ("club", [r"箱[^\d]{0,6}(\d+)\s*(?:即|節)", r"クラブ[^\d]{0,4}(\d+)"]),
        ("other", [r"パス[^\d]{0,4}(\d+)"]),
    ):
        for pat in pats:
            for m in re.finditer(pat, raw):
                try:
                    counts[ch] = max(counts[ch], int(m.group(1)))
                except ValueError:
                    pass
    return counts


def order_channels_by_count(channels: list[str], text: str = "") -> list[str]:
    """チャネルを件数が多い順に並べる（同数は CHANNEL_ORDER）。"""
    if not channels:
        return []
    counts = estimate_channel_counts(text)
    order_index = {c: i for i, c in enumerate(CHANNEL_ORDER)}
    return sorted(
        channels,
        key=lambda c: (-int(counts.get(c) or 0), order_index.get(c, 99)),
    )


def infer_channels(record: dict) -> list[str]:
    """ネト/スト/箱/その他/謎 を推定する。

    優先順位:
      1. 明示フィールド channels / channel
      2. 総括ツイート本文（channel_evidence 含む）
      3. ユーザ名の既知オーバーライド
      4. プロフィール categories（弱いフォールバック）
      5. bio / display_name
      6. 謎

    並び順は本文の件数が多い順（同数は street→online→club→other→unknown）。
    """
    evidence = " ".join(
        str(record.get(key) or "")
        for key in ("channel_evidence", "tweet_text")
    )
    if re.search(r"上半期\s*合算", evidence):
        evidence = str(record.get("channel_evidence") or "")

    def finalize(
        channels: list[str],
        *,
        drop_other_with_primary: bool = True,
        sort_text: str = "",
    ) -> list[str]:
        # 自動推定時: パス由来の other はメインチャネルがあるとき落とす
        # 🥂（キャバ等）由来の other はメインと併記する（箱ではない）
        # 明示 channels 指定時は other 併記を尊重する
        if (
            drop_other_with_primary
            and "other" in channels
            and any(c in channels for c in ("street", "online", "club"))
            and not re.search(r"🥂", sort_text or evidence)
        ):
            channels = [c for c in channels if c != "other"]
        if not channels:
            channels = ["unknown"]
        # 重複除去しつつ件数順
        uniq = []
        for c in channels:
            if c in CHANNEL_ORDER and c not in uniq:
                uniq.append(c)
        if not uniq:
            uniq = ["unknown"]
        return order_channels_by_count(uniq, sort_text or evidence)

    # 1) 事前計算済み（["unknown"] / "unknown" だけは未確定扱いで本文再推定）
    explicit = record.get("channels")
    if isinstance(explicit, list) and explicit:
        explicit_clean = [c for c in explicit if c in CHANNEL_ORDER and c != "unknown"]
        if explicit_clean:
            return finalize(
                list(explicit_clean),
                drop_other_with_primary=False,
                sort_text=evidence,
            )
    if isinstance(explicit, str) and explicit.strip():
        parts = [c for c in split_csv(explicit) if c != "unknown"]
        if parts:
            return finalize(parts, drop_other_with_primary=False, sort_text=evidence)

    single = record.get("channel")
    if isinstance(single, str) and single in CHANNEL_ORDER:
        return [single]

    found: list[str] = []

    def add_many(channels: list[str]) -> None:
        for channel in channels:
            if channel not in found:
                found.append(channel)

    # 2) 総括本文
    add_many(_scan_channel_text(evidence))

    # 3) 本文が薄いときだけ既知ユーザの上書き
    username = str(record.get("username") or "").lower()
    if not found and username in CHANNEL_OVERRIDES:
        add_many(CHANNEL_OVERRIDES[username])

    # 4) categories → bio
    if not found:
        for item in split_csv(record.get("categories", "")):
            if item in {"street", "online", "club", "other", "unknown"}:
                if item not in found:
                    found.append(item)

    if not found:
        profile_text = " ".join(
            str(record.get(key) or "") for key in ("bio", "display_name")
        )
        add_many(_scan_channel_text(profile_text))

    # 4b) 「パス」だけで other になった場合、categories / override を優先
    if found == ["other"]:
        cat_main = [
            item
            for item in split_csv(record.get("categories", ""))
            if item in {"street", "online", "club"}
        ]
        if cat_main:
            found = cat_main
        elif username in CHANNEL_OVERRIDES:
            found = list(CHANNEL_OVERRIDES[username])

    # 5) 既知ユーザは本文が path のみ等で other になった場合の補正
    if username in CHANNEL_OVERRIDES:
        if not found or found == ["other"] or found == ["unknown"]:
            found = list(CHANNEL_OVERRIDES[username])

    return finalize(found, sort_text=evidence)


def build_channel_badges_html(record: dict) -> str:
    channels = infer_channels(record)
    badges = ""
    for channel in channels:
        label = CATEGORY_LABELS.get(channel, channel)
        badges += f'<span class="badge badge-cat-{channel}">{label}</span> '
    return badges.strip()


def channels_data_attr(record: dict) -> str:
    return " ".join(infer_channels(record))


def get_period_evidence_url(record: dict) -> str:
    for field in ("evidence_url", "tweet_url"):
        url = record.get(field, "")
        if url and "/status/" in url:
            return url
    return ""


def is_profile_derived_record(record: dict) -> bool:
    if record.get("source_type") == "profile_derived":
        return True
    match_source = record.get("match_source", "")
    if isinstance(match_source, str) and match_source.startswith("profile_"):
        return True
    return bool(record.get("needs_review"))


def get_profile_source_label(record: dict) -> str:
    source_field = record.get("profile_source_field", "")
    return {
        "bio": "bio",
        "location": "location",
        "display_name": "display_name",
    }.get(source_field, "profile")


# 連続達成のしきい値（高いほど「すごい」側。同じ月数なら最大しきい値だけ表示）
STREAK_THRESHOLDS = (5, 10, 15, 20, 30, 35, 40, 45, 50)


def load_all_monthly_counts(data_dir: str = "data") -> dict[str, dict[tuple[int, int], int]]:
    """全 monthly_YYYY_MM.json から username -> {(y,m): count} を構築。"""
    import glob

    history: dict[str, dict[tuple[int, int], int]] = {}
    for path in sorted(glob.glob(os.path.join(data_dir, "monthly_20*.json"))):
        base = os.path.basename(path)
        # monthly_2026_07.json
        parts = base.replace("monthly_", "").replace(".json", "").split("_")
        if len(parts) != 2:
            continue
        try:
            year, month = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                rows = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(rows, list):
            continue
        for row in rows:
            username = str(row.get("username") or "").lower()
            if not username:
                continue
            count = int(row.get("monthly_count") or 0)
            if count <= 0:
                continue
            bucket = history.setdefault(username, {})
            key = (year, month)
            prev = bucket.get(key, 0)
            if count > prev:
                bucket[key] = count
    return history


def consecutive_month_streaks(
    month_counts: dict[tuple[int, int], int],
    year: int,
    month: int,
    thresholds: tuple[int, ...] = STREAK_THRESHOLDS,
) -> dict[int, int]:
    """対象月を終点に、各しきい値以上を何ヶ月連続で達成しているか。

    その月のデータが無い／しきい値未満で途切れる。
    """
    result: dict[int, int] = {}
    for thr in thresholds:
        streak = 0
        y, m = year, month
        for _ in range(240):  # 最大20年
            if int(month_counts.get((y, m), 0)) < thr:
                break
            streak += 1
            m -= 1
            if m < 1:
                m = 12
                y -= 1
        result[thr] = streak
    return result


def format_streak_label(streaks: dict[int, int]) -> str:
    """表示用の連続達成ラベル。

    - 1ヶ月は出さない（2ヶ月以上のみ）
    - 同じ連続月数なら最大しきい値だけ
      例: 5/10/15/20 が全部4ヶ月 → 「20即4ヶ月」
    """
    # nヶ月 -> その月数を満たす最大しきい値
    best_thr_for_months: dict[int, int] = {}
    for thr in STREAK_THRESHOLDS:
        n = int(streaks.get(thr) or 0)
        if n < 2:
            continue
        prev = best_thr_for_months.get(n, 0)
        if thr > prev:
            best_thr_for_months[n] = thr
    if not best_thr_for_months:
        return "-"
    # しきい値が高い順（20 → 15 → 10 → 5）
    parts = [
        f"{thr}即{n}ヶ月"
        for n, thr in sorted(
            best_thr_for_months.items(),
            key=lambda item: -item[1],
        )
    ]
    return " / ".join(parts)


def format_streak_html(streaks: dict[int, int]) -> str:
    label = format_streak_label(streaks)
    if label == "-":
        return '<span style="color:#555">-</span>'
    return (
        '<span style="font-size:0.82em;color:#bbb;line-height:1.35;display:inline-block">'
        + label.replace(" / ", "<br>")
        + "</span>"
    )


def build_period_value_html(record: dict, count_key: str) -> str:
    evidence_html = ""
    evidence_url = get_period_evidence_url(record)
    if evidence_url:
        evidence_html = (
            ' <a href="'
            + evidence_url
            + '" target="_blank" rel="noopener" style="font-size:0.7em;color:#888;text-decoration:none" title="証拠">🔗</a>'
        )

    review_html = ""
    if is_profile_derived_record(record):
        source_label = get_profile_source_label(record)
        review_html = (
            ' <span class="badge badge-review" title="プロフィール由来の推定値'
            + f" ({source_label})"
            + '。公開前に要確認">要確認</span>'
        )

    approximate_suffix = "+" if record.get("approximate") else ""
    return f"{record[count_key]:,}{approximate_suffix}{evidence_html}{review_html}"


def build_user_cell_html(
    username: str,
    display_name: str = "",
    avatar_url: str = "",
    profile_url: str = "",
    extra_html: str = "",
) -> str:
    """アカウント + 表示名を user-cell にまとめて出す。"""
    href = profile_url or f"https://twitter.com/{username}"
    if avatar_url:
        av_html = f'<img class="avatar" src="{avatar_url}" alt="">'
    else:
        av_html = '<div class="avatar avatar-placeholder"></div>'
    name = (display_name or "").strip()
    name_html = (
        f'<div class="user-display-name">{name}</div>' if name else ""
    )
    return (
        f'<td class="user-cell">{av_html}'
        f'<div class="user-info">'
        f'<a href="{href}" target="_blank" rel="noopener">@{username}</a>'
        f"{name_html}{extra_html}"
        f"</div></td>"
    )


def collapse_duplicate_accounts(records: list[dict]) -> list[dict]:
    merged_records = [dict(r) for r in records]
    by_username = {r["username"]: r for r in merged_records}
    hidden_usernames: set[str] = set()

    for duplicate_username, canonical_username in DUPLICATE_ACCOUNT_CANONICALS.items():
        duplicate = by_username.get(duplicate_username)
        canonical = by_username.get(canonical_username)
        if not duplicate or not canonical:
            continue

        canonical["sokusuu"] = max(canonical.get("sokusuu", 0), duplicate.get("sokusuu", 0))
        canonical["categories"] = join_unique_csv(
            canonical.get("categories", ""),
            duplicate.get("categories", ""),
        )
        canonical["alt_accounts"] = join_unique_csv(
            canonical.get("alt_accounts", ""),
            duplicate_username,
            duplicate.get("alt_accounts", ""),
            exclude={canonical_username},
        )
        if not canonical.get("bio"):
            canonical["bio"] = duplicate.get("bio", "")
        if not canonical.get("location"):
            canonical["location"] = duplicate.get("location", "")
        if not canonical.get("profile_image_url"):
            canonical["profile_image_url"] = duplicate.get("profile_image_url", "")
        if not canonical.get("evidence_url"):
            canonical["evidence_url"] = duplicate.get("evidence_url", "")
        canonical["approximate"] = canonical.get("approximate") or duplicate.get("approximate")
        hidden_usernames.add(duplicate_username)

    visible_records = [r for r in merged_records if r["username"] not in hidden_usernames]
    return sorted(
        visible_records,
        key=lambda r: (r.get("sokusuu", 0), r.get("followers_count", 0)),
        reverse=True,
    )


def filter_by_category(records: list[dict], category: str) -> list[dict]:
    """チャネルでフィルタする。'all' なら全件返す。"""
    if category == "all":
        return records
    return [r for r in records if category in infer_channels(r)]


def build_ranking_rows(records: list[dict], show_category: bool = False) -> str:
    """ランキングテーブルの行HTMLを生成する"""
    rows = ""
    for i, r in enumerate(records, 1):
        medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")

        source_badge = (
            '<span class="badge badge-profile">プロフィール</span>'
            if r["source"] == "profile"
            else '<span class="badge badge-pinned">固定ツイート</span>'
        )

        followers = r.get("followers_count", 0)
        followers_str = f"{followers:,}" if followers else "-"

        alt_html = ""
        alt = r.get("alt_accounts", "")
        if alt:
            alt_html = f'<span class="alt-badge">= {alt}</span>'

        avatar_url = r.get("profile_image_url", "")

        cat_html = ""
        if show_category:
            cat_html = f"<td>{build_channel_badges_html(r)}</td>"

        user_cell = build_user_cell_html(
            r["username"],
            r.get("display_name", ""),
            avatar_url,
            r.get("url", ""),
            extra_html=alt_html,
        )
        ch_attr = channels_data_attr(r)
        rows += f"""
            <tr data-channels="{ch_attr}">
                <td class="rank">{medal}{i}</td>
                {user_cell}
                <td class="display-name">{r['display_name']}</td>
                <td class="sokusuu">{r['sokusuu']:,}{"+" if r.get("approximate") else ""}{' <a href="' + r['evidence_url'] + '" target="_blank" rel="noopener" style="font-size:0.7em;color:#888;text-decoration:none" title="証拠">🔗</a>' if r.get('evidence_url') else ''}</td>
                <td>{source_badge}</td>
                {cat_html}
                <td class="followers">{followers_str}</td>
            </tr>"""
    return rows


def generate_html(records: list[dict]) -> str:
    """ランキングHTMLを生成する"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # チャネル別テーブルを生成
    tab_buttons = ""
    tab_contents = ""
    categories = ["all", "street", "online", "club", "other", "unknown"]
    for idx, cat in enumerate(categories):
        label = CATEGORY_LABELS[cat]
        filtered = filter_by_category(records, cat)
        active = active_class(cat)
        count = len(filtered)

        tab_buttons += f'        <div class="tab{active}" onclick="switchTab(\'{cat}\')">{label} ({count})</div>\n'

        show_cat = cat == "all"
        cat_header = CHANNEL_COL_TH if show_cat else ''
        rows = build_ranking_rows(filtered, show_category=show_cat)

        tab_contents += f"""
    <div id="tab-{cat}" class="tab-content{active}">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>即数</th>
                    <th>ソース</th>
                    {cat_header}
                    <th>フォロワー</th>
                </tr>
            </thead>
            <tbody>{rows}
            </tbody>
        </table>
    </div>
"""

    # フォロワー数ランキング
    followers_sorted = sorted(records, key=lambda r: r.get("followers_count", 0), reverse=True)
    followers_rows = ""
    rank = 0
    for r in followers_sorted:
        followers = r.get("followers_count", 0)
        if followers == 0:
            continue
        rank += 1
        medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(rank, "")
        user_cell = build_user_cell_html(
            r["username"],
            r.get("display_name", ""),
            r.get("profile_image_url", ""),
        )
        followers_rows += f"""
            <tr>
                <td class="rank">{medal}{rank}</td>
                {user_cell}
                <td class="display-name">{r['display_name']}</td>
                <td class="followers">{followers:,}</td>
                <td class="sokusuu">{r['sokusuu']:,}</td>
            </tr>"""

    followers_active = active_class("followers")
    tab_buttons += f'        <div class="tab{followers_active}" onclick="switchTab(\'followers\')">フォロワー数</div>\n'
    tab_contents += f"""
    <div id="tab-followers" class="tab-content{followers_active}">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>フォロワー</th>
                    <th>即数</th>
                </tr>
            </thead>
            <tbody>{followers_rows}
            </tbody>
        </table>
    </div>
"""

    # 月間ランキング
    monthly_file = "data/monthly_ranking.json"
    if SHOW_PERIOD_TABS and os.path.exists(monthly_file):
        with open(monthly_file, "r", encoding="utf-8") as f:
            monthly_data = json.load(f)
        monthly_rows = ""
        for i, r in enumerate(monthly_data, 1):
            medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")
            achieved_m = r.get("achieved_date")
            date_str = f'<span style="color:#888">{achieved_m}</span>' if achieved_m else '<span style="color:#444">-</span>'
            user_cell = build_user_cell_html(
                r["username"],
                r.get("display_name", ""),
                r.get("profile_image_url", ""),
            )
            monthly_rows += f"""
            <tr>
                <td class="rank">{medal}{i}</td>
                {user_cell}
                <td class="display-name">{r.get('display_name', '')}</td>
                <td class="sokusuu">{build_period_value_html(r, 'monthly_best')}</td>
                <td>{date_str}</td>
            </tr>"""

        monthly_active = active_class("monthly")
        tab_buttons += f'        <div class="tab{monthly_active}" onclick="switchTab(\'monthly\')">月間記録 ({len(monthly_data)})</div>\n'
        tab_contents += f"""
    <div id="tab-monthly" class="tab-content{monthly_active}">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>月間最多</th>
                    <th>達成時期</th>
                </tr>
            </thead>
            <tbody>{monthly_rows}
            </tbody>
        </table>
    </div>
"""

    # 年間ランキング
    yearly_file = "data/yearly_ranking.json"
    if SHOW_PERIOD_TABS and os.path.exists(yearly_file):
        with open(yearly_file, "r", encoding="utf-8") as f:
            yearly = json.load(f)
        yearly_rows = ""
        for i, r in enumerate(yearly, 1):
            medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")
            achieved = r.get("achieved_year")
            year_str = f'<span style="color:#888">{achieved}年</span>' if achieved else '<span style="color:#444">-</span>'
            user_cell = build_user_cell_html(
                r["username"],
                r.get("display_name", ""),
                r.get("profile_image_url", ""),
            )
            yearly_rows += f"""
            <tr>
                <td class="rank">{medal}{i}</td>
                {user_cell}
                <td class="display-name">{r.get('display_name', '')}</td>
                <td class="sokusuu">{build_period_value_html(r, 'yearly_best')}</td>
                <td>{year_str}</td>
            </tr>"""

        yearly_active = active_class("yearly")
        tab_buttons += f'        <div class="tab{yearly_active}" onclick="switchTab(\'yearly\')">年間記録 ({len(yearly)})</div>\n'
        tab_contents += f"""
    <div id="tab-yearly" class="tab-content{yearly_active}">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>年間最多</th>
                    <th>達成年</th>
                </tr>
            </thead>
            <tbody>{yearly_rows}
            </tbody>
        </table>
    </div>
"""

    import glob

    yearly_files = (
        sorted(glob.glob("data/yearly_20*.json"), reverse=True)
        if SHOW_PERIOD_TABS and SHOW_PERIOD_DETAIL_TABS
        else []
    )
    yearly_divs = ""
    yearly_options = ""
    first_year_id = ""

    for yf in yearly_files:
        basename = os.path.basename(yf)
        try:
            y_year = int(basename.replace("yearly_", "").replace(".json", ""))
        except ValueError:
            continue

        with open(yf, "r", encoding="utf-8") as f:
            y_data = json.load(f)
        if not y_data:
            continue

        year_id = "y" + str(y_year)
        is_first = not first_year_id
        if is_first:
            first_year_id = year_id

        y_rows = ""
        for i, r in enumerate(y_data, 1):
            medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")
            y_rows += '<tr data-channels="' + channels_data_attr(r) + '">'
            y_rows += '<td class="rank">' + medal + str(i) + '</td>'
            y_rows += build_user_cell_html(
                r["username"],
                r.get("display_name", ""),
                r.get("profile_image_url", ""),
            )
            y_rows += '<td class="display-name">' + r.get('display_name', '') + '</td>'
            y_rows += '<td class="sokusuu">' + build_period_value_html(r, "yearly_count") + '</td>'
            y_rows += '<td>' + build_channel_badges_html(r) + '</td>'
            y_rows += '</tr>'

        display = "block" if is_first else "none"
        yearly_divs += (
            '<div id="yearly-' + year_id + '" style="display:' + display + '">'
            '<table><thead><tr><th>#</th><th>アカウント</th><th>表示名</th><th>即数</th>'
            + CHANNEL_COL_TH
            + '</tr></thead><tbody>' + y_rows + '</tbody></table></div>'
        )
        selected = " selected" if is_first else ""
        # 月次合算の上半期などは period / period_label を優先して表示名を変える
        period = (y_data[0].get("period") or "") if y_data else ""
        period_label = (y_data[0].get("period_label") or "") if y_data else ""
        if period == "h1" or period_label == "上半期":
            year_title = f"{y_year}年上半期"
            year_note = "月次合算"
        else:
            year_title = f"{y_year}年"
            year_note = "集計中"
        yearly_options += (
            '<option value="'
            + year_id
            + '"'
            + selected
            + ">"
            + year_title
            + " ("
            + str(len(y_data))
            + "件・"
            + year_note
            + ")</option>"
        )

    if yearly_divs:
        yearlyselect_active = active_class("yearlyselect")
        tab_buttons += f'        <div class="tab{yearlyselect_active}" onclick="switchTab(\'yearlyselect\')">年別</div>\n'
        tab_contents += """
    <div id="tab-yearlyselect" class="tab-content""" + yearlyselect_active + """">
        <div style="text-align:center;margin-bottom:15px">
            <select id="yearlySelect" onchange="switchYearly()" style="padding:8px 16px;border:1px solid #333;border-radius:8px;background:#1a1a1a;color:#e0e0e0;font-size:0.95em">
                """ + yearly_options + """
            </select>
        </div>
        """ + yearly_divs + """
    </div>
"""

    # 月別ランキング（1つのタブ内でセレクトボックス切り替え）
    monthly_files = (
        sorted(glob.glob("data/monthly_20*.json"), reverse=True)
        if SHOW_PERIOD_TABS and SHOW_PERIOD_DETAIL_TABS
        else []
    )
    monthly_divs = ""
    monthly_options = ""
    first_month_id = ""
    default_month_id = normalize_month_id(DEFAULT_MONTH)
    monthly_history = load_all_monthly_counts("data")

    for mf in monthly_files:
        basename = os.path.basename(mf)
        parts = basename.replace("monthly_", "").replace(".json", "").split("_")
        if len(parts) != 2:
            continue
        m_year, m_month = int(parts[0]), int(parts[1])

        with open(mf, "r", encoding="utf-8") as f:
            m_data = json.load(f)
        if not m_data:
            continue

        month_id = "m" + str(m_year) + str(m_month).zfill(2)
        is_first = not first_month_id
        if is_first:
            first_month_id = month_id
        is_default_month = month_id == default_month_id if default_month_id else is_first

        # 月間4即以下は月別表に載せない（5即以上のみ）
        ranked_data = [
            r for r in m_data if int(r.get("monthly_count") or 0) >= 5
        ]
        ranked_data = sorted(
            ranked_data,
            key=lambda r: (
                -int(r.get("monthly_count") or 0),
                (r.get("username") or "").lower(),
            ),
        )

        m_rows = ""
        for i, r in enumerate(ranked_data, 1):
            medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")
            username_key = str(r.get("username") or "").lower()
            streaks = consecutive_month_streaks(
                monthly_history.get(username_key, {}),
                m_year,
                m_month,
            )
            m_rows += '<tr data-channels="' + channels_data_attr(r) + '">'
            m_rows += '<td class="rank">' + medal + str(i) + '</td>'
            m_rows += build_user_cell_html(
                r["username"],
                r.get("display_name", ""),
                r.get("profile_image_url", ""),
            )
            m_rows += '<td class="display-name">' + r.get('display_name', '') + '</td>'
            m_rows += '<td class="sokusuu">' + build_period_value_html(r, "monthly_count") + '</td>'
            m_rows += '<td>' + build_channel_badges_html(r) + '</td>'
            m_rows += '<td class="streak">' + format_streak_html(streaks) + '</td>'
            m_rows += '</tr>'

        display = "block" if is_default_month else "none"
        monthly_divs += (
            '<div id="monthly-' + month_id + '" style="display:' + display + '">'
            '<table><thead><tr><th>#</th><th>アカウント</th><th>表示名</th><th>即数</th>'
            + CHANNEL_COL_TH
            + '<th title="各しきい値以上を何ヶ月連続（当月終点）">'
            "連続</th></tr></thead><tbody>"
            + m_rows
            + '</tbody></table></div>'
        )
        selected = " selected" if is_default_month else ""
        monthly_options += (
            '<option value="' + month_id + '"' + selected + '>'
            + str(m_year) + '年' + str(m_month) + '月 ('
            + "5即以上)</option>"
        )

    if monthly_divs:
        monthlyselect_active = active_class("monthlyselect")
        tab_buttons += f'        <div class="tab{monthlyselect_active}" onclick="switchTab(\'monthlyselect\')">月別</div>\n'
        tab_contents += """
    <div id="tab-monthlyselect" class="tab-content""" + monthlyselect_active + """">
        <div style="text-align:center;margin-bottom:15px">
            <select id="monthlySelect" onchange="switchMonthly()" style="padding:8px 16px;border:1px solid #333;border-radius:8px;background:#1a1a1a;color:#e0e0e0;font-size:0.95em">
                """ + monthly_options + """
            </select>
        </div>
        """ + monthly_divs + """
    </div>
"""

    # 統計
    max_sokusuu = records[0]['sokusuu'] if records else 0
    avg_sokusuu = sum(r['sokusuu'] for r in records) // len(records) if records else 0
    sorted_vals = sorted(r['sokusuu'] for r in records)
    n = len(sorted_vals)
    median_sokusuu = (sorted_vals[n//2-1] + sorted_vals[n//2]) // 2 if n % 2 == 0 else sorted_vals[n//2] if n else 0

    # 分布タブ
    ranges = [
        (1000, float('inf'), '1000+'),
        (500, 999, '500-999'),
        (300, 499, '300-499'),
        (200, 299, '200-299'),
        (100, 199, '100-199'),
        (50, 99, '50-99'),
        (10, 49, '10-49'),
    ]
    max_count = 0
    dist_data = []
    for lo, hi, label in ranges:
        count = sum(1 for r in records if lo <= r['sokusuu'] <= hi)
        dist_data.append((label, count))
        if count > max_count:
            max_count = count

    dist_bars = ""
    for label, count in dist_data:
        pct = (count / max_count * 100) if max_count else 0
        dist_bars += f"""
        <div style="display:flex;align-items:center;gap:10px;margin:8px 0">
            <div style="width:80px;text-align:right;color:#aaa;font-size:0.9em">{label}</div>
            <div style="flex:1;background:#252525;border-radius:4px;overflow:hidden;height:28px">
                <div style="width:{pct:.0f}%;background:#ff6b6b;height:100%;border-radius:4px;min-width:2px"></div>
            </div>
            <div style="width:50px;color:#e0e0e0;font-weight:bold;font-size:0.9em">{count}件</div>
        </div>"""

    # カテゴリ別分布
    cat_colors = {
        "street": "#60a5fa",
        "online": "#2dd4bf",
        "club": "#c084fc",
        "other": "#fbbf24",
        "unknown": "#9ca3af",
    }
    cat_dist = ""
    for cat in CHANNEL_ORDER:
        cat_label = CATEGORY_LABELS[cat]
        cat_count = len([r for r in records if cat in infer_channels(r)])
        cat_pct = (cat_count / len(records) * 100) if records else 0
        color = cat_colors[cat]
        cat_dist += f"""
        <div style="display:flex;align-items:center;gap:10px;margin:8px 0">
            <div style="width:100px;text-align:right;color:#aaa;font-size:0.9em">{cat_label}</div>
            <div style="flex:1;background:#252525;border-radius:4px;overflow:hidden;height:28px">
                <div style="width:{cat_pct:.0f}%;background:{color};height:100%;border-radius:4px;min-width:2px"></div>
            </div>
            <div style="width:80px;color:#e0e0e0;font-size:0.9em">{cat_count}件 ({cat_pct:.0f}%)</div>
        </div>"""

    dist_active = active_class("dist")
    tab_buttons += f'        <div class="tab{dist_active}" onclick="switchTab(\'dist\')">分布</div>\n'
    tab_contents += f"""
    <div id="tab-dist" class="tab-content{dist_active}">
        <div style="background:#1a1a1a;border-radius:12px;padding:20px;margin-bottom:20px">
            <h3 style="color:#fff;margin-bottom:15px">即数分布</h3>
            {dist_bars}
        </div>
        <div style="background:#1a1a1a;border-radius:12px;padding:20px">
            <h3 style="color:#fff;margin-bottom:15px">チャネル分布（ストナン/ネトナン/箱/その他/謎）</h3>
            {cat_dist}
        </div>
    </div>
"""

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>即数ランキング</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f;
            color: #e0e0e0;
            padding: 20px;
            max-width: 1000px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            font-size: 2em;
            margin: 20px 0 5px;
            color: #fff;
        }}
        .subtitle {{
            text-align: center;
            color: #888;
            margin-bottom: 30px;
            font-size: 0.9em;
        }}
        .stats {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 12px;
            padding: 15px 25px;
            text-align: center;
        }}
        .stat-card .number {{
            font-size: 2em;
            font-weight: bold;
            color: #ff6b6b;
        }}
        .stat-card .label {{
            font-size: 0.85em;
            color: #888;
            margin-top: 5px;
        }}
        .tabs {{
            display: flex;
            gap: 8px;
            margin-bottom: 20px;
            justify-content: center;
            flex-wrap: wrap;
        }}
        .tab {{
            padding: 8px 18px;
            border: 1px solid #333;
            border-radius: 8px;
            background: #1a1a1a;
            color: #aaa;
            cursor: pointer;
            font-size: 0.9em;
            transition: all 0.2s;
        }}
        .tab:hover {{ border-color: #555; color: #fff; }}
        .tab.active {{ background: #ff6b6b; border-color: #ff6b6b; color: #fff; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #1a1a1a;
            border-radius: 12px;
            overflow: hidden;
        }}
        th {{
            background: #252525;
            padding: 12px 15px;
            text-align: left;
            font-weight: 600;
            color: #aaa;
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        td {{
            padding: 12px 15px;
            border-top: 1px solid #2a2a2a;
        }}
        tr:hover {{ background: #222; }}
        .rank {{ font-weight: bold; width: 60px; color: #fff; }}
        .user-cell {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .avatar {{
            width: 36px;
            height: 36px;
            border-radius: 50%;
            flex-shrink: 0;
            object-fit: cover;
        }}
        .avatar-placeholder {{
            background: #333;
        }}
        .user-info a {{ color: #1d9bf0; text-decoration: none; }}
        .user-info a:hover {{ text-decoration: underline; }}
        .user-display-name {{
            color: #ccc;
            font-size: 0.85em;
            margin-top: 2px;
            line-height: 1.3;
            word-break: break-word;
        }}
        .display-name {{ color: #999; font-size: 0.9em; }}
        .sokusuu {{ font-weight: bold; color: #ff6b6b; font-size: 1.1em; }}
        .followers {{ font-weight: bold; color: #1d9bf0; }}
        .badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 0.75em;
            font-weight: 600;
            margin-right: 3px;
        }}
        .badge-profile {{ background: #1a3a2a; color: #4ade80; }}
        .badge-pinned {{ background: #3a2a1a; color: #fbbf24; }}
        .badge-review {{ background: #3a1f1f; color: #fca5a5; }}
        .badge-cat-street {{ background: #1a2a3a; color: #60a5fa; }}
        .badge-cat-online {{ background: #1a3a3a; color: #2dd4bf; }}
        .badge-cat-club {{ background: #2a1a3a; color: #c084fc; }}
        .badge-cat-other {{ background: #3a2f1a; color: #fbbf24; }}
        .badge-cat-unknown {{ background: #2a2a2a; color: #9ca3af; }}
        .badge-cat-none {{ background: #2a2a2a; color: #666; }}
        .alt-badge {{
            display: block;
            font-size: 0.75em;
            color: #888;
            margin-top: 2px;
        }}
        .disclaimer {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 15px;
            margin-top: 30px;
            font-size: 0.8em;
            color: #888;
        }}
        .footer {{
            text-align: center;
            color: #555;
            margin-top: 40px;
            font-size: 0.8em;
            line-height: 1.8;
        }}
        @media (max-width: 600px) {{
            body {{ padding: 10px; }}
            .stats {{ flex-direction: column; align-items: center; }}
            th, td {{ padding: 8px 10px; font-size: 0.85em; }}
            /* 表示名カラムは狭いので隠すが、user-cell 内の表示名は残す */
            th:nth-child(3),
            td.display-name {{ display: none; }}
            .tabs {{ gap: 5px; }}
            .tab {{ padding: 6px 12px; font-size: 0.8em; }}
        }}
    </style>
</head>
<body>
    <h1>即数ランキング</h1>
    <p class="subtitle">最終更新: {now}</p>

    <div class="stats">
        <div class="stat-card">
            <div class="number">{len(records)}</div>
            <div class="label">集計数</div>
        </div>
        <div class="stat-card">
            <div class="number">{max_sokusuu:,}</div>
            <div class="label">最高即数</div>
        </div>
        <div class="stat-card">
            <div class="number">{avg_sokusuu:,}</div>
            <div class="label">平均即数</div>
        </div>
        <div class="stat-card">
            <div class="number">{median_sokusuu:,}</div>
            <div class="label">中央値</div>
        </div>
    </div>

    <div style="text-align:center;margin-bottom:20px">
        <input type="text" id="searchBox" placeholder="ユーザー名で検索..." oninput="filterRows()"
            style="padding:8px 16px;border:1px solid #333;border-radius:8px;background:#1a1a1a;color:#e0e0e0;font-size:0.95em;width:300px;outline:none;">
    </div>

    <div class="tabs">
{tab_buttons}    </div>

{tab_contents}

    <div class="disclaimer">
        <strong>注意事項:</strong>
        即数は全て自己申告ベースであり、正確性は保証されません。
        プロフィールおよび固定ツイートから自動抽出した値です。
        チャネル（ストナン / ネトナン / 箱 / その他 / 謎）はプロフィール・総括ツイートのキーワードから自動判定しています。
        複数チャネルに当てはまる場合は複数表示され、総括内訳の件数が多い順に並べます。判定できない場合は「謎」です。
    </div>

    <div class="footer">
        <p>Data collected from X (Twitter) profiles and pinned tweets</p>
        <p>Built with Python</p>
    </div>

    <script>
        function switchTab(tab) {{
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
            document.getElementById('tab-' + tab).classList.add('active');
            const clicked = (typeof event !== 'undefined' && event.target) ? event.target : null;
            if (clicked) clicked.classList.add('active');
            document.getElementById('searchBox').value = '';
            filterRows();
        }}
        function switchMonthly() {{
            const sel = document.getElementById('monthlySelect');
            const val = sel.value;
            document.querySelectorAll('[id^="monthly-m"]').forEach(el => el.style.display = 'none');
            const target = document.getElementById('monthly-' + val);
            if (target) target.style.display = 'block';
        }}
        function switchYearly() {{
            const sel = document.getElementById('yearlySelect');
            const val = sel.value;
            document.querySelectorAll('[id^="yearly-y"]').forEach(el => el.style.display = 'none');
            const target = document.getElementById('yearly-' + val);
            if (target) target.style.display = 'block';
        }}
        function filterRows() {{
            const q = document.getElementById('searchBox').value.toLowerCase();
            const active = document.querySelector('.tab-content.active');
            if (!active) return;
            // 表示中のテーブルだけ検索（月別/年別は visible な子テーブル）
            const tables = [];
            active.querySelectorAll('table').forEach(table => {{
                const wrap = table.closest('[id^="monthly-"], [id^="yearly-"]');
                if (wrap && wrap.style.display === 'none') return;
                tables.push(table);
            }});
            if (!tables.length) {{
                active.querySelectorAll('tbody tr').forEach(tr => {{
                    const text = tr.textContent.toLowerCase();
                    tr.style.display = text.includes(q) ? '' : 'none';
                }});
                return;
            }}
            tables.forEach(table => {{
                table.querySelectorAll('tbody tr').forEach(tr => {{
                    const text = tr.textContent.toLowerCase();
                    tr.style.display = text.includes(q) ? '' : 'none';
                }});
            }});
        }}
    </script>
</body>
</html>"""
    return html


def main():
    records = load_data(INPUT_JSON)
    if not records:
        return
    records = collapse_duplicate_accounts(records)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html = generate_html(records)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    # チャネル別件数を表示
    all_count = len(records)
    counts = {c: len(filter_by_category(records, c)) for c in CHANNEL_ORDER}
    print(f"[OUTPUT] {OUTPUT_HTML} を生成しました")
    print(
        f"  総合: {all_count} 名"
        f" / ストナン: {counts['street']}"
        f" / ネトナン: {counts['online']}"
        f" / 箱: {counts['club']}"
        f" / その他: {counts['other']}"
        f" / 謎: {counts['unknown']}"
    )


if __name__ == "__main__":
    main()
