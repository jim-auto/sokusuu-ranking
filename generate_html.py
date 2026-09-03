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
# 年別 / 月別タブは集計がまだ薄いので公開しない。戻すときは True。
SHOW_PERIOD_DETAIL_TABS = env_flag("SHOW_PERIOD_DETAIL_TABS", default=False)
DEFAULT_TAB = os.getenv("DEFAULT_TAB", "all").strip() or "all"
DEFAULT_MONTH = os.getenv("DEFAULT_MONTH", "").strip()

# 年別 / 月別を一時的に戻す場合でも、この年は出さない。
HIDDEN_YEARLY_YEARS = {2025}
HIDDEN_MONTHLY_YEARS = {2026}

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
# 表・バッジ用の短い表示名（件数付き内訳で使う）
CHANNEL_SHORT_LABELS = {
    "street": "スト",
    "online": "ネト",
    "club": "箱",
    "other": "その他",
    "unknown": "謎",
}
# 絵文字 → チャネル（総括内訳）。表示は「実際に使われた絵文字」を優先する
EMOJI_TO_CHANNEL: dict[str, str] = {
    "🐶": "street",
    "🦁": "street",
    "🦉": "street",
    "🏪": "street",
    "🦐": "street",
    "Ⓜ": "street",
    "Ⓜ️": "street",
    "🍐": "online",
    "🍎": "online",
    "🔥": "online",
    "🗼": "online",
    "🍛": "online",
    "🪩": "online",
    "🦾": "club",
    "🧚": "club",
    "📦": "club",
    "🟦": "club",
    "⬛": "club",
    "⬛\ufe0f": "club",
    "⬜": "club",
    "⬜\ufe0f": "club",
    "◼": "club",
    "◾": "club",
    "▪": "club",
    "◻": "club",
    "◽": "club",
    "🥂": "other",
    "💯": "other",  # 黎さん総括など（🦉/🐶 と並ぶ内訳）
}
CHANNEL_ORDER = ["street", "online", "club", "other", "unknown"]
# 表ヘッダ: 複数チャネル時は総括内訳の件数が多い順
CHANNEL_COL_HEADER = "チャネル（即数が多い順）"
CHANNEL_COL_TH = (
    f'<th title="総括本文の内訳。件数付きは実際の絵文字ごと（例: ネト（🍐5・🍎6・🔥2））。'
    f'キーワードのみのときは スト（9）">'
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
    # kent_o_o: 総括が「N節/N即」のみでチャネル根拠なし → 本文推定に任せ謎にする
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
    # 強欲: 月次は「今月N即目」のみ。ネト＋パス（表示はネトナン/その他）
    "greed_pua": ["online", "other"],
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


def _normalize_emoji_token(token: str) -> str:
    """絵文字トークンをマップ照合用に正規化。"""
    if not token:
        return ""
    # VS16 付き/なし両対応
    bare = token.replace("\ufe0f", "")
    if token in EMOJI_TO_CHANNEL:
        return token
    if bare in EMOJI_TO_CHANNEL:
        return bare
    if bare + "\ufe0f" in EMOJI_TO_CHANNEL:
        return bare + "\ufe0f"
    return token


def parse_channel_breakdown(text: str) -> dict[str, dict]:
    """総括本文からチャネル内訳を解析する。

    戻り値:
      {
        "street": {"total": 6, "emojis": {"🐶": 5, "🦐": 1}, "keyword": 0},
        ...
      }

    ルール:
      - 絵文字は「実際に使われたもの」ごとに集計
      - 数量は 🐶5 / 🐶×3 / 🦾:7 のみ（🔥  18 のような年齢は数量にしない）
      - 🐶&🏪:10 や 🗼🍛x2 はまとめて1回だけ加点
      - 絵文字内訳があるチャネルはキーワード件数を足さない（二重計上防止）
    """
    empty = {
        c: {"total": 0, "emojis": {}, "keyword": 0} for c in CHANNEL_ORDER if c != "unknown"
    }
    if not text or not str(text).strip():
        return empty

    raw = str(text)
    # 🥂📦 は箱ではなくその他（キャバ等）
    for m in list(re.finditer(r"🥂\s*📦\s*[：:／/×xｘ*]?\s*(\d+)?", raw)):
        qty = int(m.group(1)) if m.group(1) else 1
        empty["other"]["emojis"]["🥂📦"] = empty["other"]["emojis"].get("🥂📦", 0) + qty
        empty["other"]["total"] += qty
        raw = raw[: m.start()] + (" " * (m.end() - m.start())) + raw[m.end() :]

    emoji_alts = sorted(EMOJI_TO_CHANNEL.keys(), key=len, reverse=True)
    emoji_re = "|".join(re.escape(e) for e in emoji_alts)
    consumed: list[tuple[int, int]] = []
    # 明示数量付きで確定した絵文字（🦉7 など）。ケース行の単独 🦉弾 は二重計上しない
    explicit_emojis: set[str] = set()

    def _overlap(a: int, b: int) -> bool:
        return any(not (b <= s or a >= e) for s, e in consumed)

    def _add_group(
        tokens: list[str],
        qty: int,
        start: int,
        end: int,
        *,
        explicit: bool = False,
    ) -> None:
        if _overlap(start, end) or qty <= 0:
            return
        norms: list[str] = []
        channels_in_group: list[str] = []
        for tok in tokens:
            norm = _normalize_emoji_token(tok)
            ch = EMOJI_TO_CHANNEL.get(norm) or EMOJI_TO_CHANNEL.get(tok)
            if not ch:
                continue
            norms.append(norm)
            if ch not in channels_in_group:
                channels_in_group.append(ch)
        if not norms or not channels_in_group:
            return
        # ケース行の単独カウントは、同じ絵文字の明示集計があるときスキップ
        if not explicit and any(n in explicit_emojis for n in norms):
            consumed.append((start, end))
            return
        primary = channels_in_group[0]
        head = norms[0]
        empty[primary]["emojis"][head] = empty[primary]["emojis"].get(head, 0) + qty
        empty[primary]["total"] += qty
        if explicit:
            for n in norms:
                explicit_emojis.add(n)
        consumed.append((start, end))

    # 1) 🐶&🏪:10 / 🐶＆🏪×10
    for m in re.finditer(
        rf"((?:{emoji_re})(?:\s*[&＆]\s*(?:{emoji_re}))+)\s*[:：×xｘ*]\s*(\d+)",
        raw,
    ):
        tokens = re.findall(emoji_re, m.group(1))
        _add_group(tokens, int(m.group(2)), m.start(), m.end(), explicit=True)

    # 2) 連続絵文字 + ×N（🗼🍛x2）
    for m in re.finditer(rf"((?:{emoji_re}){{2,}})\s*[×xｘ*]\s*(\d+)", raw):
        tokens = re.findall(emoji_re, m.group(1))
        _add_group(tokens, int(m.group(2)), m.start(), m.end(), explicit=True)

    # 3) 単体絵文字 + 数量（直結 / × / :）or 数量なし=1
    #    空白+数字は年齢なので数量にしない（🔥  18 売り子）
    #    数量なしの単独絵文字は「リスト行」（行頭 or 直後が /）だけ数える
    #    → 帰省🟦・🦾&🏪で 等の装飾を即数にしない
    #    ヘッダ集計 🦉7 🐶1 は explicit（ケース行 🦉弾 と二重計上しない）
    for m in re.finditer(
        rf"({emoji_re})(?:(\d+)|(?:\s*[×xｘ*]\s*(\d+))|(?:\s*[:：]\s*(\d+)))?",
        raw,
    ):
        if _overlap(m.start(), m.end()):
            continue
        qty_raw = m.group(2) or m.group(3) or m.group(4)
        if qty_raw:
            qty = int(qty_raw)
            _add_group([m.group(1)], qty, m.start(), m.end(), explicit=True)
            continue
        nxt = raw[m.end() : m.end() + 1]
        after2 = raw[m.end() : m.end() + 2]
        if nxt in {"&", "＆"}:
            continue
        # リスト行: 行頭 / 直後スラッシュ / 弾・準・即 など
        line_prefix = raw[max(0, raw.rfind("\n", 0, m.start()) + 1) : m.start()]
        at_line_start = line_prefix.strip() == ""
        list_like = nxt in {"/", "／"} or after2.startswith(
            ("弾", "準", "即", "そ", "節", "×", "ｘ", "x")
        )
        if not at_line_start and not list_like:
            continue
        _add_group([m.group(1)], 1, m.start(), m.end(), explicit=False)

    # キーワード件数
    keyword = {c: 0 for c in empty}

    for ch, pats in (
        (
            "street",
            [
                r"(?<![イアウ])スト\s*[×xｘ*：:／/]?\s*(\d+)\s*(?:即|節)?",
                r"弾丸\s*(\d+)\s*準(?:即)?\s*(\d+)",
                r"弾丸\s*[×xｘ*]?\s*(\d+)",
            ],
        ),
        (
            "online",
            [
                r"(?<![ア-ン])ネト\s*[×xｘ*：:／/]?\s*(\d+)\s*(?:即|節)?",
            ],
        ),
        (
            "club",
            [
                r"(?<![ア-ン])箱\s*[×xｘ*：:／/]?\s*(\d+)\s*(?:即|節)?",
                r"クラブ\s*[×xｘ*]?\s*(\d+)",
            ],
        ),
        (
            "other",
            [
                r"パス(?!ワード)\s*[×xｘ*]?\s*(\d+)",
                r"紹介\s*[×xｘ*]?\s*(\d+)\s*(?:節|即)?",
                r"その他\s*[×xｘ*：:／/]?\s*(\d+)",
                r"合コン\s*[×xｘ*]?\s*(\d+)",
            ],
        ),
    ):
        for pat in pats:
            for m in re.finditer(pat, raw):
                try:
                    vals = [int(g) for g in m.groups() if g is not None]
                    if vals:
                        keyword[ch] = max(keyword[ch], sum(vals))
                except ValueError:
                    pass

    # 数量なしパスの個数（「1.パス」「パス(強欲)」など）
    if keyword["other"] == 0:
        path_hits = len(re.findall(r"パス(?!ワード)", raw))
        if path_hits:
            keyword["other"] = path_hits

    app_total = 0
    for m in re.finditer(r"(?:某\s*app|使用\s*APP)\s*[×xｘ*]?\s*(\d+)", raw, re.I):
        app_total += int(m.group(1))
    if app_total:
        keyword["online"] = max(keyword["online"], app_total)

    for ch, data in empty.items():
        data["keyword"] = int(keyword.get(ch) or 0)
        # 明示キーワード（スト17即）が絵文字装飾より大きいときはキーワードを優先
        if data["keyword"] > data["total"]:
            data["emojis"] = {}
            data["total"] = data["keyword"]
        elif data["total"] <= 0 and data["keyword"] > 0:
            data["total"] = data["keyword"]

    return empty


def estimate_channel_counts(text: str) -> dict[str, int]:
    """総括本文からチャネル別の件数をざっくり数える（表示順用）。"""
    breakdown = parse_channel_breakdown(text)
    counts = {c: 0 for c in CHANNEL_ORDER}
    for ch, data in breakdown.items():
        counts[ch] = int(data.get("total") or 0)
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
    #    （ネト+その他 のように other を意図的に併記するケースは drop しない）
    username = str(record.get("username") or "").lower()
    if not found and username in CHANNEL_OVERRIDES:
        return finalize(
            list(CHANNEL_OVERRIDES[username]),
            drop_other_with_primary=False,
            sort_text=evidence,
        )

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
            return finalize(
                list(CHANNEL_OVERRIDES[username]),
                drop_other_with_primary=False,
                sort_text=evidence,
            )

    # 5) 既知ユーザは本文が薄い / unknown のときの補正
    if username in CHANNEL_OVERRIDES:
        if not found or found == ["other"] or found == ["unknown"]:
            return finalize(
                list(CHANNEL_OVERRIDES[username]),
                drop_other_with_primary=False,
                sort_text=evidence,
            )

    return finalize(found, sort_text=evidence)


def channel_evidence_text(record: dict) -> str:
    """チャネル件数推定用の本文（channel_evidence 優先）。"""
    return " ".join(
        str(record.get(key) or "")
        for key in ("channel_evidence", "tweet_text")
    ).strip()


def get_channel_counts(record: dict) -> dict[str, int]:
    """レコードからチャネル別即数内訳を推定する。"""
    return estimate_channel_counts(channel_evidence_text(record))


def get_channel_breakdown(record: dict) -> dict[str, dict]:
    """レコードから絵文字単位の内訳を返す。"""
    return parse_channel_breakdown(channel_evidence_text(record))


def format_channel_parts(record: dict) -> list[tuple[str, str]]:
    """件数が多い順の (channel_key, 表示ラベル) を返す。

    - 絵文字内訳あり: ネト（🍐5・🍎6・🔥2） / スト（🐶5・🦐1）
    - キーワードのみ: スト（9）
    - 件数なし: スト / ネト / 謎
    """
    evidence = channel_evidence_text(record)
    channels = list(infer_channels(record))
    breakdown = get_channel_breakdown(record)
    # 内訳件数があるチャネルは必ず出す（💯→その他 など infer 漏れ防止）
    for ch, data in breakdown.items():
        if ch == "unknown":
            continue
        if int(data.get("total") or 0) > 0 and ch not in channels:
            channels.append(ch)
    # 件数0のチャネルは内訳がある他チャネルがあるとき落とす（装飾ネト誤爆）
    if any(int((breakdown.get(c) or {}).get("total") or 0) > 0 for c in channels):
        channels = [
            c
            for c in channels
            if c == "unknown"
            or int((breakdown.get(c) or {}).get("total") or 0) > 0
            or c not in breakdown
        ]
    channels = order_channels_by_count(channels, evidence)
    parts: list[tuple[str, str]] = []
    for channel in channels:
        short = CHANNEL_SHORT_LABELS.get(channel, CATEGORY_LABELS.get(channel, channel))
        data = breakdown.get(channel) or {}
        emojis: dict[str, int] = data.get("emojis") or {}
        total = int(data.get("total") or 0)
        if emojis:
            # 件数多い絵文字順。同数は出現順維持
            ordered = sorted(emojis.items(), key=lambda kv: (-int(kv[1]), kv[0]))
            inner = "・".join(f"{emo}{cnt}" for emo, cnt in ordered if cnt > 0)
            label = f"{short}（{inner}）" if inner else short
        elif total > 0:
            label = f"{short}（{total}）"
        else:
            label = short
        parts.append((channel, label))
    return parts


def format_channel_text(record: dict, sep: str = "、") -> str:
    """Markdown / テキスト用のチャネル表示。"""
    return sep.join(label for _, label in format_channel_parts(record))


UNALLOCATED_CHANNEL = "unallocated"
UNALLOCATED_LABEL = "内訳なし"


def _emoji_merge_key(emo: str) -> str:
    return (emo or "").replace("\ufe0f", "")


def summarize_channel_totals(
    records: list[dict],
    count_key: str = "monthly_count",
) -> dict:
    """掲載レコードの即数をチャネル内訳で合算する。

    - 件数付き内訳がある分だけそのチャネルへ
    - チャネル名だけで件数がない人（スト / ネト / 謎）は、その人の即数をそのチャネルへ
    - 残り（北国・相席・某席など絵文字チャネルに載せない分）は内訳なし
    """
    chan_tot = {c: 0 for c in CHANNEL_ORDER}
    chan_tot[UNALLOCATED_CHANNEL] = 0
    emoji_tot: dict[str, dict[str, int]] = {c: {} for c in CHANNEL_ORDER}
    keyword_only = {c: 0 for c in CHANNEL_ORDER}

    for r in records:
        n = int(r.get(count_key) or 0)
        if n <= 0:
            continue
        bd = get_channel_breakdown(r)
        assigned = 0
        unlabeled: list[str] = []
        for ch, _label in format_channel_parts(r):
            data = bd.get(ch) or {}
            total = int(data.get("total") or 0)
            emojis = data.get("emojis") or {}
            if total > 0:
                chan_tot[ch] = chan_tot.get(ch, 0) + total
                assigned += total
                if emojis:
                    emo_sum = 0
                    for emo, cnt in emojis.items():
                        cnt_i = int(cnt)
                        if cnt_i <= 0:
                            continue
                        key = _emoji_merge_key(emo)
                        bucket = emoji_tot.setdefault(ch, {})
                        bucket[key] = bucket.get(key, 0) + cnt_i
                        emo_sum += cnt_i
                    extra = total - emo_sum
                    if extra > 0:
                        keyword_only[ch] = keyword_only.get(ch, 0) + extra
                else:
                    keyword_only[ch] = keyword_only.get(ch, 0) + total
            elif ch:
                unlabeled.append(ch)
        rest = n - assigned
        if rest > 0:
            if len(unlabeled) == 1 and assigned == 0:
                ch = unlabeled[0]
                chan_tot[ch] = chan_tot.get(ch, 0) + rest
                keyword_only[ch] = keyword_only.get(ch, 0) + rest
            else:
                chan_tot[UNALLOCATED_CHANNEL] += rest

    return {
        "people": len(records),
        "grand": sum(int(r.get(count_key) or 0) for r in records),
        "totals": chan_tot,
        "emojis": emoji_tot,
        "keyword_only": keyword_only,
    }


def _channel_summary_columns(summary: dict) -> list[tuple[str, str, int]]:
    """(channel_key, 表示名, 即数) をスト→ネト→箱→その他→謎→内訳なしの順で返す。"""
    totals: dict[str, int] = summary.get("totals") or {}
    cols: list[tuple[str, str, int]] = []
    for ch in CHANNEL_ORDER:
        n = int(totals.get(ch) or 0)
        if n <= 0:
            continue
        label = CHANNEL_SHORT_LABELS.get(ch, CATEGORY_LABELS.get(ch, ch))
        cols.append((ch, label, n))
    unalloc = int(totals.get(UNALLOCATED_CHANNEL) or 0)
    if unalloc > 0:
        cols.append((UNALLOCATED_CHANNEL, UNALLOCATED_LABEL, unalloc))
    return cols


def _channel_inner_label(summary: dict, ch: str) -> str:
    emo_map = (summary.get("emojis") or {}).get(ch) or {}
    kw = int((summary.get("keyword_only") or {}).get(ch) or 0)
    parts: list[str] = []
    ordered = sorted(emo_map.items(), key=lambda kv: (-int(kv[1]), kv[0]))
    for emo, cnt in ordered:
        if int(cnt) > 0:
            parts.append(f"{emo}{cnt}")
    if kw > 0:
        parts.append(f"件数のみ{kw}")
    return "・".join(parts)


def format_channel_summary_markdown(
    records: list[dict],
    count_key: str = "monthly_count",
) -> str:
    """チャネル合計の Markdown（チャネルを列にした横長表）。"""
    summary = summarize_channel_totals(records, count_key=count_key)
    grand = int(summary["grand"] or 0)
    people = int(summary["people"] or 0)
    cols = _channel_summary_columns(summary)
    headers = [""] + [label for _ch, label, _n in cols] + ["合計"]
    align = ["---"] + [":---:" for _ in cols] + [":---:"]
    counts = ["即数"] + [str(n) for _ch, _label, n in cols] + [str(grand)]
    ratios = ["構成比"]
    for _ch, _label, n in cols:
        ratios.append(f"{100.0 * n / grand:.1f}%" if grand else "-")
    ratios.append("100%")
    inners = ["内訳"]
    for ch, _label, _n in cols:
        inners.append(_channel_inner_label(summary, ch) or "-")
    inners.append("")
    lines = [
        "## チャネル合計（5即以上）",
        "",
        f"掲載{people}人・計{grand}即。総括の件数付き内訳を合算。"
        "チャネル名だけの人（スト / ネト / 謎）はその即数をそのチャネルへ。"
        "絵文字チャネルに載せない分（北国・相席・某席など）は内訳なし。",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(align) + " |",
        "| " + " | ".join(counts) + " |",
        "| " + " | ".join(ratios) + " |",
        "| " + " | ".join(inners) + " |",
        "",
    ]
    return "\n".join(lines)


def build_channel_summary_html(
    records: list[dict],
    count_key: str = "monthly_count",
) -> str:
    """月別タブ用のチャネル合計 HTML（チャネルを列にした横長表）。"""
    summary = summarize_channel_totals(records, count_key=count_key)
    grand = int(summary["grand"] or 0)
    people = int(summary["people"] or 0)
    note = (
        f"掲載{people}人・計{grand}即。総括の件数付き内訳を合算。"
        "チャネル名だけの人はその即数をそのチャネルへ。"
        "絵文字チャネルに載せない分は内訳なし。"
    )
    cols = _channel_summary_columns(summary)
    head = "<th></th>"
    count_cells = "<th>即数</th>"
    ratio_cells = "<th>構成比</th>"
    inner_cells = "<th>内訳</th>"
    for ch, label, n in cols:
        badge_class = "none" if ch == UNALLOCATED_CHANNEL else ch
        head += f'<th><span class="badge badge-cat-{badge_class}">{label}</span></th>'
        pct = f"{100.0 * n / grand:.1f}%" if grand else "-"
        inner = _channel_inner_label(summary, ch) or "-"
        count_cells += f'<td class="sokusuu">{n}</td>'
        ratio_cells += f"<td>{pct}</td>"
        inner_cells += f'<td class="channel-inner">{inner}</td>'
    head += "<th>合計</th>"
    count_cells += f'<td class="sokusuu">{grand}</td>'
    ratio_cells += "<td>100%</td>"
    inner_cells += "<td></td>"
    return (
        '<div class="channel-summary">'
        '<p class="channel-summary-title">チャネル合計（5即以上）</p>'
        f'<p class="channel-summary-note">{note}</p>'
        "<table><thead><tr>"
        + head
        + "</tr></thead><tbody><tr>"
        + count_cells
        + "</tr><tr>"
        + ratio_cells
        + "</tr><tr>"
        + inner_cells
        + "</tr></tbody></table></div>"
    )


def build_channel_badges_html(record: dict) -> str:
    badges = ""
    for channel, label in format_channel_parts(record):
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
        if y_year in HIDDEN_YEARLY_YEARS:
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
        if m_year in HIDDEN_MONTHLY_YEARS:
            continue

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
            + build_channel_summary_html(ranked_data, count_key="monthly_count")
            + '<table><thead><tr><th>#</th><th>アカウント</th><th>表示名</th><th>即数</th>'
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
        .channel-summary {{ margin-bottom: 24px; overflow-x: auto; }}
        .channel-summary table {{ margin-bottom: 0; min-width: 760px; }}
        .channel-summary th,
        .channel-summary td {{ text-align: center; vertical-align: top; }}
        .channel-summary .channel-inner {{
            font-size: 0.82em;
            color: #ccc;
            white-space: nowrap;
        }}
        .channel-summary-title {{
            color: #fff;
            font-weight: 600;
            margin: 0 0 8px;
        }}
        .channel-summary-note {{
            color: #888;
            font-size: 0.85em;
            margin: 0 0 12px;
            line-height: 1.5;
        }}
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
