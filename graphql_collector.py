"""
GraphQL API版 即数収集スクリプト

Seleniumの代わりにTwitter内部GraphQL APIを直接叩くことで高速化。
1件あたり約1秒（Seleniumの5-10倍速）。
"""

import csv
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# --- 定数 ---

OUTPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_CSV = "data/sokusuu_accounts.csv"
COOKIE_FILE = "data/.twitter_cookies.json"
DISCOVERED_FILE = "data/discovered_accounts.json"
SEED_FILE = "seed_accounts.txt"

BEARER_TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

GRAPHQL_USER_BY_SCREEN_NAME = "https://x.com/i/api/graphql/G3KGOASz96M-Qu0nwmGXNg/UserByScreenName"
GRAPHQL_TWEET_DETAIL = "https://x.com/i/api/graphql/nBS-WpgA6ZG0CyNHD517JQ/TweetDetail"

USER_FEATURES = json.dumps({
    "hidden_profile_subscriptions_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "highlights_tweets_tab_ui_enabled": True,
    "responsive_web_twitter_article_notes_tab_enabled": True,
    "subscriptions_feature_can_gift_premium": True,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
})

TWEET_FEATURES = json.dumps({
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
})

# 即数パターン（scraper.pyと同一）
SOKUSUU_PATTERNS = [
    re.compile(r"通算\s*即\s*(\d+)"),
    re.compile(r"即数\s*(\d+)"),
    re.compile(r"経験人数\s*(\d+)"),
    re.compile(r"体験人数\s*(\d+)"),
    re.compile(r"(\d+)\s*人斬り"),
    re.compile(r"人斬り\s*(\d+)"),
    re.compile(r"斬り数\s*(\d+)"),
    re.compile(r"(\d+)\s*斬り"),
    re.compile(r"斬り\s*(\d+)"),
    re.compile(r"total\s*(\d+)\s*即", re.IGNORECASE),
    re.compile(r"(\d+)\s*即"),
    re.compile(r"(\d+)\s*get", re.IGNORECASE),
    re.compile(r"ゲット数\s*(\d+)"),
    re.compile(r"GET\s*(\d+)", re.IGNORECASE),
    re.compile(r"GN\s*(\d+)", re.IGNORECASE),
    re.compile(r"S数\s*(\d+)"),
    re.compile(r"即\s*(\d+)"),
]

CATEGORY_PATTERNS = {
    "street": re.compile(
        r"(スト(?:ナン|リート)?|street|路上|声かけ|声掛け)", re.IGNORECASE
    ),
    "club": re.compile(
        r"(クラブ|箱|club|ラウンジ|lounge|キャバ)", re.IGNORECASE
    ),
    "online": re.compile(
        r"(ネト|アプリ|tinder|ペアーズ|pairs|タップル|tapple|bumble|マッチング|OLD|with|omiai|ネットナンパ|ネトナン|ネッナン|マチアプ)",
        re.IGNORECASE,
    ),
}


@dataclass
class SokusuuRecord:
    username: str
    display_name: str
    sokusuu: int
    source: str
    url: str
    followers_count: int = 0
    bio: str = ""
    alt_accounts: str = ""
    categories: str = ""
    profile_image_url: str = ""


def extract_sokusuu(text: str) -> Optional[int]:
    if not text:
        return None
    # 年号・日付を除去して誤検出を防ぐ
    cleaned = re.sub(r'(20[12]\d)\s*[年./]', 'YEAR_', text)
    cleaned = re.sub(r'20[12]\d/\d{1,2}/\d{1,2}', 'DATE_', cleaned)
    values = []
    for pattern in SOKUSUU_PATTERNS:
        matches = pattern.findall(cleaned)
        values.extend(int(m) for m in matches)
    if not values:
        return None
    return max(values)


def detect_categories(bio: str, username: str) -> list[str]:
    text = f"{bio} {username}"
    cats = []
    for cat_name, pattern in CATEGORY_PATTERNS.items():
        if pattern.search(text):
            cats.append(cat_name)
    return cats


class TwitterGraphQL:
    def __init__(self, cookie_file: str = COOKIE_FILE):
        cookies = json.load(open(cookie_file, "r"))
        cookie_dict = {c["name"]: c["value"] for c in cookies}
        ct0 = cookie_dict["ct0"]
        cookie_str = "; ".join(f'{c["name"]}={c["value"]}' for c in cookies)

        self.session = requests.Session()
        self.session.headers.update({
            "authorization": BEARER_TOKEN,
            "x-csrf-token": ct0,
            "cookie": cookie_str,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
        })
        self.rate_limit_remaining = None
        self.rate_limit_reset = None

    def _handle_rate_limit(self, resp: requests.Response):
        """レート制限ヘッダーを確認し、必要なら待機"""
        remaining = resp.headers.get("x-rate-limit-remaining")
        reset = resp.headers.get("x-rate-limit-reset")
        if remaining is not None:
            self.rate_limit_remaining = int(remaining)
        if reset is not None:
            self.rate_limit_reset = int(reset)

        if resp.status_code == 429:
            if self.rate_limit_reset:
                wait = max(self.rate_limit_reset - int(time.time()), 5)
            else:
                wait = 60
            print(f"  [RATE LIMIT] {wait}秒待機...")
            time.sleep(wait)
            return True
        return False

    def get_user(self, screen_name: str) -> Optional[dict]:
        """ユーザープロフィールを取得"""
        variables = json.dumps({
            "screen_name": screen_name,
            "withSafetyModeUserFields": True,
        })
        for attempt in range(3):
            try:
                resp = self.session.get(
                    GRAPHQL_USER_BY_SCREEN_NAME,
                    params={"variables": variables, "features": USER_FEATURES},
                    timeout=15,
                )
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    return None

                data = resp.json()
                user = data.get("data", {}).get("user", {}).get("result", {})
                if not user or user.get("__typename") == "UserUnavailable":
                    return None

                legacy = user.get("legacy", {})
                img = legacy.get("profile_image_url_https", "")
                img = img.replace("_normal.", "_400x400.")

                return {
                    "screen_name": legacy.get("screen_name", screen_name),
                    "name": legacy.get("name", screen_name),
                    "description": legacy.get("description", ""),
                    "followers_count": legacy.get("followers_count", 0),
                    "profile_image_url": img,
                    "pinned_tweet_ids": legacy.get("pinned_tweet_ids_str", []),
                }
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    time.sleep(2)
                    continue
                print(f"  [ERROR] @{screen_name}: リクエスト失敗 - {e}")
                return None
        return None

    def get_tweet_text(self, tweet_id: str) -> Optional[str]:
        """ツイートのテキストを取得"""
        variables = json.dumps({
            "focalTweetId": tweet_id,
            "with_rux_injections": False,
            "rankingMode": "Relevance",
            "includePromotedContent": True,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
        })
        for attempt in range(2):
            try:
                resp = self.session.get(
                    GRAPHQL_TWEET_DETAIL,
                    params={"variables": variables, "features": TWEET_FEATURES},
                    timeout=15,
                )
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    return None

                data = resp.json()
                instructions = (data.get("data", {})
                                .get("threaded_conversation_with_injections_v2", {})
                                .get("instructions", []))
                for inst in instructions:
                    for entry in inst.get("entries", []):
                        result = (entry.get("content", {})
                                  .get("itemContent", {})
                                  .get("tweet_results", {})
                                  .get("result", {}))
                        if result:
                            legacy = result.get("legacy", {})
                            ft = legacy.get("full_text", "")
                            if ft:
                                return ft
                return None
            except requests.exceptions.RequestException:
                if attempt < 1:
                    time.sleep(2)
                    continue
                return None
        return None


def collect_one(api: TwitterGraphQL, username: str) -> Optional[SokusuuRecord]:
    """1ユーザーの即数を収集"""
    user = api.get_user(username)
    if not user:
        return None

    bio = user["description"]
    display_name = user["name"]
    followers_count = user["followers_count"]
    profile_image_url = user["profile_image_url"]

    # bioから即数を抽出
    profile_sokusuu = extract_sokusuu(bio)
    profile_url = f"https://twitter.com/{username}"

    # bioになければ固定ツイートをチェック
    pinned_sokusuu = None
    pinned_url = None
    if user["pinned_tweet_ids"]:
        tweet_id = user["pinned_tweet_ids"][0]
        tweet_text = api.get_tweet_text(tweet_id)
        if tweet_text:
            pinned_sokusuu = extract_sokusuu(tweet_text)
            pinned_url = f"https://twitter.com/{username}/status/{tweet_id}"

    # 即数決定
    if profile_sokusuu is not None and pinned_sokusuu is not None:
        if pinned_sokusuu > profile_sokusuu:
            sokusuu, source, url = pinned_sokusuu, "pinned_tweet", pinned_url
        else:
            sokusuu, source, url = profile_sokusuu, "profile", profile_url
    elif profile_sokusuu is not None:
        sokusuu, source, url = profile_sokusuu, "profile", profile_url
    elif pinned_sokusuu is not None:
        sokusuu, source, url = pinned_sokusuu, "pinned_tweet", pinned_url
    else:
        return None

    if sokusuu < 10:
        return None

    cats = detect_categories(bio, username)
    cats_str = ", ".join(cats) if cats else ""
    print(f"  [OK] @{username}: 即{sokusuu} (source: {source}, followers: {followers_count}, cats: {cats_str or 'none'})")
    return SokusuuRecord(
        username=username,
        display_name=display_name,
        sokusuu=sokusuu,
        source=source,
        url=url,
        followers_count=followers_count,
        bio=bio,
        categories=cats_str,
        profile_image_url=profile_image_url,
    )


def merge_alt_accounts(records: list[SokusuuRecord]) -> list[SokusuuRecord]:
    """scraper.pyと同じサブ垢統合ロジック"""
    mention_map: dict[str, set[str]] = {}
    username_set = {r.username.lower() for r in records}

    for r in records:
        mentions = re.findall(r"@([A-Za-z0-9_]{1,15})", r.bio)
        for m in mentions:
            if m.lower() in username_set and m.lower() != r.username.lower():
                mention_map.setdefault(r.username.lower(), set()).add(m.lower())

    merged = {}
    skip = set()
    records_by_name = {r.username.lower(): r for r in records}

    for r in records:
        if r.username.lower() in skip:
            continue
        alts = mention_map.get(r.username.lower(), set())
        main = r
        alt_names = []
        for alt_name in alts:
            if alt_name in skip:
                continue
            alt_r = records_by_name.get(alt_name)
            if alt_r:
                if alt_r.sokusuu > main.sokusuu:
                    alt_names.append(main.username)
                    main = alt_r
                else:
                    alt_names.append(alt_r.username)
                skip.add(alt_name)

        if alt_names:
            existing_alts = main.alt_accounts.split(", ") if main.alt_accounts else []
            all_alts = sorted(set(existing_alts + alt_names) - {""})
            main = SokusuuRecord(**{**asdict(main), "alt_accounts": ", ".join(all_alts)})

        merged[main.username.lower()] = main

    return list(merged.values())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GraphQL版 即数収集")
    parser.add_argument("--cookie", default=COOKIE_FILE, help="Cookieファイルパス")
    args = parser.parse_args()

    print("=" * 50)
    print("即数収集（GraphQL API版）")
    print("=" * 50)

    # 対象アカウント読み込み
    if os.path.exists(DISCOVERED_FILE):
        with open(DISCOVERED_FILE, "r", encoding="utf-8") as f:
            all_accounts = json.load(f)
        # シードも含める
        if os.path.exists(SEED_FILE):
            with open(SEED_FILE, "r", encoding="utf-8") as f:
                seeds = [l.strip().lstrip("@") for l in f if l.strip() and not l.startswith("#")]
            all_accounts = sorted(set(all_accounts) | set(seeds))
        print(f"[INFO] 対象: {len(all_accounts)} アカウント")
    else:
        print("[ERROR] discovered_accounts.json がありません")
        return

    # 既に収集済みのアカウントをスキップ
    collected_usernames = set()
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
        collected_usernames = {r["username"] for r in existing}
        print(f"[INFO] 収集済み: {len(collected_usernames)} アカウント（スキップ）")

    remaining = [a for a in all_accounts if a not in collected_usernames]
    print(f"[INFO] 未収集: {len(remaining)} アカウント\n")

    if not remaining:
        print("[INFO] 全アカウント収集済み")
        return

    # GraphQL API初期化
    api = TwitterGraphQL(cookie_file=args.cookie)

    # 収集
    records: list[SokusuuRecord] = []
    for i, username in enumerate(remaining):
        record = collect_one(api, username)
        if record:
            records.append(record)

        if (i + 1) % 50 == 0:
            print(f"[PROGRESS] {i + 1}/{len(remaining)} 処理済み ({len(records)} 件ヒット)")
            # 中間保存
            _save_merged(records, existing if collected_usernames else [])

        # レート制限に近づいたら少し待つ
        if api.rate_limit_remaining is not None and api.rate_limit_remaining < 10:
            print(f"  [WARN] レート制限残り{api.rate_limit_remaining}、少し待機...")
            time.sleep(5)

    print(f"\n[INFO] 収集完了: {len(records)} 件ヒット（{len(remaining)} 件処理）")

    if not records and not collected_usernames:
        print("[WARN] 即数が見つかったアカウントがありません。")
        return

    # 最終保存
    _save_merged(records, existing if collected_usernames else [])


def _save_merged(new_records: list[SokusuuRecord], existing: list[dict]):
    """新規データと既存データをマージして保存"""
    existing_map = {r["username"]: r for r in existing}

    for r in new_records:
        rec = asdict(r)
        if r.username in existing_map:
            old = existing_map[r.username]
            if r.sokusuu >= old.get("sokusuu", 0):
                if old.get("profile_image_url") and not rec.get("profile_image_url"):
                    rec["profile_image_url"] = old["profile_image_url"]
                if old.get("categories") and not rec.get("categories"):
                    rec["categories"] = old["categories"]
                if old.get("alt_accounts") and not rec.get("alt_accounts"):
                    rec["alt_accounts"] = old["alt_accounts"]
                existing_map[r.username] = rec
        else:
            existing_map[r.username] = rec

    records_final = sorted(existing_map.values(), key=lambda r: r["sokusuu"], reverse=True)

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records_final, f, ensure_ascii=False, indent=2)

    fieldnames = ["username", "display_name", "sokusuu", "source", "url",
                  "followers_count", "alt_accounts", "categories", "bio", "profile_image_url"]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records_final:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"[SAVE] {OUTPUT_JSON} ({len(records_final)} 件, マージ済み)")


if __name__ == "__main__":
    main()
