"""
月間/年間即数ランキング収集スクリプト

月別:
  python monthly_collect.py --mode monthly --year 2026 --month 2

年別:
  python monthly_collect.py --mode yearly --year 2025
"""

import argparse
import asyncio
import json
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta

import requests

COOKIE_FILES = [
    "data/.twitter_cookies.json",
    "data/.twitter_cookies_worker1.json",
    "data/.twitter_cookies_worker2.json",
    "data/.twitter_cookies_worker3.json",
    "data/.twitter_cookies_worker4.json",
    "data/.twitter_cookies_worker5.json",
]
PRIMARY_COOKIE_FILE = COOKIE_FILES[0]

BEARER_TOKEN = (
    "Bearer "
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)
USER_FEATURES = json.dumps(
    {
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
    }
)
TWEET_FEATURES = json.dumps(
    {
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
    }
)

OUTPUT_JSON = "data/sokusuu_accounts.json"
MONTHLY_RANKING_JSON = "data/monthly_ranking.json"
YEARLY_RANKING_JSON = "data/yearly_ranking.json"
USER_ID_CACHE_JSON = "data/.user_id_cache.json"
RATE_LIMIT_GRACE_SECONDS = 2
MAX_API_AUTO_WAIT_SECONDS = 20
PLAYWRIGHT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
PLAYWRIGHT_STEALTH_SCRIPT = """
(() => {
  const defineGetter = (target, key, getter) => {
    try {
      Object.defineProperty(target, key, { get: getter, configurable: true });
    } catch (_) {}
  };
  defineGetter(Navigator.prototype, "webdriver", () => undefined);
  defineGetter(Navigator.prototype, "languages", () => ["ja-JP", "ja", "en-US", "en"]);
  defineGetter(Navigator.prototype, "plugins", () => [1, 2, 3, 4, 5]);
  defineGetter(Navigator.prototype, "platform", () => "Win32");
  window.chrome = window.chrome || {};
  window.chrome.runtime = window.chrome.runtime || {};
})();
"""

_USER_ID_CACHE = None


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_usernames_file(path):
    usernames = []
    seen = set()
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue
            username = line.lstrip("@").strip()
            if not username:
                continue
            key = username.lower()
            if key in seen:
                continue
            seen.add(key)
            usernames.append(username)
    return usernames


def get_evidence_url(row):
    if not row:
        return ""
    for field in ("evidence_url", "tweet_url"):
        url = row.get(field, "")
        if url and "/status/" in url:
            return url
    return ""


def is_profile_match_source(match_source):
    return bool(match_source) and match_source.startswith("profile_")


def infer_profile_source_field(match_source):
    return match_source[len("profile_") :] if is_profile_match_source(match_source) else ""


def classify_period_source(row):
    if not row:
        return "tweet_evidence"
    source_type = row.get("source_type", "")
    if source_type in {"tweet_evidence", "profile_derived"}:
        return source_type
    if is_profile_match_source(row.get("match_source", "")) or row.get("needs_review"):
        return "profile_derived"

    evidence_url = get_evidence_url(row)
    username = row.get("username", "")
    profile_url = f"https://x.com/{username}" if username else ""
    for field in ("source_url", "tweet_url"):
        url = row.get(field, "")
        if url and not evidence_url and profile_url and url == profile_url:
            return "profile_derived"
    return "tweet_evidence"


def normalize_period_row(row):
    normalized = dict(row or {})
    username = normalized.get("username", "")
    source_type = classify_period_source(normalized)
    normalized["source_type"] = source_type

    evidence_url = get_evidence_url(normalized)
    source_url = normalized.get("source_url", "")
    tweet_url = normalized.get("tweet_url", "")

    if source_type == "profile_derived":
        normalized["needs_review"] = True
        if not normalized.get("profile_source_field"):
            inferred_field = infer_profile_source_field(normalized.get("match_source", ""))
            if inferred_field:
                normalized["profile_source_field"] = inferred_field
        if not source_url:
            if tweet_url and "/status/" not in tweet_url:
                normalized["source_url"] = tweet_url
            elif username:
                normalized["source_url"] = f"https://x.com/{username}"
        normalized["tweet_url"] = ""
        normalized.pop("evidence_url", None)
    else:
        normalized["needs_review"] = bool(normalized.get("needs_review", False))
        if evidence_url:
            normalized["tweet_url"] = evidence_url
            normalized["evidence_url"] = evidence_url
            if not source_url:
                normalized["source_url"] = evidence_url
        else:
            normalized["tweet_url"] = ""
            normalized.pop("evidence_url", None)
        if not normalized["needs_review"]:
            normalized.pop("profile_source_field", None)

    return normalized


def get_match_source_priority(match_source):
    if not match_source:
        return 2
    if match_source in {"timeline", "timeline_browser"}:
        return 3
    if match_source in {"search", "global_search"}:
        return 2
    if match_source.startswith("profile_"):
        return 0
    return 1


def build_result_score(row, count_key):
    return (
        row.get(count_key, 0),
        1 if get_evidence_url(row) else 0,
        0
        if classify_period_source(row) == "profile_derived"
        else get_match_source_priority(row.get("match_source", "")),
    )


def build_period_result(account, hit, value_key, match_source):
    source_url = hit.get("url", "")
    evidence_url = source_url if source_url and "/status/" in source_url else ""
    is_profile_source = is_profile_match_source(match_source)
    if is_profile_source:
        evidence_url = ""

    account_row = dict(account or {})
    account_row.setdefault("username", hit.get("username", ""))

    return normalize_period_row(
        {
        "username": account_row.get("username", ""),
        "display_name": account_row.get("display_name", ""),
        value_key: hit["count"],
        "tweet_url": evidence_url,
        "source_url": source_url,
        "evidence_url": evidence_url,
        "tweet_text": hit.get("text", ""),
        "tweet_created_at": hit.get("created_at", ""),
        "followers_count": account_row.get("followers_count", 0),
        "categories": account_row.get("categories", ""),
        "profile_image_url": account_row.get("profile_image_url", ""),
        "match_source": match_source,
        "profile_source_field": hit.get("source_field", "") if is_profile_source else "",
        "needs_review": is_profile_source,
        }
    )


def should_replace_result(existing, candidate, count_key):
    if not existing:
        return True
    return build_result_score(candidate, count_key) > build_result_score(
        existing, count_key
    )


def restore_prefetched_hits(results, value_key):
    hits = {}
    for row in results:
        if row.get("match_source") != "global_search":
            continue
        count = row.get(value_key)
        username = row.get("username")
        if not count or not username:
            continue
        hits[username] = {
            "username": username,
            "count": count,
            "url": row.get("source_url")
            or get_evidence_url(row)
            or f"https://x.com/{username}",
            "text": row.get("tweet_text", ""),
            "created_at": row.get("tweet_created_at", ""),
        }
    return hits


def save_collect_state(path, processed_usernames, results, prefetch_state=None):
    state = {
        "processed_usernames": sorted(processed_usernames),
        "results": results,
    }
    if prefetch_state is not None:
        state["prefetch_state"] = prefetch_state
    save_json(path, state)


def load_user_id_cache():
    global _USER_ID_CACHE
    if _USER_ID_CACHE is None:
        _USER_ID_CACHE = load_json(USER_ID_CACHE_JSON, {})
    return _USER_ID_CACHE


def save_user_id_cache(cache):
    global _USER_ID_CACHE
    _USER_ID_CACHE = cache
    save_json(USER_ID_CACHE_JSON, cache)


def create_sessions():
    sessions = []
    for cookie_file in COOKIE_FILES:
        if not os.path.exists(cookie_file):
            continue
        try:
            cookies = load_json(cookie_file, [])
            cookie_dict = {c["name"]: c["value"] for c in cookies}
            ct0 = cookie_dict["ct0"]
            cookie_str = "; ".join(f'{c["name"]}={c["value"]}' for c in cookies)

            session = requests.Session()
            session.headers.update(
                {
                    "authorization": BEARER_TOKEN,
                    "x-csrf-token": ct0,
                    "cookie": cookie_str,
                    "user-agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36"
                    ),
                    "x-twitter-active-user": "yes",
                    "x-twitter-auth-type": "OAuth2Session",
                }
            )
            sessions.append(
                {
                    "name": os.path.basename(cookie_file),
                    "session": session,
                    "available_at": 0.0,
                }
            )
        except Exception:
            continue
    return sessions


def api_get(sessions, session_idx, url, params, max_auto_wait_seconds=None):
    """レート制限を回避しながら API を叩く。"""
    if not sessions:
        return None
    if max_auto_wait_seconds is None:
        max_auto_wait_seconds = MAX_API_AUTO_WAIT_SECONDS

    for _ in range(max(len(sessions) * 4, 1)):
        now = time.time()
        selected_idx = None
        earliest_available = None

        for offset in range(len(sessions)):
            idx = (session_idx[0] + offset) % len(sessions)
            available_at = sessions[idx].get("available_at", 0.0)
            if earliest_available is None or available_at < earliest_available:
                earliest_available = available_at
            if available_at <= now:
                selected_idx = idx
                break

        if selected_idx is None:
            wait = max(int((earliest_available or now) - now) + 1, 1)
            if wait <= max_auto_wait_seconds:
                time.sleep(wait)
                continue
            print(
                f"  [ALL RATE LIMITED] {wait}秒待機想定 -> "
                "APIを諦めてSearchへフォールバック"
            )
            return None

        session_idx[0] = selected_idx
        session_state = sessions[selected_idx]
        session = session_state["session"]
        try:
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                reset = resp.headers.get("x-rate-limit-reset")
                wait_until = (
                    int(reset) + RATE_LIMIT_GRACE_SECONDS if reset else now + 60
                )
                session_state["available_at"] = max(
                    session_state.get("available_at", 0.0), wait_until
                )
                session_idx[0] = (selected_idx + 1) % len(sessions)
                continue

            remaining = resp.headers.get("x-rate-limit-remaining")
            reset = resp.headers.get("x-rate-limit-reset")
            if remaining is not None and reset is not None and remaining == "0":
                session_state["available_at"] = max(
                    session_state.get("available_at", 0.0),
                    int(reset) + RATE_LIMIT_GRACE_SECONDS,
                )
            else:
                session_state["available_at"] = 0.0
            return resp
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            session_state["available_at"] = time.time() + 3
            session_idx[0] = (selected_idx + 1) % len(sessions)
    return None


def get_user_id(sessions, session_idx, screen_name):
    cache = load_user_id_cache()
    cache_key = screen_name.lower()
    cached = cache.get(cache_key)
    if cached:
        return cached

    resp = api_get(
        sessions,
        session_idx,
        "https://x.com/i/api/graphql/G3KGOASz96M-Qu0nwmGXNg/UserByScreenName",
        {
            "variables": json.dumps(
                {"screen_name": screen_name, "withSafetyModeUserFields": True}
            ),
            "features": USER_FEATURES,
        },
    )
    if not resp or resp.status_code != 200:
        return None
    user = resp.json().get("data", {}).get("user", {}).get("result", {})
    rest_id = user.get("rest_id")
    if rest_id:
        cache[cache_key] = rest_id
        save_user_id_cache(cache)
    return rest_id


def unwrap_tweet_result(result):
    if not isinstance(result, dict):
        return {}
    if result.get("__typename") == "TweetWithVisibilityResults":
        result = result.get("tweet", {})
    inner = result.get("result")
    if isinstance(inner, dict):
        result = inner
        if result.get("__typename") == "TweetWithVisibilityResults":
            result = result.get("tweet", {})
    return result if isinstance(result, dict) else {}


def extract_full_text(result):
    legacy = result.get("legacy", {})
    text = legacy.get("full_text", "")
    if text:
        return text

    note_result = (
        result.get("note_tweet", {})
        .get("note_tweet_results", {})
        .get("result", {})
    )
    if isinstance(note_result, dict):
        text = note_result.get("text", "")
        if text:
            return text
        inner = note_result.get("result", {})
        if isinstance(inner, dict):
            text = inner.get("text", "")
            if text:
                return text
    return ""


def extract_screen_name(result):
    user = result.get("core", {}).get("user_results", {}).get("result", {})
    if user.get("__typename") == "UserUnavailable":
        return ""
    inner = user.get("result")
    if isinstance(inner, dict):
        user = inner
    if not isinstance(user, dict):
        return ""
    core = user.get("core", {})
    if isinstance(core, dict) and core.get("screen_name"):
        return core["screen_name"]
    legacy = user.get("legacy", {})
    return legacy.get("screen_name", "")


def extract_display_name(result):
    user = result.get("core", {}).get("user_results", {}).get("result", {})
    if user.get("__typename") == "UserUnavailable":
        return ""
    inner = user.get("result")
    if isinstance(inner, dict):
        user = inner
    if not isinstance(user, dict):
        return ""
    core = user.get("core", {})
    if isinstance(core, dict) and core.get("name"):
        return core["name"]
    legacy = user.get("legacy", {})
    return legacy.get("name", "")


def parse_tweet_items(entries):
    tweets = []
    bottom_cursor = None

    def append_tweet(item_content):
        result = unwrap_tweet_result(
            item_content.get("tweet_results", {}).get("result", {})
        )
        if not result:
            return

        legacy = result.get("legacy", {})
        tweet_id = legacy.get("id_str") or result.get("rest_id", "")
        text = extract_full_text(result)
        created_at = legacy.get("created_at", "")
        username = extract_screen_name(result)
        display_name = extract_display_name(result)
        if text and tweet_id:
            tweets.append(
                {
                    "id": tweet_id,
                    "text": text,
                    "created_at": created_at,
                    "username": username,
                    "display_name": display_name,
                }
            )

    for entry in entries:
        content = entry.get("content", {})

        if content.get("cursorType") == "Bottom":
            bottom_cursor = content.get("value")

        item_content = content.get("itemContent", {})
        if item_content:
            append_tweet(item_content)

        for item in content.get("items", []):
            nested = item.get("item", {}).get("itemContent", {})
            if nested:
                append_tweet(nested)

    return tweets, bottom_cursor


def get_user_tweets(sessions, session_idx, user_id, count=40, max_pages=4):
    """ユーザーのツイートを複数ページ取得する。"""
    all_tweets = []
    seen_ids = set()
    cursor = None

    for _ in range(max_pages):
        variables = {
            "userId": user_id,
            "count": count,
            "includePromotedContent": False,
            "withQuickPromoteEligibilityTweetFields": False,
            "withVoice": False,
            "withV2Timeline": True,
        }
        if cursor:
            variables["cursor"] = cursor

        resp = api_get(
            sessions,
            session_idx,
            "https://x.com/i/api/graphql/E3opETHurmVJflFsUBVuUQ/UserTweets",
            {"variables": json.dumps(variables), "features": TWEET_FEATURES},
        )
        if not resp or resp.status_code != 200:
            break

        data = resp.json()
        instructions = (
            data.get("data", {})
            .get("user", {})
            .get("result", {})
            .get("timeline_v2", {})
            .get("timeline", {})
            .get("instructions", [])
        )

        page_tweets = []
        next_cursor = None
        for inst in instructions:
            tweets, bottom_cursor = parse_tweet_items(inst.get("entries", []))
            page_tweets.extend(tweets)
            if bottom_cursor:
                next_cursor = bottom_cursor

        new_count = 0
        for tweet in page_tweets:
            if tweet["id"] in seen_ids:
                continue
            seen_ids.add(tweet["id"])
            all_tweets.append(tweet)
            new_count += 1

        if not next_cursor or new_count == 0:
            break
        cursor = next_cursor

    return all_tweets


def extract_user_tweets_from_body(body):
    instructions = (
        body.get("data", {})
        .get("user", {})
        .get("result", {})
        .get("timeline_v2", {})
        .get("timeline", {})
        .get("instructions", [])
    )

    captured = []
    seen_ids = set()
    for inst in instructions:
        tweets, _ = parse_tweet_items(inst.get("entries", []))
        for tweet in tweets:
            if tweet["id"] in seen_ids:
                continue
            seen_ids.add(tweet["id"])
            captured.append(tweet)
    return captured


def clean_tweet_text(text):
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[\U00010000-\U0010ffff]", " ", text)
    text = text.replace("\n", " ")
    return re.sub(r"\s+", " ", text).strip()


def extract_monthly_count(text, year, month, strict=False):
    """ツイート本文から対象月の月間即数を抽出する。"""
    cleaned = clean_tweet_text(text)
    if cleaned.startswith("RT @"):
        return None

    month_names = {
        1: ["1月", "一月", "jan"],
        2: ["2月", "二月", "feb"],
        3: ["3月", "三月", "mar"],
        4: ["4月", "四月", "apr"],
        5: ["5月", "五月", "may"],
        6: ["6月", "六月", "jun"],
        7: ["7月", "七月", "jul"],
        8: ["8月", "八月", "aug"],
        9: ["9月", "九月", "sep"],
        10: ["10月", "十月", "oct"],
        11: ["11月", "十一月", "nov"],
        12: ["12月", "十二月", "dec"],
    }
    month_tokens = "|".join(re.escape(name) for name in month_names.get(month, []))
    has_explicit_month = bool(re.search(rf"(?:{month_tokens})", cleaned, re.IGNORECASE))
    has_report_keyword = bool(
        re.search(
            r"(?:実績|結果|戦績|総括|統括|振り返り|振返り|着地|出撃|報告|まとめ|締め)",
            cleaned,
        )
    )
    has_generic_month = bool(re.search(r"(?:月間|今月)", cleaned))
    has_promo_keyword = bool(
        re.search(r"(?:tips?|TIPS|運用術|攻略|ノウハウ|講習|コンサル|教材|方法)", cleaned)
    )
    has_goal_keyword = bool(
        re.search(r"(?:目標|あと\d+\s*即|したい|したみい|予定|目指す|いくぞ|死守)", cleaned)
    )
    has_third_person_cue = bool(
        re.search(
            r"(?:っぽい|らしい|大先輩|他人|友達|助けていただ|助けてもら|"
            r"成果を出した|生み出した|この方)",
            cleaned,
        )
    )
    has_failed_goal_cue = bool(
        re.search(r"(?:達成できなかった|達成できず|いけそう|参考記録)", cleaned)
    )
    next_month = 1 if month == 12 else month + 1
    has_cross_month_range = bool(
        re.search(rf"{month}\s*/\s*\d+\s*[〜~\-ー]\s*{next_month}\s*/\s*\d+", cleaned)
    )
    if strict and not has_explicit_month and re.search(rf"{next_month}月", cleaned):
        return None

    if strict and not has_explicit_month and not has_report_keyword:
        return None
    if strict and not has_explicit_month and (has_promo_keyword or has_third_person_cue):
        return None
    if strict and has_third_person_cue and has_promo_keyword:
        return None
    if strict and has_goal_keyword and not has_report_keyword:
        return None
    if strict and has_failed_goal_cue:
        return None
    if strict and has_cross_month_range:
        return None
    if strict and re.search(r"(?:ランキング|rank)", cleaned, re.IGNORECASE) and re.search(
        r"(?:さん|この方|1位|2位|3位)", cleaned
    ):
        return None

    if re.search(r"累計|通算|total|トータル", cleaned, re.IGNORECASE):
        return None
    if re.search(r"(?:月間データ|回転数|BIG|REG|HANABI|スロット|パチスロ)", cleaned, re.IGNORECASE):
        return None
    if re.search(
        r"(?:即現金|買取|お支払い|予約後|キャンセル不可|お問い合わせ|DMください)",
        cleaned,
        re.IGNORECASE,
    ):
        return None
    if strict and not has_report_keyword and re.search(
        r"(?:#PR|来店|調査隊|ちゅんげー|パチンコ|PACHINKO)", cleaned, re.IGNORECASE
    ):
        return None

    if not (has_report_keyword or has_generic_month):
        exclude_patterns = [
            r"\d+即したら",
            r"即すら(?:できねー|できない)",
            r"0-\d+即",
        ]
        if any(re.search(pattern, cleaned) for pattern in exclude_patterns):
            return None

    count_unit = r"(?:即|get|g\b|そ\b)"
    has_multi_month_series = len(re.findall(r"\d{1,2}\s*月(?!間)", cleaned)) >= 2
    component_sum = None

    if strict and has_report_keyword and not has_multi_month_series:
        component_values = []
        component_spans = []
        component_patterns = [
            r"(?:スト|ネト|アポ|弾丸|準即|準|ブメ|パス|リア|箱|クラブ|wiz|with|m|gt)\s*[/:：]?\s*(\d+)\s*"
            + count_unit,
            r"(?:弾丸|準即|準|ブメ|パス)\s*"
            + count_unit
            + r"\s*(\d+)",
            r"(\d+)\s*(?:弾丸|準即|準|ブメ|パス)\s*" + count_unit,
        ]
        for pattern in component_patterns:
            for match in re.finditer(pattern, cleaned, re.IGNORECASE):
                match_text = cleaned[match.start() : min(len(cleaned), match.end() + 2)]
                if re.search(r"\d+\s*(?:即|そ|get|g\b)目(?!標)", match_text):
                    continue
                span = match.span()
                if any(not (span[1] <= start or span[0] >= end) for start, end in component_spans):
                    continue
                context = cleaned[max(0, match.start() - 12) : min(len(cleaned), match.end() + 12)]
                if re.search(r"(?:目標|残\d+\s*(?:即|そ|get|g\b)|チャレ)", context):
                    continue
                value = int(match.group(1))
                if 0 < value <= 100:
                    component_values.append(value)
                    component_spans.append(span)
        if len(component_values) >= 2:
            summed = sum(component_values)
            if 0 < summed <= 100:
                component_sum = summed

    strong_patterns = []
    if has_report_keyword:
        explicit_total_patterns = [
            r"(?:結果|実績)\s*[】\]）」)]?\s*[=:：／/|・\-ー]?\s*(?:計|合計)?\s*(\d+)\s*"
            + count_unit,
            r"(?:計|合計)\s*(\d+)\s*" + count_unit,
            r"(?:計|合計)\s*(\d+)\s*(?:弾丸|準|準即|ブメ|パス)\s*" + count_unit,
        ]
        for pattern in explicit_total_patterns:
            match = re.search(pattern, cleaned, re.IGNORECASE)
            if not match:
                continue
            value = int(match.group(1))
            if 0 < value <= 500:
                return value

        strong_patterns.extend(
            [
                rf"(?:{month_tokens})\s*[)）:：/／・\-ー]?\s*(?:計|合計)?\s*(\d+)\s*"
                + count_unit,
                r"(?:総括|統括|まとめ|振り返り|振返り|報告|締め)\s*[】\]）」)]?\s*[=:：／/|・\-ー]?\s*(?:計|合計)?\s*(\d+)\s*"
                + count_unit,
                rf"(?:{month_tokens}).{{0,40}}?(?:総括|統括|まとめ|振り返り|振返り|報告|締め).{{0,40}}?(\d+)\s*"
                + count_unit,
            ]
        )

    for pattern in strong_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if not match:
            continue
        match_text = cleaned[match.start() : min(len(cleaned), match.end() + 2)]
        if strict and re.search(r"\d+\s*(?:即|そ|get|g\b)目(?!標)", match_text):
            continue
        value = int(match.group(1))
        if 0 < value <= 500:
            if component_sum is not None:
                return max(value, component_sum)
            return value

    if component_sum is not None:
        return component_sum

    if strict and has_report_keyword and not has_multi_month_series and component_sum is None:
        component_values = []
        component_spans = []
        component_patterns = [
            r"(?:スト|ネト|アポ|弾丸|準即|準|ブメ|パス|リア|箱|クラブ|wiz|with|m|gt)\s*[/:：]?\s*(\d+)\s*"
            + count_unit,
            r"(?:弾丸|準即|準|ブメ|パス)\s*"
            + count_unit
            + r"\s*(\d+)",
            r"(\d+)\s*(?:弾丸|準即|準|ブメ|パス)\s*" + count_unit,
        ]
        for pattern in component_patterns:
            for match in re.finditer(pattern, cleaned, re.IGNORECASE):
                match_text = cleaned[match.start() : min(len(cleaned), match.end() + 2)]
                if re.search(r"\d+\s*(?:即|そ|get|g\b)目(?!標)", match_text):
                    continue
                span = match.span()
                if any(not (span[1] <= start or span[0] >= end) for start, end in component_spans):
                    continue
                context = cleaned[max(0, match.start() - 12) : min(len(cleaned), match.end() + 12)]
                if re.search(r"(?:目標|残\d+\s*(?:即|そ|get|g\b)|チャレ)", context):
                    continue
                value = int(match.group(1))
                if 0 < value <= 100:
                    component_values.append(value)
                    component_spans.append(span)
        if len(component_values) >= 2:
            summed = sum(component_values)
            if 0 < summed <= 100:
                return summed

    patterns = [
        rf"(?:{month_tokens})\s*(?:結果|実績|戦績|総括|統括|報告|着地|振り返り|振返り|まとめ|締め)\s*[】\]）」)]?\s*[=:：／/|は]?\s*(?:計|合計)?\s*(\d+)\s*{count_unit}",
        rf"[【\[]\s*(?:{month_tokens})\s*(?:結果|実績|戦績|総括|統括|報告|着地|振り返り|振返り|まとめ|締め)?\s*[】\]]\s*(?:計|合計)?\s*(\d+)\s*{count_unit}",
        rf"(?:{month_tokens})\s*[はの=:：／/|]?\s*(?:計|合計)?\s*(\d+)\s*{count_unit}",
        rf"(?:{month_tokens}).{{0,24}}?(?:計|合計|結果|実績|戦績|総括|統括|報告|着地|振り返り|振返り|まとめ|締め)?\s*(\d+)\s*{count_unit}",
    ]

    if has_report_keyword:
        patterns.append(
            r"(?:今月|月間)\s*(?:は|の結果|の実績|の総括|の統括|の報告|の振り返り|の振返り|のまとめ|の着地)?\s*[=:：／/|]?\s*(?:計|合計)?\s*(\d+)\s*"
            + count_unit
        )
    if has_explicit_month or has_report_keyword:
        patterns.append(r"(\d+)\s*即\s*(?:でした|です|達成|着地)")

    for pattern in patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if not match:
            continue
        match_text = cleaned[match.start() : min(len(cleaned), match.end() + 2)]
        if strict and re.search(r"\d+\s*(?:即|そ|get|g\b)目(?!標)", match_text):
            continue
        context = cleaned[max(0, match.start() - 18) : min(len(cleaned), match.end() + 18)]
        if strict and re.search(
            r"(?:目標|予定|目指|したい|いくぞ|までには?|死守|達成できなかった|"
            r"達成できず|いけそう|参考記録|チャレ|残\d+\s*(?:即|そ|get|g\b))",
            context,
        ) and not re.search(r"(?:結果|実績)", context):
            continue
        if strict and re.search(r"(?:講習|コンサル|教材|固定ツイート|興味ある方)", context):
            continue
        value = int(match.group(1))
        if 0 < value <= 500:
            return value
    return None


def extract_yearly_count(text, year, strict=False):
    """ツイート本文から対象年の年間即数を抽出する。"""
    cleaned = clean_tweet_text(text)
    if cleaned.startswith("RT @"):
        return None

    short_year = str(year)[2:]
    year_tokens = "|".join(re.escape(token) for token in (f"{year}年", f"{short_year}年"))
    has_explicit_year = bool(re.search(rf"(?:{year_tokens})", cleaned, re.IGNORECASE))
    has_year_month_phrase = bool(
        re.search(rf"(?:{year_tokens})\s*(?:1[0-2]|[1-9])月", cleaned, re.IGNORECASE)
    )
    has_report_keyword = bool(
        re.search(r"(?:年間|年最多|年最高|結果|実績|戦績|総括|振り返り|着地|まとめ)", cleaned)
    )
    has_strong_report_keyword = bool(
        re.search(r"(?:結果|実績|戦績|総括|振り返り|着地|今年|本年|まとめ)", cleaned)
    )
    has_promo_keyword = bool(
        re.search(r"(?:tips?|TIPS|運用術|攻略|ノウハウ|講習|コンサル|教材|方法)", cleaned)
    )
    has_goal_keyword = bool(re.search(r"(?:目標|予定|目指す|狙う|達成したい)", cleaned))

    if re.search(r"累計|通算|total|トータル", cleaned, re.IGNORECASE):
        return None
    if has_year_month_phrase and not has_strong_report_keyword:
        return None
    if strict and not has_explicit_year and not has_strong_report_keyword:
        return None
    if strict and not has_explicit_year and has_promo_keyword:
        return None
    if strict and has_goal_keyword:
        return None
    if strict and re.search(r"(?:予感|見込み|予定|なりそう)", cleaned):
        return None

    month_count_matches = list(
        re.finditer(
            r"(1[0-2]|[1-9])月\s*[=:：]?\s*(\d+)\s*(?:即|get|g\b)",
            cleaned,
            re.IGNORECASE,
        )
    )
    if len(month_count_matches) >= 3:
        month_values = {}
        for match in month_count_matches:
            month_values[int(match.group(1))] = int(match.group(2))

        non_month_counts = []
        for match in re.finditer(
            r"(?:(?:計|合計|総計|トータル|total)\s*)?(\d+)\s*(?:即|get|g\b)",
            cleaned,
            re.IGNORECASE,
        ):
            value = int(match.group(1))
            if not 0 < value <= 2000:
                continue
            before = cleaned[max(0, match.start() - 6) : match.start()]
            if re.search(r"(?:1[0-2]|[1-9])月\s*$", before):
                continue
            non_month_counts.append(match)

        last_month_end = month_count_matches[-1].end()
        first_month_start = month_count_matches[0].start()
        total_keyword_pattern = re.compile(
            r"(?:計|合計|総計|トータル|total)", re.IGNORECASE
        )

        for match in non_month_counts:
            if match.start() >= last_month_end and total_keyword_pattern.search(match.group(0)):
                return int(match.group(1))
        for match in non_month_counts:
            if match.start() >= last_month_end:
                return int(match.group(1))
        for match in non_month_counts:
            if match.start() < first_month_start and total_keyword_pattern.search(match.group(0)):
                return int(match.group(1))
        if len(month_values) >= 10:
            summed = sum(month_values.values())
            if 0 < summed <= 2000:
                return summed

    patterns = [
        rf"(?:{year_tokens})\s*(?:の)?\s*(?:結果|実績|戦績|総括|振り返り|着地|まとめ)?\s*[=:：／/|は]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b)",
        rf"(?:{year_tokens}).{{0,30}}?(?:計|合計|結果|実績|戦績|総括|振り返り|着地|まとめ)?\s*(\d+)\s*(?:即|get|g\b)",
        r"(?:年間|年最多|年最高)\s*(?:は|の)?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b)",
        r"(?:今年|本年)\s*(?:は|の結果|の実績|の総括|の振り返り|の着地|のまとめ)?\s*[=:：／/|]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b)",
    ]

    year_month_pattern = re.compile(
        rf"(?:{year_tokens})\s*(?:1[0-2]|[1-9])月", re.IGNORECASE
    )
    annual_context_pattern = re.compile(
        r"(?:年間|年最多|年最高|今年|本年|総括|振り返り|着地|まとめ)"
    )

    for pattern in patterns:
        for match in re.finditer(pattern, cleaned, re.IGNORECASE):
            window = cleaned[max(0, match.start() - 16) : min(len(cleaned), match.end() + 16)]
            if year_month_pattern.search(window) and not annual_context_pattern.search(window):
                continue
            value = int(match.group(1))
            if 0 < value <= 2000:
                return value
    return None


def extract_yearly_profile_count(text, year):
    """プロフィール系テキストから明示的な年間即数だけを拾う。"""
    cleaned = clean_tweet_text(text)
    if not cleaned:
        return None

    short_year = str(year)[2:]
    count_unit = r"(?:即|get|g\b)"
    count_suffix = rf"(?:\([^)]{{0,30}}\))?\s*{count_unit}"
    patterns = [
        rf"(?:(?:{year}|{short_year})年)\s*(?:→|:|：|は|=)?\s*(\d+)(?:\(\+\d+\))?\s*{count_suffix}",
        rf"(?:(?:{year}|{short_year})年).{{0,14}}?(\d+)(?:\(\+\d+\))?\s*{count_suffix}",
        rf"(?:(?:{year}|{short_year})年)\s*(\d+)(?=\s*(?:\([^)]{{0,30}}\))?\s*(?:即|get|g\b)|[/|,、 ])",
        rf"(?<!\d)(?:{year}|{short_year})\s*[:：]\s*(\d+)(?:\(\+\d+\))?(?=\s*即|[/|,、 ])",
    ]
    exclude_pattern = re.compile(
        rf"(?:\d{{1,2}}月|\d{{1,2}}日|FY|年度|上半期|下半期|目標|予定|目指す|"
        rf"累計|通算|合計|開始|から|今ここ|"
        rf"(?:\d{{2,4}})\s*[-〜~]\s*(?:{year}|{short_year})年)"
    )

    for pattern in patterns:
        for match in re.finditer(pattern, cleaned):
            window = cleaned[max(0, match.start() - 8) : min(len(cleaned), match.end() + 8)]
            if exclude_pattern.search(window):
                continue
            value = int(match.group(1))
            if 0 < value <= 2000:
                return value
    return None


def extract_yearly_profile_month_series_count(text, year):
    """プロフィール内の月別内訳から対象年の年間即数を合算する。"""
    cleaned = clean_tweet_text(text)
    if not cleaned:
        return None

    normalized = unicodedata.normalize("NFKC", cleaned)
    short_year = str(year)[2:]
    next_year = year + 1
    next_short_year = f"{next_year % 100:02d}"
    start_tokens = [
        str(year),
        f"{short_year}年",
        f"{year}.",
        f"{year}/",
        f"{short_year}.",
        f"{short_year}/",
    ]
    end_tokens = [
        str(next_year),
        f"{next_short_year}年",
        f"{next_year}.",
        f"{next_year}/",
        f"{next_short_year}.",
        f"{next_short_year}/",
    ]
    best = None

    def extract_month_pairs(segment):
        pairs = {}
        i = 0
        while i < len(segment) - 1:
            if not segment[i].isdigit():
                i += 1
                continue
            j = i
            while j < len(segment) and segment[j].isdigit():
                j += 1
            if j >= len(segment) or segment[j] != "月":
                i = j
                continue

            k = j + 1
            while k < len(segment) and segment[k] in " :：/.-~〜":
                k += 1
            n = k
            while n < len(segment) and segment[n].isdigit():
                n += 1
            if n == k:
                i = j
                continue

            month = int(segment[i:j])
            value = int(segment[k:n])
            if 1 <= month <= 12:
                pairs[month] = value
            i = j

        for match in re.finditer(r"/(1[0-2]|[1-9])\s+(\d{1,3})(?=(?:/(?:1[0-2]|[1-9])\s+\d{1,3})|[^0-9]|$)", segment):
            month = int(match.group(1))
            value = int(match.group(2))
            if 1 <= month <= 12:
                pairs[month] = value
        return pairs

    for token in start_tokens:
        search_from = 0
        while True:
            start = normalized.find(token, search_from)
            if start == -1:
                break
            if start > 0 and normalized[start - 1].isdigit():
                search_from = start + len(token)
                continue

            end = len(normalized)
            next_indexes = [
                normalized.find(next_token, start + len(token))
                for next_token in end_tokens
            ]
            next_indexes = [idx for idx in next_indexes if idx != -1]
            if next_indexes:
                end = min(next_indexes)

            segment = normalized[start:end]
            pairs = extract_month_pairs(segment)
            if len(pairs) >= 5:
                total = sum(pairs.values())
                score = (len(pairs), total)
                if 0 < total <= 2000 and (best is None or score > best[0]):
                    best = (score, total)

            search_from = start + len(token)

    return best[1] if best else None


def extract_monthly_profile_count(text, year, month):
    """プロフィール内の対象年月の月次内訳から月間即数を抽出する。"""
    cleaned = clean_tweet_text(text)
    if not cleaned:
        return None

    normalized = unicodedata.normalize("NFKC", cleaned)
    short_year = str(year)[2:]
    next_year = year + 1
    next_short_year = f"{next_year % 100:02d}"
    start_tokens = [
        str(year),
        f"{short_year}年",
        f"{year}.",
        f"{year}/",
        f"{short_year}.",
        f"{short_year}/",
    ]
    end_tokens = [
        str(next_year),
        f"{next_short_year}年",
        f"{next_year}.",
        f"{next_year}/",
        f"{next_short_year}.",
        f"{next_short_year}/",
    ]

    def extract_month_pairs(segment):
        pairs = {}
        i = 0
        while i < len(segment) - 1:
            if not segment[i].isdigit():
                i += 1
                continue
            j = i
            while j < len(segment) and segment[j].isdigit():
                j += 1
            if j >= len(segment) or segment[j] != "月":
                i = j
                continue

            k = j + 1
            while k < len(segment) and segment[k] in " :：/.-~〜":
                k += 1
            n = k
            while n < len(segment) and segment[n].isdigit():
                n += 1
            if n == k:
                i = j
                continue

            parsed_month = int(segment[i:j])
            value = int(segment[k:n])
            if 1 <= parsed_month <= 12:
                pairs[parsed_month] = value
            i = j

        for match in re.finditer(
            r"/(1[0-2]|[1-9])\s+(\d{1,3})(?=(?:/(?:1[0-2]|[1-9])\s+\d{1,3})|[^0-9]|$)",
            segment,
        ):
            parsed_month = int(match.group(1))
            value = int(match.group(2))
            if 1 <= parsed_month <= 12:
                pairs[parsed_month] = value
        return pairs

    best = None
    for token in start_tokens:
        search_from = 0
        while True:
            start = normalized.find(token, search_from)
            if start == -1:
                break
            if start > 0 and normalized[start - 1].isdigit():
                search_from = start + len(token)
                continue

            end = len(normalized)
            next_indexes = [
                normalized.find(next_token, start + len(token))
                for next_token in end_tokens
            ]
            next_indexes = [idx for idx in next_indexes if idx != -1]
            if next_indexes:
                end = min(next_indexes)

            segment = normalized[start:end]
            pairs = extract_month_pairs(segment)
            value = pairs.get(month)
            if value is not None and len(pairs) >= 2 and 0 < value <= 500:
                score = (len(pairs), value)
                if best is None or score > best[0]:
                    best = (score, value)

            search_from = start + len(token)

    if best:
        return best[1]

    month_pattern = re.compile(
        rf"(?:(?:{year}|{short_year})年).{{0,24}}?{month}月\s*[:：]?\s*(\d+)\s*(?:即|get|g\b)",
        re.IGNORECASE,
    )
    match = month_pattern.search(normalized)
    if match:
        value = int(match.group(1))
        if 0 < value <= 500:
            return value
    return None


def find_monthly_profile_hit(account, year, month):
    for source in ("bio", "location", "display_name"):
        text = account.get(source, "")
        count = extract_monthly_profile_count(text, year, month)
        if not count:
            continue
        return {
            "count": count,
            "url": f"https://x.com/{account['username']}",
            "text": clean_tweet_text(text)[:240],
            "created_at": "",
            "source_field": source,
        }
    return None


def find_yearly_profile_hit(account, year):
    for source in ("bio", "location", "display_name"):
        text = account.get(source, "")
        count = extract_yearly_profile_count(text, year)
        if not count:
            count = extract_yearly_profile_month_series_count(text, year)
        if not count:
            continue
        return {
            "count": count,
            "url": f"https://x.com/{account['username']}",
            "text": clean_tweet_text(text)[:240],
            "created_at": "",
            "source_field": source,
        }
    return None


def build_reporting_window(mode, year, month=None):
    if mode == "yearly":
        start = datetime(year, 12, 20)
        end = datetime(year + 1, 1, 15)
    else:
        if month is None:
            raise ValueError("monthly mode requires month")
        start = datetime(year, month, 20)
        if month == 12:
            end = datetime(year + 1, 1, 15)
        else:
            end = datetime(year, month + 1, 15)
    return start.date(), end.date()


def is_in_reporting_window(created_at, mode, year, month=None):
    if not created_at:
        return True

    try:
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return True

    start_date, end_date = build_reporting_window(mode, year, month)
    tweet_date = dt.date()
    return start_date <= tweet_date <= end_date


def build_search_query(username, mode, year, month=None):
    start_date, end_date = build_reporting_window(mode, year, month)
    until_date = end_date + timedelta(days=1)

    if mode == "yearly":
        short_year = str(year)[2:]
        keywords = (
            f'("{year}年" OR "{short_year}年" OR 年間 OR 年最多 OR 年最高 '
            "OR 総括 OR 振り返り OR 着地 OR 戦績 OR 今年 OR 本年 OR まとめ)"
        )
    else:
        keywords = (
            f'("{month}月" OR 月間 OR 今月 OR 結果 OR 実績 OR 総括 '
            "OR 戦績 OR 着地 OR 報告 OR 振り返り OR まとめ OR 即 OR get OR そ)"
        )

    return (
        f"from:{username} {keywords} "
        f"since:{start_date.isoformat()} until:{until_date.isoformat()}"
    )


def build_global_search_query_groups(mode, year, month=None):
    start_date, end_date = build_reporting_window(mode, year, month)
    until_date = end_date + timedelta(days=1)

    if mode == "yearly":
        short_year = str(year)[2:]
        base = (
            f'since:{start_date.isoformat()} until:{until_date.isoformat()} '
            f'("{year}年" OR "{short_year}年" OR 今年 OR 本年 OR 年間)'
        )
        return [
            [
                base + " (総括 OR 結果 OR 実績 OR 戦績 OR 振り返り OR 着地 OR まとめ)",
                base + " (総括 OR 結果 OR 実績 OR まとめ)",
                base + " (結果 OR 実績)",
            ],
            [
                base + " (即 OR get)",
                f'since:{start_date.isoformat()} until:{until_date.isoformat()} "{year}年" 即',
                f'since:{start_date.isoformat()} until:{until_date.isoformat()} 今年 即',
            ],
        ]

    base = (
        f'since:{start_date.isoformat()} until:{until_date.isoformat()} '
        f'("{month}月" OR "{month} 月" OR 月間 OR 今月)'
    )
    return [
        [
            base + " (総括 OR 結果 OR 実績 OR 戦績 OR 振り返り OR 着地 OR 報告 OR まとめ)",
            base + " (総括 OR 結果 OR 実績 OR 報告)",
            base + " (結果 OR 実績)",
        ],
        [
            base + " (即 OR get OR そ)",
            f'since:{start_date.isoformat()} until:{until_date.isoformat()} "{month}月" 即',
            f'since:{start_date.isoformat()} until:{until_date.isoformat()} 月間 即',
            f'since:{start_date.isoformat()} until:{until_date.isoformat()} 今月 即',
        ],
    ]


def build_global_search_queries(mode, year, month=None):
    return [group[0] for group in build_global_search_query_groups(mode, year, month)]


def pick_best_hit(tweets, username, mode, year, month=None, strict=False):
    best_hit = None

    for tweet in tweets:
        if not is_in_reporting_window(tweet.get("created_at", ""), mode, year, month):
            continue
        if mode == "monthly":
            count = extract_monthly_count(
                tweet.get("text", ""), year, month, strict=strict
            )
        else:
            count = extract_yearly_count(tweet.get("text", ""), year, strict=strict)
        if not count:
            continue

        hit = {
            "count": count,
            "url": f"https://x.com/{username}/status/{tweet['id']}",
            "text": clean_tweet_text(tweet.get("text", ""))[:240],
            "created_at": tweet.get("created_at", ""),
        }
        if best_hit is None or count > best_hit["count"]:
            best_hit = hit

    return best_hit


def pick_best_hits_by_user(tweets, usernames, mode, year, month=None, strict=False):
    targets = {username.lower(): username for username in usernames}
    best_hits = {}

    for tweet in tweets:
        username = (tweet.get("username") or "").lower()
        original_username = targets.get(username)
        if not original_username:
            continue
        if not is_in_reporting_window(tweet.get("created_at", ""), mode, year, month):
            continue
        if mode == "monthly":
            count = extract_monthly_count(
                tweet.get("text", ""), year, month, strict=strict
            )
        else:
            count = extract_yearly_count(tweet.get("text", ""), year, strict=strict)
        if not count:
            continue

        hit = {
            "username": original_username,
            "count": count,
            "url": f"https://x.com/{original_username}/status/{tweet['id']}",
            "text": clean_tweet_text(tweet.get("text", ""))[:240],
            "created_at": tweet.get("created_at", ""),
        }
        current = best_hits.get(original_username)
        if current is None or count > current["count"]:
            best_hits[original_username] = hit

    return best_hits


def merge_best_hit_maps(best_hits, new_hits):
    merged = dict(best_hits or {})
    for username, hit in (new_hits or {}).items():
        current = merged.get(username)
        if current is None or hit["count"] > current["count"]:
            merged[username] = hit
    return merged


async def search_query_tweets(context, query, scrolls=3, return_meta=False):
    """Playwright の検索画面で SearchTimeline を拾う。"""
    captured = []
    seen_ids = set()
    response_seen = asyncio.Event()
    response_count = 0

    async def capture(response):
        nonlocal response_count
        if "SearchTimeline" not in response.url:
            return
        try:
            body = await response.json()
        except Exception:
            return
        response_count += 1

        instructions = (
            body.get("data", {})
            .get("search_by_raw_query", {})
            .get("search_timeline", {})
            .get("timeline", {})
            .get("instructions", [])
        )
        for inst in instructions:
            tweets, _ = parse_tweet_items(inst.get("entries", []))
            for tweet in tweets:
                if tweet["id"] in seen_ids:
                    continue
                seen_ids.add(tweet["id"])
                captured.append(tweet)
        response_seen.set()

    async def block_lightweight(route):
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
        else:
            await route.continue_()

    page = await context.new_page()
    await page.route("**/*", block_lightweight)
    page.on("response", capture)
    try:
        url = f"https://x.com/search?q={requests.utils.quote(query)}&src=typed_query&f=live"
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        for _ in range(scrolls):
            try:
                await asyncio.wait_for(response_seen.wait(), timeout=4)
                response_seen.clear()
            except asyncio.TimeoutError:
                pass
            await page.mouse.wheel(0, 2400)
            await page.wait_for_timeout(900)
    except Exception:
        await page.wait_for_timeout(1500)
    finally:
        try:
            await page.close()
        except Exception:
            pass

    if return_meta:
        return captured, {"saw_response": response_count > 0, "response_count": response_count}
    return captured


async def browse_user_tweets(context, username, scrolls=4, return_meta=False):
    """Playwright のユーザーページで UserTweets を拾う。"""
    captured = []
    seen_ids = set()
    response_seen = asyncio.Event()
    response_count = 0

    async def capture(response):
        nonlocal response_count
        if "UserTweets" not in response.url:
            return
        try:
            body = await response.json()
        except Exception:
            return
        response_count += 1

        for tweet in extract_user_tweets_from_body(body):
            if tweet["id"] in seen_ids:
                continue
            seen_ids.add(tweet["id"])
            captured.append(tweet)
        response_seen.set()

    async def block_lightweight(route):
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
        else:
            await route.continue_()

    page = await context.new_page()
    await page.route("**/*", block_lightweight)
    page.on("response", capture)
    try:
        await page.goto(
            f"https://x.com/{username}",
            wait_until="domcontentloaded",
            timeout=15000,
        )
        for _ in range(scrolls):
            try:
                await asyncio.wait_for(response_seen.wait(), timeout=4)
                response_seen.clear()
            except asyncio.TimeoutError:
                pass
            await page.mouse.wheel(0, 2400)
            await page.wait_for_timeout(900)
    except Exception:
        await page.wait_for_timeout(1500)
    finally:
        try:
            await page.close()
        except Exception:
            pass

    if return_meta:
        return captured, {"saw_response": response_count > 0, "response_count": response_count}
    return captured


async def search_query_tweets_with_rotation(
    playwright_contexts, context_idx, query, scrolls=3, return_meta=False
):
    last_captured = []
    last_meta = {"saw_response": False, "response_count": 0, "contexts_tried": []}
    contexts_tried = []
    for _ in range(max(len(playwright_contexts), 1)):
        selected = get_next_playwright_context(playwright_contexts, context_idx)
        contexts_tried.append(selected["name"])
        captured, meta = await search_query_tweets(
            selected["context"],
            query,
            scrolls=scrolls,
            return_meta=True,
        )
        last_captured = captured
        last_meta = {
            **meta,
            "context_name": selected["name"],
            "contexts_tried": list(contexts_tried),
        }
        if meta["saw_response"]:
            if return_meta:
                return captured, last_meta
            return captured
    if return_meta:
        return last_captured, last_meta
    return last_captured


async def browse_user_tweets_with_rotation(
    playwright_contexts, context_idx, username, scrolls=4, return_meta=False
):
    last_captured = []
    last_meta = {"saw_response": False, "response_count": 0, "contexts_tried": []}
    contexts_tried = []
    for _ in range(max(len(playwright_contexts), 1)):
        selected = get_next_playwright_context(playwright_contexts, context_idx)
        contexts_tried.append(selected["name"])
        captured, meta = await browse_user_tweets(
            selected["context"],
            username,
            scrolls=scrolls,
            return_meta=True,
        )
        last_captured = captured
        last_meta = {
            **meta,
            "context_name": selected["name"],
            "contexts_tried": list(contexts_tried),
        }
        if meta["saw_response"]:
            if return_meta:
                return captured, last_meta
            return captured
    if return_meta:
        return last_captured, last_meta
    return last_captured


async def search_user_period(playwright_contexts, context_idx, username, mode, year, month=None):
    query = build_search_query(username, mode, year, month)
    captured = await search_query_tweets_with_rotation(
        playwright_contexts,
        context_idx,
        query,
        scrolls=3,
    )
    return pick_best_hit(captured, username, mode, year, month)


async def browse_user_period(
    playwright_contexts, context_idx, username, mode, year, month=None
):
    captured = await browse_user_tweets_with_rotation(
        playwright_contexts,
        context_idx,
        username,
        scrolls=4,
    )
    return pick_best_hit(captured, username, mode, year, month, strict=True)


def build_batch_search_query(usernames, mode, year, month=None):
    start_date, end_date = build_reporting_window(mode, year, month)
    until_date = end_date + timedelta(days=1)
    from_clause = " OR ".join(f"(from:{username})" for username in usernames)
    if mode == "yearly":
        short_year = str(year)[2:]
        keywords = (
            f'("{year}年" OR "{short_year}年" OR 今年 OR 本年 OR 年間 '
            "OR 総括 OR 戦績 OR まとめ OR 振り返り OR 即 OR get)"
        )
    else:
        keywords = (
            f'("{month}月" OR 月間 OR 今月 OR 総括 OR 戦績 OR まとめ '
            "OR 振り返り OR 即 OR get)"
        )
    return (
        f"({from_clause}) {keywords} "
        f"since:{start_date.isoformat()} until:{until_date.isoformat()}"
    )


async def search_user_batch_period(
    playwright_contexts,
    context_idx,
    usernames,
    mode,
    year,
    month=None,
    scrolls=5,
    return_meta=False,
):
    query = build_batch_search_query(usernames, mode, year, month)
    captured, meta = await search_query_tweets_with_rotation(
        playwright_contexts,
        context_idx,
        query,
        scrolls=scrolls,
        return_meta=True,
    )
    hits = pick_best_hits_by_user(captured, usernames, mode, year, month, strict=True)
    result_meta = {
        "response_seen": meta["saw_response"],
        "search_mode": "direct" if meta["saw_response"] else "no_response",
        "contexts_tried": meta.get("contexts_tried", []),
        "response_count": meta.get("response_count", 0),
        "target_count": len(usernames),
    }
    if meta["saw_response"] or len(usernames) <= 1:
        if return_meta:
            return hits, result_meta
        return hits

    split_index = len(usernames) // 2
    left_usernames = usernames[:split_index]
    right_usernames = usernames[split_index:]
    split_hits = {}
    split_response_seen = False

    for subset in (left_usernames, right_usernames):
        if not subset:
            continue
        subset_hits, subset_meta = await search_user_batch_period(
            playwright_contexts,
            context_idx,
            subset,
            mode,
            year,
            month,
            scrolls=scrolls,
            return_meta=True,
        )
        split_hits = merge_best_hit_maps(split_hits, subset_hits)
        split_response_seen = split_response_seen or subset_meta.get("response_seen", False)

    result_meta["response_seen"] = split_response_seen
    result_meta["search_mode"] = f"split:{len(left_usernames)}+{len(right_usernames)}"
    if return_meta:
        return split_hits, result_meta
    return split_hits


async def search_global_query_group(
    playwright_contexts, context_idx, query_group, scrolls=5
):
    last_captured = []
    last_meta = {
        "response_seen": False,
        "search_mode": "no_response",
        "variant_index": len(query_group),
        "variant_count": len(query_group),
    }
    for variant_index, query in enumerate(query_group, start=1):
        captured, meta = await search_query_tweets_with_rotation(
            playwright_contexts,
            context_idx,
            query,
            scrolls=scrolls,
            return_meta=True,
        )
        last_captured = captured
        last_meta = {
            "response_seen": meta["saw_response"],
            "search_mode": (
                "direct"
                if meta["saw_response"] and variant_index == 1
                else f"variant:{variant_index}/{len(query_group)}"
                if meta["saw_response"]
                else "no_response"
            ),
            "variant_index": variant_index,
            "variant_count": len(query_group),
            "contexts_tried": meta.get("contexts_tried", []),
            "response_count": meta.get("response_count", 0),
        }
        if meta["saw_response"]:
            return captured, last_meta
    return last_captured, last_meta


async def search_target_batches(
    playwright_contexts,
    context_idx,
    usernames,
    mode,
    year,
    month=None,
    batch_size=15,
    scrolls=6,
    best_hits=None,
    start_index=0,
    progress_callback=None,
):
    best_hits = dict(best_hits or {})
    total_batches = (len(usernames) + batch_size - 1) // batch_size if usernames else 0
    start_batch_number = (start_index // batch_size) + 1 if usernames else 0
    for batch_number, index in enumerate(
        range(start_index, len(usernames), batch_size),
        start=start_batch_number,
    ):
        batch = usernames[index : index + batch_size]
        hits, search_meta = await search_user_batch_period(
            playwright_contexts,
            context_idx,
            batch,
            mode,
            year,
            month,
            scrolls=scrolls,
            return_meta=True,
        )
        best_hits = merge_best_hit_maps(best_hits, hits)
        if progress_callback:
            progress_callback(
                best_hits,
                {
                    "phase": "batch",
                    "batch_number": batch_number,
                    "total_batches": total_batches,
                    "next_index": min(index + batch_size, len(usernames)),
                    "total_targets": len(usernames),
                    "batch_hits": len(hits),
                    "response_seen": search_meta.get("response_seen", False),
                    "search_mode": search_meta.get("search_mode", "direct"),
                },
            )
    return best_hits


async def search_global_period(
    playwright_contexts,
    context_idx,
    usernames,
    mode,
    year,
    month=None,
    best_hits=None,
    start_query_index=0,
    progress_callback=None,
):
    query_groups = build_global_search_query_groups(mode, year, month)
    best_hits = dict(best_hits or {})

    for query_index in range(start_query_index, len(query_groups)):
        captured, search_meta = await search_global_query_group(
            playwright_contexts,
            context_idx,
            query_groups[query_index],
            scrolls=5,
        )
        query_hits = pick_best_hits_by_user(
            captured, usernames, mode, year, month, strict=True
        )
        best_hits = merge_best_hit_maps(best_hits, query_hits)
        if progress_callback:
            progress_callback(
                best_hits,
                {
                    "phase": "global",
                    "query_index": query_index + 1,
                    "total_queries": len(query_groups),
                    "query_hits": len(query_hits),
                    "response_seen": search_meta.get("response_seen", False),
                    "search_mode": search_meta.get("search_mode", "direct"),
                },
            )

    return best_hits


def build_output_file(mode, year, month=None):
    if mode == "yearly":
        return f"data/yearly_{year}.json"
    return f"data/monthly_{year}_{month:02d}.json"


def build_state_file(mode, year, month=None):
    if mode == "yearly":
        return f"data/.collect_state_yearly_{year}.json"
    return f"data/.collect_state_monthly_{year}_{month:02d}.json"


def merge_period_results(existing_rows, new_rows, count_key):
    merged = {}

    for row in existing_rows + new_rows:
        row = normalize_period_row(row)
        username = row.get("username")
        if not username:
            continue
        current = merged.get(username)
        if current is None or build_result_score(row, count_key) > build_result_score(
            current, count_key
        ):
            merged[username] = dict(row)
        else:
            for field in (
                "display_name",
                "followers_count",
                "categories",
                "profile_image_url",
                "source_url",
                "match_source",
                "source_type",
                "profile_source_field",
            ):
                if not merged[username].get(field) and row.get(field):
                    merged[username][field] = row[field]
            if not merged[username].get("needs_review") and row.get("needs_review"):
                merged[username]["needs_review"] = True
            current_url = merged[username].get("tweet_url")
            new_url = row.get("tweet_url")
            if not current_url and new_url:
                for field in ("tweet_url", "evidence_url", "tweet_text", "tweet_created_at"):
                    if row.get(field):
                        merged[username][field] = row[field]
            elif current_url and new_url and current_url == new_url:
                for field in ("tweet_text", "tweet_created_at", "source_url"):
                    if not merged[username].get(field) and row.get(field):
                        merged[username][field] = row[field]

    return list(merged.values())


def build_record_row(base, existing, result, mode, year, month=None):
    row = normalize_period_row(existing or {})
    result = normalize_period_row(result)
    row["username"] = result["username"]
    row["display_name"] = (
        result.get("display_name")
        or base.get("display_name")
        or row.get("display_name", "")
    )
    row["sokusuu"] = base.get("sokusuu", row.get("sokusuu", 0))
    row["followers_count"] = (
        result.get("followers_count")
        or base.get("followers_count")
        or row.get("followers_count", 0)
    )
    row["categories"] = (
        result.get("categories")
        or base.get("categories")
        or row.get("categories", "")
    )
    row["profile_image_url"] = (
        result.get("profile_image_url")
        or base.get("profile_image_url")
        or row.get("profile_image_url", "")
    )

    if mode == "yearly":
        row["yearly_best"] = result["yearly_count"]
        row["achieved_year"] = year
    else:
        row["monthly_best"] = result["monthly_count"]
        row["achieved_date"] = f"{year}-{month:02d}"

    row["match_source"] = result.get("match_source", row.get("match_source", ""))
    row["source_type"] = result.get("source_type", row.get("source_type", "tweet_evidence"))
    row["source_url"] = result.get("source_url", row.get("source_url", ""))
    row["needs_review"] = bool(result.get("needs_review", False))
    profile_source_field = result.get("profile_source_field", "")
    if profile_source_field:
        row["profile_source_field"] = profile_source_field
    else:
        row.pop("profile_source_field", None)

    evidence_url = get_evidence_url(result) or get_evidence_url(row)
    if evidence_url:
        row["evidence_url"] = evidence_url
    else:
        row.pop("evidence_url", None)

    return normalize_period_row(row)


def update_record_rankings(accounts, results, mode, year, month=None):
    ranking_path = YEARLY_RANKING_JSON if mode == "yearly" else MONTHLY_RANKING_JSON
    value_field = "yearly_best" if mode == "yearly" else "monthly_best"
    date_field = "achieved_year" if mode == "yearly" else "achieved_date"

    existing_rows = load_json(ranking_path, [])
    existing_map = {row["username"]: row for row in existing_rows}
    accounts_map = {row["username"]: row for row in accounts}
    updated = 0

    for result in results:
        username = result["username"]
        existing = existing_map.get(username, {})
        current_value = existing.get(value_field) or 0
        new_value = result["yearly_count"] if mode == "yearly" else result["monthly_count"]

        should_replace = new_value > current_value
        should_fill = (
            new_value == current_value
            and (
                not existing.get(date_field)
                or (get_evidence_url(result) and not get_evidence_url(existing))
            )
        )
        if not should_replace and not should_fill:
            continue

        existing_map[username] = build_record_row(
            accounts_map.get(username, {}),
            existing,
            result,
            mode,
            year,
            month,
        )
        updated += 1

    ranking_rows = sorted(
        existing_map.values(),
        key=lambda row: (
            -(row.get(value_field) or 0),
            -(row.get("followers_count") or 0),
            row.get("username", ""),
        ),
    )
    save_json(ranking_path, ranking_rows)
    print(f"ランキング更新: {ranking_path} ({updated}件更新)")


def validate_args(args):
    if args.mode == "monthly" and not args.month:
        raise SystemExit("--mode monthly では --month が必要です")
    if args.mode == "yearly" and args.month:
        raise SystemExit("--mode yearly では --month は不要です")
    if args.month and not 1 <= args.month <= 12:
        raise SystemExit("--month は 1-12 を指定してください")
    if args.usernames_file and not os.path.exists(args.usernames_file):
        raise SystemExit(f"--usernames-file が見つかりません: {args.usernames_file}")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size は 1 以上を指定してください")
    if args.batch_scrolls <= 0:
        raise SystemExit("--batch-scrolls は 1 以上を指定してください")


def build_playwright_cookies(raw):
    cookies = []
    for cookie in raw:
        item = {
            "name": cookie["name"],
            "value": cookie["value"],
            "domain": cookie.get("domain", ".x.com"),
            "path": cookie.get("path", "/"),
        }
        if cookie.get("secure"):
            item["secure"] = True
        if cookie.get("httpOnly"):
            item["httpOnly"] = True
        cookies.append(item)
    return cookies


def load_playwright_cookie_sets():
    cookie_sets = []
    for cookie_file in COOKIE_FILES:
        if not os.path.exists(cookie_file):
            continue
        try:
            raw = load_json(cookie_file, [])
            cookies = build_playwright_cookies(raw)
            if not cookies:
                continue
            cookie_sets.append(
                {
                    "name": os.path.basename(cookie_file),
                    "cookies": cookies,
                }
            )
        except Exception:
            continue
    if not cookie_sets:
        raise FileNotFoundError(
            "Playwright 用 Cookie が見つかりません: " + ", ".join(COOKIE_FILES)
        )
    return cookie_sets


async def create_playwright_browser(playwright, headless=True):
    return await playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--lang=ja-JP",
        ],
    )


async def create_playwright_context(browser, cookies):
    context = await browser.new_context(
        user_agent=PLAYWRIGHT_USER_AGENT,
        viewport={"width": 1280, "height": 720},
        locale="ja-JP",
        timezone_id="Asia/Tokyo",
        color_scheme="light",
        device_scale_factor=1,
        has_touch=False,
        extra_http_headers={
            "accept-language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    await context.add_init_script(PLAYWRIGHT_STEALTH_SCRIPT)
    await context.add_cookies(cookies)
    return context


async def create_playwright_contexts(playwright, cookie_sets, headless=True):
    browser = await create_playwright_browser(playwright, headless=headless)
    contexts = []
    for cookie_set in cookie_sets:
        context = await create_playwright_context(browser, cookie_set["cookies"])
        contexts.append(
            {
                "name": cookie_set["name"],
                "context": context,
            }
        )
    return browser, contexts


def get_next_playwright_context(playwright_contexts, context_idx):
    if not playwright_contexts:
        raise ValueError("Playwright context がありません")
    selected_idx = context_idx[0] % len(playwright_contexts)
    context_idx[0] = (selected_idx + 1) % len(playwright_contexts)
    return playwright_contexts[selected_idx]


async def main_async():
    from playwright.async_api import async_playwright

    parser = argparse.ArgumentParser(description="月間/年間即数ランキング収集")
    parser.add_argument("--mode", choices=["monthly", "yearly"], default="monthly")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int)
    parser.add_argument("--limit", type=int, default=0, help="チェック件数（0=全件）")
    parser.add_argument(
        "--usernames-file",
        help="対象 username 一覧ファイル（1行1件、@付き可、#コメント可）",
    )
    parser.add_argument(
        "--skip-ranking-update",
        action="store_true",
        help="monthly_ranking.json / yearly_ranking.json を更新しない",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="中断時のチェックポイントから再開する",
    )
    parser.add_argument(
        "--search-fallback",
        action="store_true",
        help="タイムラインで拾えない場合に SearchTimeline も試す",
    )
    parser.add_argument(
        "--global-search",
        action="store_true",
        help="広い検索クエリで対象ユーザーの月報/年報を先にまとめて拾う",
    )
    parser.add_argument(
        "--prefetch-only",
        action="store_true",
        help="広域/バッチ検索だけ先に回し、個別タイムライン走査を省略する",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=1,
        help="何件ごとにチェックポイント保存するか（既定: 1）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=15,
        help="batch Search 1回あたりの対象ユーザー数（既定: 15）",
    )
    parser.add_argument(
        "--batch-scrolls",
        type=int,
        default=6,
        help="batch Search 1回あたりのスクロール回数（既定: 6）",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Playwright をブラウザ表示ありで起動する（Search が headless で弱い時用）",
    )
    args = parser.parse_args()
    validate_args(args)

    accounts = load_json(OUTPUT_JSON, [])
    accounts_map = {row["username"]: row for row in accounts}
    output_file = build_output_file(args.mode, args.year, args.month)
    state_file = build_state_file(args.mode, args.year, args.month)
    if args.usernames_file:
        requested_usernames = load_usernames_file(args.usernames_file)
        if not requested_usernames:
            raise SystemExit("--usernames-file に対象ユーザーがありません")
        accounts_by_lower = {row["username"].lower(): row for row in accounts}
        missing_usernames = []
        targets = []
        for username in requested_usernames:
            account = accounts_by_lower.get(username.lower())
            if not account:
                missing_usernames.append(username)
                continue
            targets.append(account)
        if not targets:
            raise SystemExit(
                "--usernames-file のユーザーが data/sokusuu_accounts.json にありません"
            )
    else:
        missing_usernames = []
        targets = list(accounts)
    if args.limit > 0:
        targets = targets[: args.limit]
    sessions = create_sessions()
    playwright_cookie_sets = load_playwright_cookie_sets()
    session_idx = [0]
    processed_usernames = set()
    results = []
    results_map = {}
    value_key = "yearly_count" if args.mode == "yearly" else "monthly_count"
    prefetch_state = {
        "complete": False,
        "global_query_index": 0,
        "batch_offset": 0,
    }

    if args.resume and os.path.exists(state_file):
        state = load_json(state_file, {"processed_usernames": [], "results": []})
        processed_usernames = set(state.get("processed_usernames", []))
        results = state.get("results", [])
        results_map = {row["username"]: row for row in results}
        results = list(results_map.values())
        saved_prefetch_state = state.get("prefetch_state", {})
        prefetch_state["complete"] = bool(saved_prefetch_state.get("complete", False))
        prefetch_state["global_query_index"] = int(
            saved_prefetch_state.get("global_query_index", 0)
        )
        prefetch_state["batch_offset"] = int(saved_prefetch_state.get("batch_offset", 0))
        targets = [
            account
            for account in targets
            if account["username"] not in processed_usernames
        ]
    prefetched_hits = restore_prefetched_hits(results, value_key)

    label = (
        f"{args.year}年 年間即数ランキング収集"
        if args.mode == "yearly"
        else f"{args.year}年{args.month}月 月間即数ランキング収集"
    )
    print("=" * 60)
    print(label)
    print("=" * 60)
    print(f"チェック対象: {len(targets)}アカウント")
    if args.usernames_file:
        print(f"対象リスト: {args.usernames_file}")
        if missing_usernames:
            print(f"対象外（未登録 username）: {len(missing_usernames)}件")
    if sessions:
        print(f"APIフォールバック: {len(sessions)} Cookie 利用可")
    else:
        print("APIフォールバック: 利用不可（Cookieなし）")
    print(f"Playwright Browser: {len(playwright_cookie_sets)} Cookie 利用可")
    if args.resume and processed_usernames:
        print(
            f"再開: {len(processed_usernames)}件処理済み / "
            f"{len(results)}件ヒット済み"
        )
    if args.prefetch_only:
        print("個別タイムライン走査: スキップ (--prefetch-only)")
    if args.global_search:
        print(
            f"batch Search 設定: batch_size={args.batch_size}, "
            f"batch_scrolls={args.batch_scrolls}"
        )
    print()

    async with async_playwright() as p:
        browser, playwright_contexts = await create_playwright_contexts(
            p,
            playwright_cookie_sets,
            headless=not args.headful,
        )
        playwright_context_idx = [0]

        def upsert_result(candidate):
            nonlocal results
            username = candidate["username"]
            if should_replace_result(results_map.get(username), candidate, value_key):
                results_map[username] = candidate
                results = list(results_map.values())
                return True
            return False

        def refresh_prefetched_results():
            nonlocal results
            for username, hit in prefetched_hits.items():
                account = dict(accounts_map.get(username, {}))
                account.setdefault("username", username)
                candidate = build_period_result(account, hit, value_key, "global_search")
                if should_replace_result(results_map.get(username), candidate, value_key):
                    results_map[username] = candidate
            results = list(results_map.values())

        def save_state():
            save_collect_state(
                state_file,
                processed_usernames,
                results,
                prefetch_state if args.global_search else None,
            )

        if args.global_search:
            prefetch_usernames = [account["username"] for account in targets]

            def on_prefetch_progress(best_hits, progress):
                prefetched_hits.clear()
                prefetched_hits.update(best_hits)
                refresh_prefetched_results()
                search_mode_suffix = ""
                if progress.get("search_mode") and progress["search_mode"] != "direct":
                    search_mode_suffix = f" mode={progress['search_mode']}"
                if progress["phase"] == "global":
                    prefetch_state["global_query_index"] = progress["query_index"]
                    print(
                        f"  [PREFETCH:global {progress['query_index']}/"
                        f"{progress['total_queries']}] "
                        f"query_hits={progress['query_hits']} total_hits={len(prefetched_hits)}"
                        f"{search_mode_suffix}"
                    )
                else:
                    prefetch_state["batch_offset"] = progress["next_index"]
                    print(
                        f"  [PREFETCH:batch {progress['batch_number']}/"
                        f"{progress['total_batches']}] "
                        f"batch_hits={progress['batch_hits']} "
                        f"targets={progress['next_index']}/{progress['total_targets']} "
                        f"total_hits={len(prefetched_hits)}"
                        f"{search_mode_suffix}"
                    )
                if args.checkpoint_every > 0:
                    save_state()

            if prefetch_state["complete"]:
                print(f"事前Search再利用: {len(prefetched_hits)}件")
            else:
                total_global_queries = len(
                    build_global_search_query_groups(args.mode, args.year, args.month)
                )
                if 0 < prefetch_state["global_query_index"] < total_global_queries:
                    print(
                        "事前Search再開: global query "
                        f"{prefetch_state['global_query_index'] + 1} から"
                    )
                prefetched_hits = await search_global_period(
                    playwright_contexts,
                    playwright_context_idx,
                    prefetch_usernames,
                    args.mode,
                    args.year,
                    args.month,
                    best_hits=prefetched_hits,
                    start_query_index=prefetch_state["global_query_index"],
                    progress_callback=on_prefetch_progress,
                )
                prefetch_state["global_query_index"] = total_global_queries
                if 0 < prefetch_state["batch_offset"] < len(prefetch_usernames):
                    print(
                        "事前Search再開: batch "
                        f"{(prefetch_state['batch_offset'] // args.batch_size) + 1} から"
                    )
                prefetched_hits = await search_target_batches(
                    playwright_contexts,
                    playwright_context_idx,
                    prefetch_usernames,
                    args.mode,
                    args.year,
                    args.month,
                    batch_size=args.batch_size,
                    scrolls=args.batch_scrolls,
                    best_hits=prefetched_hits,
                    start_index=prefetch_state["batch_offset"],
                    progress_callback=on_prefetch_progress,
                )
                prefetch_state["batch_offset"] = len(prefetch_usernames)
                prefetch_state["complete"] = True
                refresh_prefetched_results()
                if args.checkpoint_every > 0:
                    save_state()
            if prefetched_hits:
                print(f"事前Searchヒット: {len(prefetched_hits)}件")

        for index, account in enumerate(targets, 1):
            username = account["username"]
            prefetched = prefetched_hits.get(username)
            if prefetched:
                processed_usernames.add(username)
                if args.checkpoint_every > 0 and index % args.checkpoint_every == 0:
                    save_state()
                continue
            if args.prefetch_only:
                if args.mode == "yearly":
                    hit = find_yearly_profile_hit(account, args.year)
                    if hit:
                        result = build_period_result(
                            account,
                            hit,
                            value_key,
                            f"profile_{hit['source_field']}",
                        )
                        upsert_result(result)
                        print(
                            f"  [HIT:profile_{hit['source_field']}] @{username}: "
                            f"{hit['count']}即 -> {hit['url']}"
                        )
                else:
                    hit = find_monthly_profile_hit(account, args.year, args.month)
                    if hit:
                        result = build_period_result(
                            account,
                            hit,
                            value_key,
                            f"profile_{hit['source_field']}",
                        )
                        upsert_result(result)
                        print(
                            f"  [HIT:profile_{hit['source_field']}] @{username}: "
                            f"{hit['count']}即 -> {hit['url']}"
                        )

                processed_usernames.add(username)
                if args.checkpoint_every > 0 and index % args.checkpoint_every == 0:
                    save_state()

                if index % 20 == 0:
                    print(f"  [{index}/{len(targets)}] {len(results)}件ヒット")
                continue
            hit = None
            match_source = "search"

            if sessions:
                user_id = get_user_id(sessions, session_idx, username)
                if user_id:
                    timeline_tweets = get_user_tweets(
                        sessions, session_idx, user_id, count=80, max_pages=6
                    )
                    hit = pick_best_hit(
                        timeline_tweets,
                        username,
                        args.mode,
                        args.year,
                        args.month,
                        strict=True,
                    )
                    if hit:
                        match_source = "timeline"

            if not hit:
                hit = await browse_user_period(
                    playwright_contexts,
                    playwright_context_idx,
                    username,
                    args.mode,
                    args.year,
                    args.month,
                )
                if hit:
                    match_source = "timeline_browser"

            if not hit and args.search_fallback:
                hit = await search_user_period(
                    playwright_contexts,
                    playwright_context_idx,
                    username,
                    args.mode,
                    args.year,
                    args.month,
                )
                if hit:
                    match_source = "search"

            if not hit and args.mode == "yearly":
                hit = find_yearly_profile_hit(account, args.year)
                if hit:
                    match_source = f"profile_{hit['source_field']}"
            if not hit and args.mode == "monthly":
                hit = find_monthly_profile_hit(account, args.year, args.month)
                if hit:
                    match_source = f"profile_{hit['source_field']}"

            if hit:
                result = build_period_result(account, hit, value_key, match_source)
                upsert_result(result)
                print(
                    f"  [HIT:{match_source}] @{username}: "
                    f"{hit['count']}即 -> {hit['url']}"
                )

            processed_usernames.add(username)
            if args.checkpoint_every > 0 and index % args.checkpoint_every == 0:
                save_state()

            if index % 20 == 0:
                print(f"  [{index}/{len(targets)}] {len(results)}件ヒット")

        await browser.close()

    sort_key = "yearly_count" if args.mode == "yearly" else "monthly_count"
    existing_output = load_json(output_file, [])
    results = merge_period_results(existing_output, results, sort_key)
    results.sort(
        key=lambda row: (
            -row[sort_key],
            -(row.get("followers_count") or 0),
            row["username"],
        )
    )
    save_json(output_file, results)

    if not args.skip_ranking_update:
        update_record_rankings(accounts, results, args.mode, args.year, args.month)

    if os.path.exists(state_file):
        os.remove(state_file)

    print(f"\n{'=' * 60}")
    print(label)
    print("=" * 60)
    for rank, row in enumerate(results, 1):
        print(f"  {rank}. @{row['username']}: {row[sort_key]}即")
    print(f"\n{len(results)}件 -> {output_file}")


def main():
    import asyncio

    asyncio.run(main_async())


if __name__ == "__main__":
    main()
