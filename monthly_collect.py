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


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
            sessions.append(session)
        except Exception:
            continue
    return sessions


def api_get(sessions, session_idx, url, params):
    """レート制限を回避しながら API を叩く。"""
    if not sessions:
        return None

    for _ in range(max(len(sessions) * 3, 1)):
        session = sessions[session_idx[0] % len(sessions)]
        try:
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                session_idx[0] += 1
                if session_idx[0] % len(sessions) == 0:
                    reset = resp.headers.get("x-rate-limit-reset")
                    wait = max(int(reset) - int(time.time()), 5) if reset else 60
                    print(
                        f"  [ALL RATE LIMITED] {wait}秒待機想定 -> "
                        "APIを諦めてSearchへフォールバック"
                    )
                    return None
                continue
            return resp
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            time.sleep(3)
    return None


def get_user_id(sessions, session_idx, screen_name):
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
    return user.get("rest_id")


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
        if text and tweet_id:
            tweets.append(
                {
                    "id": tweet_id,
                    "text": text,
                    "created_at": created_at,
                    "username": username,
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
        re.search(r"(?:実績|結果|戦績|総括|振り返り|着地|出撃|報告|まとめ|締め)", cleaned)
    )
    has_generic_month = bool(re.search(r"(?:月間|今月)", cleaned))
    has_promo_keyword = bool(
        re.search(r"(?:tips?|TIPS|運用術|攻略|ノウハウ|講習|コンサル|教材|方法)", cleaned)
    )
    has_goal_keyword = bool(
        re.search(r"(?:目標|あと\d+\s*即|したい|したみい|予定|目指す)", cleaned)
    )
    has_third_person_cue = bool(
        re.search(r"(?:っぽい|らしい|大先輩|他人|友達|助けていただ|助けてもら)", cleaned)
    )

    if strict and not has_explicit_month and not has_report_keyword:
        return None
    if strict and not has_explicit_month and (has_promo_keyword or has_third_person_cue):
        return None
    if strict and has_goal_keyword and not has_report_keyword:
        return None

    if re.search(r"累計|通算|total|トータル", cleaned, re.IGNORECASE):
        return None

    if not (has_report_keyword or has_generic_month):
        exclude_patterns = [
            r"\d+即したら",
            r"即すら(?:できねー|できない)",
            r"0-\d+即",
        ]
        if any(re.search(pattern, cleaned) for pattern in exclude_patterns):
            return None

    patterns = [
        rf"(?:{month_tokens})\s*[はの=:：／/|]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b|そ\b)",
        rf"(?:{month_tokens})\s*(?:結果|実績|戦績|総括|報告|着地|振り返り|まとめ|締め)\s*[】\]）」)]?\s*[=:：／/|は]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b|そ\b)",
        rf"[【\[]\s*(?:{month_tokens})\s*(?:結果|実績|戦績|総括|報告|着地|振り返り|まとめ|締め)?\s*[】\]]\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b|そ\b)",
        rf"(?:{month_tokens}).{{0,24}}?(?:計|合計|結果|実績|戦績|総括|報告|着地|振り返り|まとめ|締め)?\s*(\d+)\s*(?:即|get|g\b|そ\b)",
    ]

    if has_report_keyword:
        patterns.append(
            r"(?:今月|月間)\s*(?:は|の結果|の実績|の総括|の報告|の振り返り|のまとめ|の着地)?\s*[=:：／/|]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b|そ\b)"
        )
    if has_explicit_month or has_report_keyword:
        patterns.append(r"(\d+)\s*即\s*(?:でした|です|達成|着地)")
        patterns.append(r"(?:計|合計)\s*(\d+)\s*即")

    for pattern in patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if not match:
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
    if strict and not has_explicit_year and not has_strong_report_keyword:
        return None
    if strict and not has_explicit_year and has_promo_keyword:
        return None
    if strict and has_goal_keyword:
        return None

    patterns = [
        rf"(?:{year_tokens})\s*(?:の)?\s*(?:結果|実績|戦績|総括|振り返り|着地|まとめ)?\s*[=:：／/|は]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b)",
        rf"(?:{year_tokens}).{{0,30}}?(?:計|合計|結果|実績|戦績|総括|振り返り|着地|まとめ)?\s*(\d+)\s*(?:即|get|g\b)",
        r"(?:年間|年最多|年最高)\s*(?:は|の)?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b)",
        r"(?:今年|本年)\s*(?:は|の結果|の実績|の総括|の振り返り|の着地|のまとめ)?\s*[=:：／/|]?\s*(?:計|合計)?\s*(\d+)\s*(?:即|get|g\b)",
    ]

    for pattern in patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if not match:
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
    patterns = [
        rf"(?:(?:{year}|{short_year})年)\s*(?:→|:|：|は|=)?\s*(\d+)(?:\(\+\d+\))?\s*即",
        rf"(?:(?:{year}|{short_year})年).{{0,10}}?(\d+)(?:\(\+\d+\))?\s*即",
        rf"(?:(?:{year}|{short_year})年)\s*(\d+)(?=[/|,、 ])",
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
    start_tokens = [str(year), f"{short_year}年"]
    end_tokens = [str(next_year), f"{next_short_year}年"]
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


def build_global_search_queries(mode, year, month=None):
    start_date, end_date = build_reporting_window(mode, year, month)
    until_date = end_date + timedelta(days=1)

    if mode == "yearly":
        short_year = str(year)[2:]
        base = (
            f'since:{start_date.isoformat()} until:{until_date.isoformat()} '
            f'("{year}年" OR "{short_year}年" OR 今年 OR 本年 OR 年間)'
        )
        return [
            base + " (総括 OR 結果 OR 実績 OR 戦績 OR 振り返り OR 着地 OR まとめ)",
            base + " (即 OR get)",
        ]

    base = (
        f'since:{start_date.isoformat()} until:{until_date.isoformat()} '
        f'("{month}月" OR "{month} 月" OR 月間 OR 今月)'
    )
    return [
        base + " (総括 OR 結果 OR 実績 OR 戦績 OR 振り返り OR 着地 OR 報告 OR まとめ)",
        base + " (即 OR get OR そ)",
    ]


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


async def search_query_tweets(context, query, scrolls=3):
    """Playwright の検索画面で SearchTimeline を拾う。"""
    captured = []
    seen_ids = set()
    response_seen = asyncio.Event()

    async def capture(response):
        if "SearchTimeline" not in response.url:
            return
        try:
            body = await response.json()
        except Exception:
            return

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

    return captured


async def browse_user_tweets(context, username, scrolls=4):
    """Playwright のユーザーページで UserTweets を拾う。"""
    captured = []
    seen_ids = set()
    response_seen = asyncio.Event()

    async def capture(response):
        if "UserTweets" not in response.url:
            return
        try:
            body = await response.json()
        except Exception:
            return

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

    return captured


async def search_user_period(context, username, mode, year, month=None):
    query = build_search_query(username, mode, year, month)
    captured = await search_query_tweets(context, query, scrolls=3)
    return pick_best_hit(captured, username, mode, year, month)


async def browse_user_period(context, username, mode, year, month=None):
    captured = await browse_user_tweets(context, username, scrolls=4)
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


async def search_user_batch_period(context, usernames, mode, year, month=None, scrolls=5):
    query = build_batch_search_query(usernames, mode, year, month)
    captured = await search_query_tweets(context, query, scrolls=scrolls)
    return pick_best_hits_by_user(captured, usernames, mode, year, month, strict=True)


async def search_target_batches(
    context,
    usernames,
    mode,
    year,
    month=None,
    batch_size=15,
    scrolls=6,
):
    best_hits = {}
    for index in range(0, len(usernames), batch_size):
        batch = usernames[index : index + batch_size]
        hits = await search_user_batch_period(
            context,
            batch,
            mode,
            year,
            month,
            scrolls=scrolls,
        )
        for username, hit in hits.items():
            current = best_hits.get(username)
            if current is None or hit["count"] > current["count"]:
                best_hits[username] = hit
    return best_hits


async def search_global_period(context, usernames, mode, year, month=None):
    captured = []
    seen_ids = set()

    for query in build_global_search_queries(mode, year, month):
        for tweet in await search_query_tweets(context, query, scrolls=5):
            if tweet["id"] in seen_ids:
                continue
            seen_ids.add(tweet["id"])
            captured.append(tweet)

    return pick_best_hits_by_user(captured, usernames, mode, year, month, strict=True)


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

    def row_score(row):
        if "match_source" not in row:
            source_score = 2
        elif row.get("match_source") == "search":
            source_score = 1
        else:
            source_score = 0
        return (
            row.get(count_key, 0),
            source_score,
            1 if row.get("tweet_url") else 0,
        )

    for row in existing_rows + new_rows:
        username = row.get("username")
        if not username:
            continue
        current = merged.get(username)
        if current is None or row_score(row) > row_score(current):
            merged[username] = dict(row)
        else:
            for field in (
                "display_name",
                "followers_count",
                "categories",
                "profile_image_url",
            ):
                if not merged[username].get(field) and row.get(field):
                    merged[username][field] = row[field]
            current_url = merged[username].get("tweet_url")
            new_url = row.get("tweet_url")
            if not current_url and new_url:
                for field in ("tweet_url", "tweet_text", "tweet_created_at"):
                    if row.get(field):
                        merged[username][field] = row[field]
            elif current_url and new_url and current_url == new_url:
                for field in ("tweet_text", "tweet_created_at"):
                    if not merged[username].get(field) and row.get(field):
                        merged[username][field] = row[field]

    return list(merged.values())


def build_record_row(base, existing, result, mode, year, month=None):
    row = dict(existing or {})
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

    if result.get("tweet_url"):
        row["evidence_url"] = result["tweet_url"]
    elif row.get("evidence_url"):
        row["evidence_url"] = row["evidence_url"]

    return row


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
                or (result.get("tweet_url") and not existing.get("evidence_url"))
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


def load_playwright_cookies():
    if not os.path.exists(PRIMARY_COOKIE_FILE):
        raise FileNotFoundError(f"{PRIMARY_COOKIE_FILE} が見つかりません")

    raw = load_json(PRIMARY_COOKIE_FILE, [])
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


async def main_async():
    from playwright.async_api import async_playwright

    parser = argparse.ArgumentParser(description="月間/年間即数ランキング収集")
    parser.add_argument("--mode", choices=["monthly", "yearly"], default="monthly")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int)
    parser.add_argument("--limit", type=int, default=0, help="チェック件数（0=全件）")
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
        "--checkpoint-every",
        type=int,
        default=1,
        help="何件ごとにチェックポイント保存するか（既定: 1）",
    )
    args = parser.parse_args()
    validate_args(args)

    accounts = load_json(OUTPUT_JSON, [])
    accounts_map = {row["username"]: row for row in accounts}
    output_file = build_output_file(args.mode, args.year, args.month)
    state_file = build_state_file(args.mode, args.year, args.month)
    targets = accounts if args.limit == 0 else accounts[: args.limit]
    sessions = create_sessions()
    session_idx = [0]
    processed_usernames = set()
    results = []
    results_map = {}

    if args.resume and os.path.exists(state_file):
        state = load_json(state_file, {"processed_usernames": [], "results": []})
        processed_usernames = set(state.get("processed_usernames", []))
        results = state.get("results", [])
        results_map = {row["username"]: row for row in results}
        results = list(results_map.values())
        targets = [
            account
            for account in targets
            if account["username"] not in processed_usernames
        ]

    label = (
        f"{args.year}年 年間即数ランキング収集"
        if args.mode == "yearly"
        else f"{args.year}年{args.month}月 月間即数ランキング収集"
    )
    print("=" * 60)
    print(label)
    print("=" * 60)
    print(f"チェック対象: {len(targets)}アカウント")
    if sessions:
        print(f"APIフォールバック: {len(sessions)} Cookie 利用可")
    else:
        print("APIフォールバック: 利用不可（Cookieなし）")
    if args.resume and processed_usernames:
        print(
            f"再開: {len(processed_usernames)}件処理済み / "
            f"{len(results)}件ヒット済み"
        )
    print()

    cookies = load_playwright_cookies()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 720},
        )
        await context.add_cookies(cookies)

        prefetched_hits = {}
        if args.global_search:
            prefetched_hits = await search_global_period(
                context,
                [account["username"] for account in targets],
                args.mode,
                args.year,
                args.month,
            )
            batch_hits = await search_target_batches(
                context,
                [account["username"] for account in targets],
                args.mode,
                args.year,
                args.month,
            )
            for username, hit in batch_hits.items():
                current = prefetched_hits.get(username)
                if current is None or hit["count"] > current["count"]:
                    prefetched_hits[username] = hit
            value_key = "yearly_count" if args.mode == "yearly" else "monthly_count"
            for username, hit in prefetched_hits.items():
                base = accounts_map.get(username, {})
                results_map[username] = {
                    "username": username,
                    "display_name": base.get("display_name", ""),
                    value_key: hit["count"],
                    "tweet_url": hit["url"],
                    "tweet_text": hit["text"],
                    "tweet_created_at": hit.get("created_at", ""),
                    "followers_count": base.get("followers_count", 0),
                    "categories": base.get("categories", ""),
                    "profile_image_url": base.get("profile_image_url", ""),
                    "match_source": "global_search",
                }
            results = list(results_map.values())
            if prefetched_hits:
                print(f"事前Searchヒット: {len(prefetched_hits)}件")

        for index, account in enumerate(targets, 1):
            username = account["username"]
            prefetched = prefetched_hits.get(username)
            if prefetched:
                processed_usernames.add(username)
                if args.checkpoint_every > 0 and index % args.checkpoint_every == 0:
                    save_json(
                        state_file,
                        {
                            "processed_usernames": sorted(processed_usernames),
                            "results": results,
                        },
                    )
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
                    context, username, args.mode, args.year, args.month
                )
                if hit:
                    match_source = "timeline_browser"

            if not hit and args.search_fallback:
                hit = await search_user_period(
                    context, username, args.mode, args.year, args.month
                )
                if hit:
                    match_source = "search"

            if not hit and args.mode == "yearly":
                hit = find_yearly_profile_hit(account, args.year)
                if hit:
                    match_source = f"profile_{hit['source_field']}"

            if hit:
                value_key = "yearly_count" if args.mode == "yearly" else "monthly_count"
                result = {
                    "username": username,
                    "display_name": account.get("display_name", ""),
                    value_key: hit["count"],
                    "tweet_url": hit["url"],
                    "tweet_text": hit["text"],
                    "tweet_created_at": hit.get("created_at", ""),
                    "followers_count": account.get("followers_count", 0),
                    "categories": account.get("categories", ""),
                    "profile_image_url": account.get("profile_image_url", ""),
                    "match_source": match_source,
                }
                existing = results_map.get(username)
                if not existing or result[value_key] > existing[value_key]:
                    results_map[username] = result
                    results = list(results_map.values())
                print(
                    f"  [HIT:{match_source}] @{username}: "
                    f"{hit['count']}即 -> {hit['url']}"
                )

            processed_usernames.add(username)
            if args.checkpoint_every > 0 and index % args.checkpoint_every == 0:
                save_json(
                    state_file,
                    {
                        "processed_usernames": sorted(processed_usernames),
                        "results": results,
                    },
                )

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
