"""
DEPTH 2 高速版: 3Cookie並列 + Followers省略
残りのフォローグラフ取得 → スコアリング → Playwright収集
"""

import asyncio
import csv
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

OUTPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_CSV = "data/sokusuu_accounts.csv"
COOKIE_FILE = "data/.twitter_cookies.json"
GRAPH_FILE = "data/follow_graph.json"
SEED_FILE = "seed_accounts.txt"
BLOCK_TYPES = {"image", "stylesheet", "font", "media"}

BEARER_TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

USER_FEATURES = json.dumps({
    "hidden_profile_subscriptions_enabled": True, "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True, "verified_phone_label_enabled": False,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "highlights_tweets_tab_ui_enabled": True, "responsive_web_twitter_article_notes_tab_enabled": True,
    "subscriptions_feature_can_gift_premium": True, "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
})

FOLLOW_FEATURES = json.dumps({
    "rweb_tipjar_consumption_enabled": True, "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False, "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True, "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True, "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False, "creator_subscriptions_quote_tweet_preview_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True, "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "rweb_video_timestamps_enabled": True, "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True, "responsive_web_enhance_cards_enabled": False,
})

SOKUSUU_PATTERNS = [
    re.compile(r"通算\s*即\s*(\d+)"), re.compile(r"即数\s*(\d+)"),
    re.compile(r"経験人数\s*(\d+)"), re.compile(r"体験人数\s*(\d+)"),
    re.compile(r"(\d+)\s*人斬り"), re.compile(r"人斬り\s*(\d+)"),
    re.compile(r"斬り数\s*(\d+)"), re.compile(r"(\d+)\s*斬り"),
    re.compile(r"斬り\s*(\d+)"), re.compile(r"total\s*(\d+)\s*即", re.IGNORECASE),
    re.compile(r"(\d+)\s*即"), re.compile(r"(\d+)\s*get", re.IGNORECASE),
    re.compile(r"ゲット数\s*(\d+)"), re.compile(r"GET\s*(\d+)", re.IGNORECASE),
    re.compile(r"GN\s*(\d+)", re.IGNORECASE), re.compile(r"S数\s*(\d+)"),
    re.compile(r"即\s*(\d+)"),
]

CATEGORY_PATTERNS = {
    "street": re.compile(r"(スト(?:ナン|リート)?|street|路上|声かけ|声掛け)", re.IGNORECASE),
    "club": re.compile(r"(クラブ|箱|club|ラウンジ|lounge|キャバ)", re.IGNORECASE),
    "online": re.compile(
        r"(ネト|アプリ|tinder|ペアーズ|pairs|タップル|tapple|bumble|マッチング|OLD|with|omiai|ネットナンパ|ネトナン|ネッナン|マチアプ)",
        re.IGNORECASE),
}

NANPA_KEYWORDS = re.compile(
    r"(pua|nanpa|soku|tinder|即|ナンパ|斬り|mote|renai|恋愛|講師|ゲット|street)", re.IGNORECASE)


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


def extract_sokusuu(text):
    if not text:
        return None
    cleaned = re.sub(r'(20[12]\d)\s*[年./]', 'YEAR_', text)
    cleaned = re.sub(r'20[12]\d/\d{1,2}/\d{1,2}', 'DATE_', cleaned)
    values = []
    for p in SOKUSUU_PATTERNS:
        values.extend(int(m) for m in p.findall(cleaned))
    return max(values) if values else None


def detect_categories(bio, username):
    text = f"{bio} {username}"
    return [cat for cat, pat in CATEGORY_PATTERNS.items() if pat.search(text)]


class TwitterGraphQL:
    def __init__(self, cookie_file, worker_id=0):
        cookies = json.load(open(cookie_file))
        cookie_dict = {c["name"]: c["value"] for c in cookies}
        ct0 = cookie_dict["ct0"]
        cookie_str = "; ".join(f'{c["name"]}={c["value"]}' for c in cookies)
        self.session = requests.Session()
        self.session.headers.update({
            "authorization": BEARER_TOKEN, "x-csrf-token": ct0, "cookie": cookie_str,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-twitter-active-user": "yes", "x-twitter-auth-type": "OAuth2Session",
        })
        self.worker_id = worker_id

    def _wait_rate_limit(self, resp):
        if resp.status_code == 429:
            reset = resp.headers.get("x-rate-limit-reset")
            wait = max(int(reset) - int(time.time()), 5) if reset else 60
            print(f"  [W{self.worker_id} RATE LIMIT] {wait}秒待機...")
            time.sleep(wait)
            return True
        return False

    def get_user_id(self, screen_name):
        variables = json.dumps({"screen_name": screen_name, "withSafetyModeUserFields": True})
        for _ in range(3):
            try:
                resp = self.session.get(
                    "https://x.com/i/api/graphql/G3KGOASz96M-Qu0nwmGXNg/UserByScreenName",
                    params={"variables": variables, "features": USER_FEATURES}, timeout=15)
            except Exception:
                time.sleep(2)
                continue
            if self._wait_rate_limit(resp):
                continue
            if resp.status_code != 200:
                return None
            user = resp.json().get("data", {}).get("user", {}).get("result", {})
            return user.get("rest_id")
        return None

    def get_following(self, user_id):
        """Followingリストのみ取得（Followers省略で高速化）"""
        url = "https://x.com/i/api/graphql/iSicc7LrzWGBgDPL0tM_TQ/Following"
        all_users = []
        cursor = None
        for _ in range(10):
            variables = {"userId": user_id, "count": 200, "includePromotedContent": False}
            if cursor:
                variables["cursor"] = cursor
            for attempt in range(3):
                try:
                    resp = self.session.get(url, params={
                        "variables": json.dumps(variables), "features": FOLLOW_FEATURES}, timeout=15)
                    if self._wait_rate_limit(resp):
                        continue
                    break
                except Exception:
                    if attempt < 2:
                        time.sleep(2)
                    else:
                        return all_users
            if resp.status_code != 200:
                break
            instructions = (resp.json().get("data", {}).get("user", {}).get("result", {})
                           .get("timeline", {}).get("timeline", {}).get("instructions", []))
            new_cursor = None
            found = 0
            for inst in instructions:
                for entry in inst.get("entries", []):
                    content = entry.get("content", {})
                    ur = content.get("itemContent", {}).get("user_results", {}).get("result", {})
                    if ur:
                        sn = ur.get("legacy", {}).get("screen_name", "")
                        if sn:
                            all_users.append(sn)
                            found += 1
                    if content.get("cursorType") == "Bottom":
                        new_cursor = content.get("value")
            if not new_cursor or found == 0:
                break
            cursor = new_cursor
        return all_users

    def process_batch(self, usernames):
        """一括処理: ユーザー名→Following取得"""
        results = {}
        for u in usernames:
            uid = self.get_user_id(u)
            if not uid:
                results[u] = []
                continue
            following = self.get_following(uid)
            results[u] = following
        return results


def load_cookies_pw():
    raw = json.load(open(COOKIE_FILE))
    cookies = []
    for c in raw:
        cookie = {"name": c["name"], "value": c["value"],
                  "domain": c.get("domain", ".x.com"), "path": c.get("path", "/")}
        if c.get("secure"): cookie["secure"] = True
        if c.get("httpOnly"): cookie["httpOnly"] = True
        cookies.append(cookie)
    return cookies


async def block_resources(route):
    if route.request.resource_type in BLOCK_TYPES:
        await route.abort()
    else:
        await route.continue_()


async def scrape_profile(context, username):
    page = await context.new_page()
    await page.route("**/*", block_resources)
    try:
        await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded", timeout=12000)
        await page.wait_for_selector(
            '[data-testid="UserName"], [data-testid="error-detail"], [data-testid="empty_state_header_text"]',
            timeout=5000)
        username_el = page.locator('[data-testid="UserName"] span')
        if await username_el.count() == 0:
            return None
        display_name = await username_el.first.text_content() or username
        bio_el = page.locator('[data-testid="UserDescription"]')
        bio = await bio_el.text_content() if await bio_el.count() > 0 else ""
        followers_count = 0
        try:
            for sel in ['a[href$="/verified_followers"]', 'a[href$="/followers"]']:
                fl = page.locator(sel)
                if await fl.count() > 0:
                    text = await fl.first.text_content() or ""
                    m = re.search(r"([\d,.]+)\s*([KkMm])?", text)
                    if m:
                        num = float(m.group(1).replace(",", ""))
                        mult = m.group(2)
                        if mult and mult.upper() == "K": num *= 1000
                        elif mult and mult.upper() == "M": num *= 1000000
                        followers_count = int(num)
                    break
        except Exception:
            pass
        profile_image_url = ""
        try:
            imgs = page.locator('img[src*="profile_images"]')
            if await imgs.count() > 0:
                src = await imgs.first.get_attribute("src") or ""
                if src:
                    profile_image_url = src.replace("_normal.", "_400x400.").replace("_bigger.", "_400x400.")
        except Exception:
            pass
        profile_sokusuu = extract_sokusuu(bio)
        profile_url = f"https://twitter.com/{username}"
        pinned_sokusuu = None
        pinned_url = None
        try:
            articles = page.locator('article[data-testid="tweet"]')
            count = await articles.count()
            for i in range(min(count, 3)):
                article = articles.nth(i)
                sc = article.locator('[data-testid="socialContext"]')
                if await sc.count() > 0:
                    sc_text = (await sc.text_content() or "").lower()
                    if "pinned" in sc_text or "固定" in sc_text:
                        tweet_text_el = article.locator('[data-testid="tweetText"]')
                        if await tweet_text_el.count() > 0:
                            tweet_text = await tweet_text_el.first.text_content() or ""
                            pinned_sokusuu = extract_sokusuu(tweet_text)
                            links = article.locator('a[href*="/status/"]')
                            for j in range(await links.count()):
                                href = await links.nth(j).get_attribute("href") or ""
                                if "/status/" in href:
                                    tid = href.split("/status/")[-1].split("?")[0].split("/")[0]
                                    pinned_url = f"https://twitter.com/{username}/status/{tid}"
                                    break
                        break
        except Exception:
            pass
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
        return SokusuuRecord(username=username, display_name=display_name, sokusuu=sokusuu,
                             source=source, url=url or profile_url, followers_count=followers_count,
                             bio=bio, categories=cats_str, profile_image_url=profile_image_url)
    except Exception:
        return None
    finally:
        await page.close()


def save_results(new_records):
    existing = []
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
    existing_map = {r["username"]: r for r in existing}
    for r in new_records:
        rec = asdict(r)
        if r.username in existing_map:
            old = existing_map[r.username]
            if r.sokusuu >= old.get("sokusuu", 0):
                for k in ["profile_image_url", "categories", "alt_accounts"]:
                    if old.get(k) and not rec.get(k): rec[k] = old[k]
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
    print(f"[SAVE] {OUTPUT_JSON} ({len(records_final)} 件)")
    return len(records_final)


async def main():
    print("=" * 50)
    print("DEPTH 2 高速版 (3Cookie並列 + Followers省略)")
    print("=" * 50)

    # グラフ読み込み
    follow_graph = json.load(open(GRAPH_FILE))
    print(f"[INFO] 既存グラフ: {len(follow_graph)} アカウント分")

    # 未探索のシードを特定
    seeds = []
    if os.path.exists(SEED_FILE):
        with open(SEED_FILE, "r", encoding="utf-8") as f:
            seeds = [l.strip().lstrip("@") for l in f if l.strip() and not l.startswith("#")]

    collected_usernames = set()
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
        collected_usernames = {r["username"] for r in existing}

    # DEPTH 2のシード = 即数持ちアカウントのみ（高スコアは既に収集済み）
    next_seeds = list(collected_usernames - set(follow_graph.keys()) - set(seeds))
    print(f"[INFO] 未探索シード: {len(next_seeds)} アカウント")

    # --- Phase 1: 3Cookie並列でFollowingリスト取得 ---
    cookie_files = [COOKIE_FILE]
    for i in range(1, 4):
        f = f"data/.twitter_cookies_worker{i}.json"
        if os.path.exists(f):
            cookie_files.append(f)
    num_workers = min(len(cookie_files), 3)
    print(f"\n[Phase 1] {num_workers}ワーカー並列でFollowing取得")

    # ワーカーにシードを分割
    chunks = [[] for _ in range(num_workers)]
    for i, seed in enumerate(next_seeds):
        chunks[i % num_workers].append(seed)
    print(f"  分割: {[len(c) for c in chunks]}")

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for i in range(num_workers):
            api = TwitterGraphQL(cookie_files[i], worker_id=i + 1)
            future = executor.submit(api.process_batch, chunks[i])
            futures[future] = i + 1

        for future in as_completed(futures):
            wid = futures[future]
            try:
                results = future.result()
                follow_graph.update(results)
                print(f"  [W{wid}] 完了: {len(results)} アカウント分")
            except Exception as e:
                print(f"  [W{wid}] エラー: {e}")

    elapsed = time.time() - t0
    print(f"[Phase 1 完了] {elapsed:.0f}秒 ({len(next_seeds)}件)")

    # グラフ保存
    with open(GRAPH_FILE, "w", encoding="utf-8") as f:
        json.dump(follow_graph, f, ensure_ascii=False)
    print(f"[SAVE] グラフ保存 ({len(follow_graph)} アカウント分)")

    # --- Phase 2: スコアリング ---
    print(f"\n[Phase 2] スコアリング")
    appearance_count = Counter()
    for seed, members in follow_graph.items():
        for m in members:
            appearance_count[m] += 1

    all_seeds = set(seeds) | set(follow_graph.keys())
    candidates = {u: c for u, c in appearance_count.items() if c >= 10 and u not in all_seeds}

    print(f"  候補: {len(candidates)} アカウント")
    score_dist = Counter(candidates.values())
    for score in sorted(score_dist.keys(), reverse=True)[:10]:
        print(f"    {score}共通={score_dist[score]}件", end=" ")
    print()

    # --- Phase 3: Playwright で収集 ---
    to_collect = [u for u in candidates if u not in collected_usernames]
    to_collect.sort(key=lambda u: candidates.get(u, 0), reverse=True)
    print(f"\n[Phase 3] 即数収集: {len(to_collect)} アカウント")

    pw_cookies = load_cookies_pw()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        await context.add_cookies(pw_cookies)

        records = []
        for i in range(0, len(to_collect), 3):
            batch = to_collect[i:i + 3]
            results = await asyncio.gather(
                *[scrape_profile(context, u) for u in batch],
                return_exceptions=True)
            for r in results:
                if isinstance(r, SokusuuRecord):
                    records.append(r)
            processed = min(i + 3, len(to_collect))
            if processed % 50 < 3 or processed == len(to_collect):
                print(f"[PROGRESS] {processed}/{len(to_collect)} ({len(records)} 件ヒット)")
            if processed % 100 < 3:
                save_results(records)

        await browser.close()

    total = save_results(records)
    print(f"\n[DONE] {len(records)} 件新規ヒット / 合計 {total} 件")


if __name__ == "__main__":
    asyncio.run(main())
