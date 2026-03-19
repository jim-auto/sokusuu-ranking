"""
芋づる式ナンパ垢探索 + 即数収集（Playwright版）

戦略:
1. シードアカウントのフォロワー/フォローリストを取得
2. 複数シードを共通フォローしてる人 = ナンパ系候補（スコアリング）
3. 高スコア候補のプロフィールから即数を収集
4. 高スコア候補のフォロー/フォロワーからさらに芋づる探索
"""

import asyncio
import csv
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, asdict
from typing import Optional

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

# --- 定数 ---

OUTPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_CSV = "data/sokusuu_accounts.csv"
COOKIE_FILE = "data/.twitter_cookies.json"
GRAPH_FILE = "data/follow_graph.json"  # フォローグラフ保存
SEED_FILE = "seed_accounts.txt"

BLOCK_TYPES = {"image", "stylesheet", "font", "media"}

# 共通フォロー数の閾値（これ以上のシードを共通フォローしてたらナンパ系候補）
MIN_SHARED_FOLLOWS = 2

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
    "street": re.compile(r"(スト(?:ナン|リート)?|street|路上|声かけ|声掛け)", re.IGNORECASE),
    "club": re.compile(r"(クラブ|箱|club|ラウンジ|lounge|キャバ)", re.IGNORECASE),
    "online": re.compile(
        r"(ネト|アプリ|tinder|ペアーズ|pairs|タップル|tapple|bumble|マッチング|OLD|with|omiai|ネットナンパ|ネトナン|ネッナン|マチアプ)",
        re.IGNORECASE,
    ),
}

NANPA_KEYWORDS = re.compile(
    r"(pua|nanpa|soku|tinder|即|ナンパ|斬り|mote|renai|恋愛|講師|ゲット|street)",
    re.IGNORECASE,
)


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
    # 絵文字をスペースに置換（数字の連結を防ぐ）
    cleaned = re.sub(r'[\U00010000-\U0010ffff]', ' ', cleaned)
    values = []
    for pattern in SOKUSUU_PATTERNS:
        values.extend(int(m) for m in pattern.findall(cleaned))
    return max(values) if values else None


def detect_categories(bio: str, username: str) -> list[str]:
    text = f"{bio} {username}"
    return [cat for cat, pat in CATEGORY_PATTERNS.items() if pat.search(text)]


def load_cookies() -> list[dict]:
    raw = json.load(open(COOKIE_FILE))
    cookies = []
    for c in raw:
        cookie = {"name": c["name"], "value": c["value"],
                  "domain": c.get("domain", ".x.com"), "path": c.get("path", "/")}
        if c.get("secure"):
            cookie["secure"] = True
        if c.get("httpOnly"):
            cookie["httpOnly"] = True
        cookies.append(cookie)
    return cookies


async def block_resources(route):
    if route.request.resource_type in BLOCK_TYPES:
        await route.abort()
    else:
        await route.continue_()


# --- フォローリスト取得 ---

async def get_follow_list(context, username: str, page_type: str = "following",
                          max_scrolls: int = 20) -> list[str]:
    """フォロー/フォロワーリストを取得"""
    page = await context.new_page()
    await page.route("**/*", block_resources)

    url = f"https://x.com/{username}/{page_type}"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=12000)
    except Exception:
        pass

    # UserCellが出るまで待つ
    try:
        await page.wait_for_selector('[data-testid="UserCell"]', timeout=8000)
    except Exception:
        await page.close()
        return []

    collected = set()
    last_count = 0
    stall = 0

    for _ in range(max_scrolls):
        cells = page.locator('[data-testid="UserCell"] a[role="link"]')
        count = await cells.count()
        for i in range(count):
            try:
                href = await cells.nth(i).get_attribute("href") or ""
                if not href or "/status/" in href or "search?" in href:
                    continue
                uname = href.rstrip("/").split("/")[-1]
                if (uname and 1 <= len(uname) <= 15 and
                    uname.lower() not in {"home", "explore", "notifications", "messages",
                                           "search", "settings", "i", "compose", "intent", "hashtag"}):
                    collected.add(uname)
            except Exception:
                continue

        if len(collected) == last_count:
            stall += 1
            if stall >= 3:
                break
        else:
            stall = 0
        last_count = len(collected)

        await page.evaluate("window.scrollBy(0, 1200)")
        await page.wait_for_timeout(800)

    await page.close()
    return list(collected)


# --- プロフィール収集 ---

async def scrape_profile(context, username: str) -> Optional[SokusuuRecord]:
    """1ユーザーのプロフィール+固定ツイートから即数を収集"""
    page = await context.new_page()
    await page.route("**/*", block_resources)
    try:
        await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded", timeout=12000)
        await page.wait_for_selector(
            '[data-testid="UserName"], [data-testid="error-detail"], [data-testid="empty_state_header_text"]',
            timeout=5000,
        )

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

        # 即数抽出
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
                            lcount = await links.count()
                            for j in range(lcount):
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

        return SokusuuRecord(
            username=username, display_name=display_name, sokusuu=sokusuu,
            source=source, url=url or profile_url, followers_count=followers_count,
            bio=bio, categories=cats_str, profile_image_url=profile_image_url,
        )
    except Exception:
        return None
    finally:
        await page.close()


# --- 保存 ---

def save_results(new_records: list[SokusuuRecord]):
    """既存データとマージして保存"""
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
                    if old.get(k) and not rec.get(k):
                        rec[k] = old[k]
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
    import argparse

    parser = argparse.ArgumentParser(description="芋づる式ナンパ垢探索 + 即数収集")
    parser.add_argument("--tabs", type=int, default=3, help="並列タブ数")
    parser.add_argument("--depth", type=int, default=2, help="芋づる探索の深さ")
    parser.add_argument("--min-shared", type=int, default=MIN_SHARED_FOLLOWS,
                        help="共通フォロー数の閾値")
    args = parser.parse_args()

    print("=" * 50)
    print("芋づる式ナンパ垢探索 + 即数収集")
    print(f"並列タブ: {args.tabs} / 探索深さ: {args.depth} / 共通閾値: {args.min_shared}")
    print("=" * 50)

    # シードアカウント読み込み
    seeds = []
    if os.path.exists(SEED_FILE):
        with open(SEED_FILE, "r", encoding="utf-8") as f:
            seeds = [l.strip().lstrip("@") for l in f if l.strip() and not l.startswith("#")]
    print(f"[INFO] シード: {len(seeds)} アカウント")

    # 既存の収集済み
    collected_usernames = set()
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
        collected_usernames = {r["username"] for r in existing}
        print(f"[INFO] 収集済み: {len(collected_usernames)} アカウント")

    # フォローグラフ読み込み（前回の途中結果を再利用）
    follow_graph: dict[str, list[str]] = {}  # seed -> [followers]
    if os.path.exists(GRAPH_FILE):
        with open(GRAPH_FILE, "r", encoding="utf-8") as f:
            follow_graph = json.load(f)
        print(f"[INFO] 前回のグラフ: {len(follow_graph)} アカウント分")

    cookies = load_cookies()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        await context.add_cookies(cookies)

        all_records: list[SokusuuRecord] = []
        explored_seeds = set(follow_graph.keys())

        for depth in range(args.depth):
            print(f"\n{'='*50}")
            print(f"[DEPTH {depth + 1}/{args.depth}]")
            print(f"{'='*50}")

            # --- Phase 1: フォロワー/フォローリスト取得 ---
            targets_for_graph = [s for s in seeds if s not in explored_seeds]
            print(f"\n[Phase 1] フォローグラフ取得: {len(targets_for_graph)} アカウント")

            for i, seed in enumerate(targets_for_graph):
                print(f"[{i+1}/{len(targets_for_graph)}] @{seed} のフォロー/フォロワーを取得中...")
                t0 = time.time()

                following = await get_follow_list(context, seed, "following")
                followers = await get_follow_list(context, seed, "followers")
                combined = list(set(following) | set(followers))

                follow_graph[seed] = combined
                explored_seeds.add(seed)
                elapsed = time.time() - t0
                print(f"  following={len(following)}, followers={len(followers)}, combined={len(combined)} ({elapsed:.1f}s)")

                # 中間保存
                if (i + 1) % 10 == 0:
                    os.makedirs(os.path.dirname(GRAPH_FILE), exist_ok=True)
                    with open(GRAPH_FILE, "w", encoding="utf-8") as f:
                        json.dump(follow_graph, f, ensure_ascii=False)
                    print(f"  [SAVE] グラフ保存 ({len(follow_graph)} アカウント分)")

            # グラフ保存
            os.makedirs(os.path.dirname(GRAPH_FILE), exist_ok=True)
            with open(GRAPH_FILE, "w", encoding="utf-8") as f:
                json.dump(follow_graph, f, ensure_ascii=False)

            # --- Phase 2: スコアリング ---
            print(f"\n[Phase 2] スコアリング（共通フォロー数 >= {args.min_shared}）")

            # 各アカウントが何人のシードのフォロー/フォロワーに出現するかカウント
            appearance_count = Counter()
            for seed, members in follow_graph.items():
                for member in members:
                    appearance_count[member] += 1

            # シード自身を除外
            seed_set = set(seeds)
            candidates = {
                uname: count
                for uname, count in appearance_count.items()
                if count >= args.min_shared and uname not in seed_set
            }

            # キーワードマッチも追加（1回しか出現しなくてもユーザー名にナンパキーワードがあれば候補）
            for uname, count in appearance_count.items():
                if count == 1 and uname not in seed_set and uname not in candidates:
                    if NANPA_KEYWORDS.search(uname):
                        candidates[uname] = count

            print(f"  候補: {len(candidates)} アカウント")
            print(f"  スコア分布: ", end="")
            score_dist = Counter(candidates.values())
            for score in sorted(score_dist.keys(), reverse=True):
                print(f"{score}共通={score_dist[score]}件 ", end="")
            print()

            # --- Phase 3: 即数収集 ---
            to_collect = [u for u in candidates if u not in collected_usernames]
            print(f"\n[Phase 3] 即数収集: {len(to_collect)} アカウント（未収集分）")

            # スコア高い順にソート
            to_collect.sort(key=lambda u: candidates.get(u, 0), reverse=True)

            batch_records = []
            for i in range(0, len(to_collect), args.tabs):
                batch = to_collect[i:i + args.tabs]
                results = await asyncio.gather(
                    *[scrape_profile(context, u) for u in batch],
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, SokusuuRecord):
                        batch_records.append(result)
                        collected_usernames.add(result.username)

                processed = min(i + args.tabs, len(to_collect))
                if processed % 50 < args.tabs or processed == len(to_collect):
                    elapsed_per = (time.time() - t0) if i == 0 else 0
                    print(f"[PROGRESS] {processed}/{len(to_collect)} ({len(batch_records)} 件ヒット)")

                if processed % 100 < args.tabs:
                    save_results(batch_records + all_records)

            all_records.extend(batch_records)
            print(f"[DEPTH {depth + 1}] 結果: {len(batch_records)} 件ヒット")

            # --- Phase 4: 芋づる（次ホップのシード選定）---
            if depth < args.depth - 1:
                # 即数持ちアカウント + 高スコア候補を次のシードにする
                new_seeds = []
                for r in batch_records:
                    if r.username not in explored_seeds:
                        new_seeds.append(r.username)

                # スコア3以上の候補も次のシードに（即数なくてもナンパ界隈の可能性高い）
                for uname, score in candidates.items():
                    if score >= 3 and uname not in explored_seeds and uname not in new_seeds:
                        new_seeds.append(uname)

                print(f"\n[芋づる] 次ホップのシード: {len(new_seeds)} アカウント")
                seeds = new_seeds

            # 保存
            total = save_results(all_records)

        # シードアカウント自身も収集
        seed_uncollected = [s for s in set(seeds) if s not in collected_usernames]
        if seed_uncollected:
            print(f"\n[シード収集] {len(seed_uncollected)} アカウント")
            for i in range(0, len(seed_uncollected), args.tabs):
                batch = seed_uncollected[i:i + args.tabs]
                results = await asyncio.gather(
                    *[scrape_profile(context, u) for u in batch],
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, SokusuuRecord):
                        all_records.append(result)

        await browser.close()

    # 最終保存
    total = save_results(all_records)
    print(f"\n[DONE] 合計 {total} 件")


if __name__ == "__main__":
    asyncio.run(main())
