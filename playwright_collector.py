"""
Playwright版 即数収集スクリプト

画像/CSS/フォントをブロックし、3タブ並列でプロフィールを高速取得。
レート制限なし、1件約2.5秒。
"""

import asyncio
import csv
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

# --- 定数 ---

OUTPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_CSV = "data/sokusuu_accounts.csv"
COOKIE_FILE = "data/.twitter_cookies.json"
DISCOVERED_FILE = "data/discovered_accounts.json"
SEED_FILE = "seed_accounts.txt"

BLOCK_TYPES = {"image", "stylesheet", "font", "media"}

PARALLEL_TABS = 3  # 並列タブ数

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
    # 絵文字をスペースに置換（数字の連結を防ぐ）
    cleaned = re.sub(r'[\U00010000-\U0010ffff]', ' ', cleaned)
    values = []
    for pattern in SOKUSUU_PATTERNS:
        matches = pattern.findall(cleaned)
        values.extend(int(m) for m in matches)
    return max(values) if values else None


def detect_categories(bio: str, username: str) -> list[str]:
    text = f"{bio} {username}"
    return [cat for cat, pat in CATEGORY_PATTERNS.items() if pat.search(text)]


def load_cookies() -> list[dict]:
    raw = json.load(open(COOKIE_FILE))
    cookies = []
    for c in raw:
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ".x.com"),
            "path": c.get("path", "/"),
        }
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


async def scrape_profile(context, username: str) -> Optional[SokusuuRecord]:
    """1ユーザーのプロフィール+固定ツイートから即数を収集"""
    page = await context.new_page()
    await page.route("**/*", block_resources)
    try:
        await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded", timeout=12000)

        # UserNameまたはエラーを待つ
        await page.wait_for_selector(
            '[data-testid="UserName"], [data-testid="error-detail"], [data-testid="empty_state_header_text"]',
            timeout=5000,
        )

        username_el = page.locator('[data-testid="UserName"] span')
        if await username_el.count() == 0:
            return None

        display_name = await username_el.first.text_content() or username

        # bio
        bio_el = page.locator('[data-testid="UserDescription"]')
        bio = await bio_el.text_content() if await bio_el.count() > 0 else ""

        # フォロワー数
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
                        if mult and mult.upper() == "K":
                            num *= 1000
                        elif mult and mult.upper() == "M":
                            num *= 1000000
                        followers_count = int(num)
                    break
        except Exception:
            pass

        # プロフィール画像URL
        profile_image_url = ""
        try:
            imgs = page.locator('img[src*="profile_images"]')
            if await imgs.count() > 0:
                src = await imgs.first.get_attribute("src") or ""
                if src:
                    profile_image_url = src.replace("_normal.", "_400x400.").replace("_bigger.", "_400x400.")
        except Exception:
            pass

        # bioから即数抽出
        profile_sokusuu = extract_sokusuu(bio)
        profile_url = f"https://twitter.com/{username}"

        # 固定ツイートから即数抽出
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

                            # ツイートID取得
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
            url=url or profile_url,
            followers_count=followers_count,
            bio=bio,
            categories=cats_str,
            profile_image_url=profile_image_url,
        )

    except Exception:
        return None
    finally:
        await page.close()


def merge_alt_accounts(records: list[SokusuuRecord]) -> list[SokusuuRecord]:
    """サブ垢統合"""
    username_set = {r.username.lower() for r in records}
    mention_map: dict[str, set[str]] = {}

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


def save_merged(new_records: list[SokusuuRecord], existing: list[dict]):
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

    print(f"[SAVE] {OUTPUT_JSON} ({len(records_final)} 件)")


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Playwright版 即数収集")
    parser.add_argument("--tabs", type=int, default=PARALLEL_TABS, help="並列タブ数 (default: 3)")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--no-headless", dest="headless", action="store_false")
    args = parser.parse_args()

    print("=" * 50)
    print("即数収集（Playwright版）")
    print(f"並列タブ: {args.tabs}")
    print("=" * 50)

    # 対象アカウント読み込み
    if os.path.exists(DISCOVERED_FILE):
        with open(DISCOVERED_FILE, "r", encoding="utf-8") as f:
            all_accounts = json.load(f)
        if os.path.exists(SEED_FILE):
            with open(SEED_FILE, "r", encoding="utf-8") as f:
                seeds = [l.strip().lstrip("@") for l in f if l.strip() and not l.startswith("#")]
            all_accounts = sorted(set(all_accounts) | set(seeds))
        print(f"[INFO] 対象: {len(all_accounts)} アカウント")
    else:
        print("[ERROR] discovered_accounts.json がありません")
        return

    # 既に収集済みのアカウントをスキップ
    existing = []
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

    # Playwright起動
    cookies = load_cookies()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        await context.add_cookies(cookies)

        records: list[SokusuuRecord] = []
        total_start = time.time()
        batch_size = args.tabs

        for i in range(0, len(remaining), batch_size):
            batch = remaining[i:i + batch_size]

            # 並列でスクレイピング
            results = await asyncio.gather(
                *[scrape_profile(context, u) for u in batch],
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, SokusuuRecord):
                    records.append(result)

            processed = min(i + batch_size, len(remaining))

            if processed % 50 < batch_size or processed == len(remaining):
                elapsed = time.time() - total_start
                rate = elapsed / processed if processed > 0 else 0
                eta = rate * (len(remaining) - processed)
                print(f"[PROGRESS] {processed}/{len(remaining)} ({len(records)} 件ヒット, "
                      f"{rate:.1f}s/件, ETA {eta/60:.0f}分)")

            # 100件ごとに中間保存
            if processed % 100 < batch_size:
                save_merged(records, existing)

        await browser.close()

    # 最終保存
    if records:
        # サブ垢統合
        all_records_for_merge = records[:]
        for e in existing:
            all_records_for_merge.append(SokusuuRecord(**{
                k: e.get(k, "" if isinstance(SokusuuRecord.__dataclass_fields__[k].default, str) else 0)
                for k in SokusuuRecord.__dataclass_fields__
            }))
        merged = merge_alt_accounts(all_records_for_merge)
        # dictに変換して保存
        final_list = [asdict(r) for r in merged]
        final_list.sort(key=lambda r: r["sokusuu"], reverse=True)

        os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, indent=2)

        fieldnames = ["username", "display_name", "sokusuu", "source", "url",
                      "followers_count", "alt_accounts", "categories", "bio", "profile_image_url"]
        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in final_list:
                writer.writerow({k: r.get(k, "") for k in fieldnames})

        print(f"\n[DONE] {OUTPUT_JSON} ({len(final_list)} 件, サブ垢統合済み)")
    else:
        print("\n[DONE] 新規ヒットなし")

    total = time.time() - total_start
    print(f"[TIME] 合計 {total/60:.1f}分")


if __name__ == "__main__":
    asyncio.run(main())
