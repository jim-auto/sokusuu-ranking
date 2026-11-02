"""
クッキー無しで月別即数候補を探す軽量スクリプト

Google / DuckDuckGo の site:x.com 検索 + fxtwitter で個別ツイート取得。
完全な収集は難しいが、クラウド（GitHub Actions）でシークレット無しで動きます。

使用例:
  python public_monthly_collect.py --year 2026 --month 10
"""

import argparse
import json
import os
import re
import time
from datetime import datetime
from urllib.parse import quote_plus

import requests

OUTPUT_DIR = "data"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}


def search_google_site_x(query: str, max_results: int = 20):
    """Googleで site:x.com 検索してリンクを抽出（簡易スクレイピング）"""
    url = f"https://www.google.com/search?q={quote_plus(query)}&num={max_results}&hl=ja"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        # 非常に簡易的なリンク抽出（実際はもっと正確なパーサが望ましい）
        links = re.findall(r'https://x\.com/[a-zA-Z0-9_]+/status/\d+', resp.text)
        return list(dict.fromkeys(links))[:max_results]
    except Exception as e:
        print(f"Google search failed: {e}")
        return []


def fetch_tweet_via_fxtwitter(status_url: str):
    """fxtwitter 経由でツイート情報を取得（クッキー不要）"""
    # https://x.com/user/status/123 → https://api.fxtwitter.com/user/status/123
    match = re.search(r'x\.com/([^/]+)/status/(\d+)', status_url)
    if not match:
        return None
    user, status_id = match.groups()
    api_url = f"https://api.fxtwitter.com/{user}/status/{status_id}"
    try:
        resp = requests.get(api_url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def extract_sokusuu_candidates(text: str):
    """テキストから「即」関連の数値を抽出試みる"""
    if not text:
        return []
    # 例: 「10即」「月間30即」「即数 25」など
    patterns = [
        r'(\d{1,3})\s*即',
        r'即\s*(\d{1,3})',
        r'月間\s*(\d{1,3})',
        r'(\d{1,3})\s*人',
    ]
    found = []
    for pat in patterns:
        found.extend(re.findall(pat, text))
    return list(set(found))


def collect_for_month(year: int, month: int):
    queries = [
        f'site:x.com 即 月間 OR 報告 OR まとめ {year}年{month}月',
        f'site:x.com "即数" OR "月間即" {year}-{month:02d}',
        f'site:x.com ナンパ 即 {month}月',
    ]
    candidates = []
    seen_urls = set()

    for q in queries:
        print(f"Searching: {q}")
        links = search_google_site_x(q, max_results=15)
        for link in links:
            if link in seen_urls:
                continue
            seen_urls.add(link)
            tweet_data = fetch_tweet_via_fxtwitter(link)
            if not tweet_data:
                continue
            tweet = tweet_data.get("tweet") or tweet_data
            text = tweet.get("text") or ""
            author = tweet.get("author") or {}
            numbers = extract_sokusuu_candidates(text)
            if numbers or "即" in text:
                candidates.append({
                    "username": author.get("screen_name") or author.get("username"),
                    "display_name": author.get("name"),
                    "text": text[:300],
                    "url": link,
                    "possible_counts": numbers,
                    "created_at": tweet.get("created_at") or tweet.get("date"),
                    "source": "public_search",
                    "needs_review": True,
                })
            time.sleep(1.5)  # 优しいペース

    return candidates


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    results = collect_for_month(args.year, args.month)

    out_path = os.path.join(OUTPUT_DIR, f"public_monthly_{args.year}_{args.month:02d}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "collected_at": datetime.now().isoformat(),
            "year": args.year,
            "month": args.month,
            "method": "cookie-free public search (Google + fxtwitter)",
            "count": len(results),
            "results": results,
        }, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(results)} candidates to {out_path}")
    print("注意: これは候補の抽出です。精査・整理が必要です。")


if __name__ == "__main__":
    main()
