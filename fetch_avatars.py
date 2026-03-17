"""
即数ランキング — プロフィール画像一括取得スクリプト

Twitter syndication API を使ってプロフィール画像URLを取得する。
ブラウザ不要・ログイン不要で高速。プロキシ経由でレート制限を回避。
"""

import json
import os
import re
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

INPUT_JSON = "data/sokusuu_accounts.json"


def get_proxy_url() -> str:
    """環境変数からプロキシURLを構築する"""
    server = os.getenv("PROXY_SERVER", "")
    user = os.getenv("PROXY_USER", "")
    pw = os.getenv("PROXY_PASS", "")
    if server and user:
        from urllib.parse import urlparse
        parsed = urlparse(server)
        return f"http://{user}:{pw}@{parsed.hostname}:{parsed.port}"
    return ""


def fetch_avatar(client: httpx.Client, username: str) -> str:
    """Twitter syndication API からプロフィール画像URLを取得する。429時はリトライ。"""
    url = f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}"
    for attempt in range(3):
        try:
            resp = client.get(url, timeout=10, follow_redirects=True)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"    429 レート制限、{wait}秒待機...")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                return ""
            imgs = re.findall(r"https://pbs\.twimg\.com/profile_images/[^\"\s]+", resp.text)
            if imgs:
                return imgs[0].replace("_normal.", "_400x400.").replace("_bigger.", "_400x400.")
            return ""
        except Exception:
            pass
    return ""


def main():
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        records = json.load(f)

    proxy_url = get_proxy_url()
    if proxy_url:
        print(f"[INFO] プロキシ使用: {proxy_url.split('@')[1]}")
        client = httpx.Client(proxy=proxy_url)
    else:
        print("[INFO] プロキシなし（直接接続）")
        client = httpx.Client()

    # 画像未取得のアカウントだけ対象
    missing = [r for r in records if not r.get("profile_image_url")]
    print(f"[INFO] {len(missing)}/{len(records)} アカウントのアバターを取得します")

    updated = 0
    for i, record in enumerate(missing):
        username = record["username"]
        avatar_url = fetch_avatar(client, username)

        if avatar_url:
            record["profile_image_url"] = avatar_url
            updated += 1
            print(f"  [{i+1}/{len(missing)}] @{username}: OK")
        else:
            print(f"  [{i+1}/{len(missing)}] @{username}: NOT FOUND")

        # レート制限回避のため間隔を空ける
        time.sleep(3)

    client.close()

    with open(INPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    total_with_img = sum(1 for r in records if r.get("profile_image_url"))
    print(f"\n[DONE] 新規{updated}件取得、合計 {total_with_img}/{len(records)} 件")


if __name__ == "__main__":
    main()
