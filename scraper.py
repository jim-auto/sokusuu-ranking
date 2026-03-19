"""
即数（そくすう）収集スクリプト

X（Twitter）のプロフィールおよび固定ツイートから即数を抽出し、
ランキング用データセットを生成する。

使用ライブラリ: undetected-chromedriver + selenium
"""

import csv
import json
import os
import random
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv

load_dotenv()


# --- 定数 ---

SEED_FILE = "seed_accounts.txt"
OUTPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_CSV = "data/sokusuu_accounts.csv"
COOKIE_FILE = "data/.twitter_cookies.json"

# 即数を検出する正規表現パターン（優先度順）
# 長いパターンを先にマッチさせることで誤検出を防ぐ
SOKUSUU_PATTERNS = [
    re.compile(r"通算\s*即\s*(\d+)"),         # 通算即120, 通算 即 120
    re.compile(r"即数\s*(\d+)"),               # 即数120, 即数 120
    re.compile(r"経験人数\s*(\d+)"),           # 経験人数1400
    re.compile(r"体験人数\s*(\d+)"),           # 体験人数50
    re.compile(r"(\d+)\s*人斬り"),              # 700人斬り
    re.compile(r"人斬り\s*(\d+)"),             # 人斬り200
    re.compile(r"斬り数\s*(\d+)"),             # 斬り数150
    re.compile(r"(\d+)\s*斬り"),               # 80斬り
    re.compile(r"斬り\s*(\d+)"),               # 斬り80
    re.compile(r"total\s*(\d+)\s*即", re.IGNORECASE),  # total250即
    re.compile(r"(\d+)\s*即"),                 # 250即
    re.compile(r"(\d+)\s*get", re.IGNORECASE), # 167get, 50get
    re.compile(r"ゲット数\s*(\d+)"),           # ゲット数100
    re.compile(r"GET\s*(\d+)", re.IGNORECASE), # GET100, get 50
    re.compile(r"GN\s*(\d+)", re.IGNORECASE),  # GN数50 (GetNanpa)
    re.compile(r"S数\s*(\d+)"),                # S数30
    re.compile(r"即\s*(\d+)"),                 # 即120, 即 120（汎用・最後）
]

# フォロワー/フォロー探索の設定
DISCOVERED_FILE = "data/discovered_accounts.json"
MAX_FOLLOW_SCROLL = 30  # フォロー/フォロワーページのスクロール回数上限

# カテゴリ判定キーワード（bio + ユーザー名から判定）
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


# --- データクラス ---

@dataclass
class SokusuuRecord:
    """即数レコード"""
    username: str
    display_name: str
    sokusuu: int
    source: str          # "profile" or "pinned_tweet"
    url: str
    followers_count: int = 0
    bio: str = ""              # プロフィール文（同一人物検出用）
    alt_accounts: str = ""     # 統合されたサブ垢（カンマ区切り）
    categories: str = ""       # カテゴリ（カンマ区切り: street, club, online）
    profile_image_url: str = ""  # プロフィール画像URL


# --- ユーティリティ関数 ---

def load_seed_accounts(filepath: str) -> list[str]:
    """seed_accounts.txt からユーザー名一覧を読み込む"""
    if not os.path.exists(filepath):
        print(f"[ERROR] {filepath} が見つかりません")
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        accounts = []
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                accounts.append(line.lstrip("@"))

    print(f"[INFO] {len(accounts)} アカウントを読み込みました")
    return accounts


def extract_sokusuu(text: str) -> Optional[int]:
    """
    テキストから即数を抽出する。
    複数マッチした場合は最大値を返す。
    """
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

    if not values:
        return None

    return max(values)


def detect_categories(bio: str, username: str) -> list[str]:
    """bio とユーザー名からカテゴリを判定する"""
    text = f"{bio} {username}"
    cats = []
    for cat_name, pattern in CATEGORY_PATTERNS.items():
        if pattern.search(text):
            cats.append(cat_name)
    return cats


def random_sleep(min_sec: float = 1.0, max_sec: float = 3.0):
    """人間らしいランダムな待機"""
    time.sleep(random.uniform(min_sec, max_sec))


# --- ブラウザ管理クラス ---

class TwitterBrowser:
    """undetected-chromedriver によるブラウザ管理"""

    def __init__(self, headless: bool = False, proxy_url: str = None):
        self.headless = headless
        self.proxy_url = proxy_url
        self.driver = None
        self._proxy_ext_dir = None
        self._login_avatar_url = ""  # ログインユーザーのアバターURL（除外用）

    def _create_proxy_extension(self) -> str:
        """認証付きプロキシ用のChrome Extension を動的生成する"""
        import tempfile
        from urllib.parse import urlparse

        parsed = urlparse(self.proxy_url)
        host = parsed.hostname
        port = parsed.port or 80
        username = parsed.username or os.getenv("PROXY_USER", "")
        password = parsed.password or os.getenv("PROXY_PASS", "")

        ext_dir = tempfile.mkdtemp(prefix="proxy_ext_")

        manifest = """{
  "version": "1.0.0",
  "manifest_version": 2,
  "name": "Proxy Auth",
  "permissions": ["proxy", "tabs", "unlimitedStorage", "storage",
                   "<all_urls>", "webRequest", "webRequestBlocking"],
  "background": {"scripts": ["background.js"]},
  "minimum_chrome_version": "22.0.0"
}"""

        background = """var config = {
  mode: "fixed_servers",
  rules: {
    singleProxy: { scheme: "http", host: "%s", port: %d },
    bypassList: ["localhost"]
  }
};
chrome.proxy.settings.set({value: config, scope: "regular"}, function(){});
function callbackFn(details) {
  return { authCredentials: { username: "%s", password: "%s" } };
}
chrome.webRequest.onAuthRequired.addListener(
  callbackFn, {urls: ["<all_urls>"]}, ['blocking']
);""" % (host, port, username, password)

        with open(os.path.join(ext_dir, "manifest.json"), "w") as f:
            f.write(manifest)
        with open(os.path.join(ext_dir, "background.js"), "w") as f:
            f.write(background)

        return ext_dir

    def start(self):
        """ブラウザを起動する"""
        options = uc.ChromeOptions()
        options.page_load_strategy = "eager"
        options.add_argument("--lang=ja-JP")
        options.add_argument("--window-size=1280,800")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        if self.proxy_url:
            self._proxy_ext_dir = self._create_proxy_extension()
            options.add_argument(f"--load-extension={self._proxy_ext_dir}")

        if self.headless:
            options.add_argument("--headless=new")

        self.driver = uc.Chrome(options=options, use_subprocess=True, version_main=145)
        self.driver.implicitly_wait(3)
        self.driver.set_page_load_timeout(30 if self.proxy_url else 15)
        proxy_info = f" (proxy: {self.proxy_url})" if self.proxy_url else ""
        print(f"[INFO] ブラウザ起動完了{proxy_info}")

    def quit(self):
        """ブラウザを終了する"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            print("[INFO] ブラウザ終了")

    def load_cookies(self):
        """保存済みCookieを読み込む"""
        cookie_path = Path(COOKIE_FILE)
        if not cookie_path.exists():
            # influencer_tweet_collector のCookieをフォールバックとして使う
            fallback = Path(r"C:\Users\ryohe\workspace\influencer_tweet_collector\data\.twitter_cookies.json")
            if fallback.exists():
                cookie_path = fallback
                print("[INFO] influencer_tweet_collector のCookieを流用します")
            else:
                print("[WARN] 保存済みCookieが見つかりません")
                return False

        try:
            # まずTwitterにアクセスしてドメインを設定
            try:
                self.driver.get("https://twitter.com")
            except TimeoutException:
                self.driver.execute_script("window.stop();")
            random_sleep(3, 5)

            with open(cookie_path, "r") as f:
                cookies = json.load(f)

            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception:
                    pass

            print(f"[INFO] Cookie読み込み完了 ({len(cookies)} 件)")
            return True
        except Exception as e:
            print(f"[ERROR] Cookie読み込み失敗: {e}")
            return False

    def save_cookies(self):
        """Cookieを保存する"""
        try:
            Path(COOKIE_FILE).parent.mkdir(parents=True, exist_ok=True)
            cookies = self.driver.get_cookies()
            with open(COOKIE_FILE, "w") as f:
                json.dump(cookies, f, indent=2)
            print(f"[INFO] Cookie保存完了 ({len(cookies)} 件)")
        except Exception as e:
            print(f"[ERROR] Cookie保存失敗: {e}")

    def login(self):
        """Xにログインする（Cookieがない場合）"""
        username = os.getenv("TWITTER_USERNAME")
        password = os.getenv("TWITTER_PASSWORD")

        if not username or not password:
            print("[ERROR] 環境変数 TWITTER_USERNAME / TWITTER_PASSWORD が未設定です")
            print("        .env ファイルを作成してください")
            return False

        try:
            print("[INFO] ログイン開始...")
            self.driver.get("https://twitter.com")
            random_sleep(2, 4)

            self.driver.get("https://twitter.com/i/flow/login")
            random_sleep(3, 5)

            # ユーザー名入力
            selectors = [
                'input[autocomplete="username"]',
                'input[name="text"]',
                'input[type="text"]',
            ]
            username_input = None
            for sel in selectors:
                try:
                    username_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                    break
                except TimeoutException:
                    continue

            if not username_input:
                print("[ERROR] ユーザー名入力欄が見つかりません")
                return False

            # 人間らしくキー入力
            for char in username:
                username_input.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            random_sleep(0.5, 1.0)
            username_input.send_keys(Keys.RETURN)
            random_sleep(2, 4)

            # パスワード入力
            try:
                password_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="password"]'))
                )
            except TimeoutException:
                print("[ERROR] パスワード入力欄が見つかりません")
                return False

            for char in password:
                password_input.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            random_sleep(0.5, 1.0)
            password_input.send_keys(Keys.RETURN)
            random_sleep(3, 5)

            # ログイン確認
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="primaryColumn"]'))
                )
                print("[INFO] ログイン成功")
                self._capture_login_avatar()
                self.save_cookies()
                return True
            except TimeoutException:
                print("[ERROR] ログイン失敗（タイムライン表示を確認できません）")
                return False

        except Exception as e:
            print(f"[ERROR] ログイン中にエラー: {e}")
            return False

    def _capture_login_avatar(self):
        """ログインユーザーのアバターURLを記録する（プロフィール画像の除外用）"""
        try:
            source = self.driver.page_source
            urls = re.findall(r'https://pbs\.twimg\.com/profile_images/[^"\s\)&]+', source)
            if urls:
                # ホーム画面にはログインユーザーのアバターだけがある
                clean = urls[0].split("&quot;")[0].split('"')[0]
                # IDの部分を抽出（/profile_images/1234567890/xxx_normal.jpg → 1234567890）
                parts = clean.split("/profile_images/")
                if len(parts) > 1:
                    avatar_id = parts[1].split("/")[0]
                    self._login_avatar_url = avatar_id
                    print(f"[INFO] ログインユーザーのアバターID: {avatar_id}")
        except Exception:
            pass

    def ensure_logged_in(self) -> bool:
        """ログイン状態を確認し、必要に応じてログインする"""
        if self.load_cookies():
            # Cookieリロードしてログイン状態を確認
            try:
                self.driver.get("https://x.com/home")
            except TimeoutException:
                try:
                    self.driver.execute_script("window.stop();")
                except Exception:
                    pass
            random_sleep(4, 6)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="primaryColumn"]'))
                )
                print("[INFO] ログイン済み（Cookie有効）")
                self._capture_login_avatar()
                self.save_cookies()
                return True
            except TimeoutException:
                print("[WARN] Cookieが無効です。再ログインします...")

        return self.login()

    def _extract_text(self, element) -> str:
        """要素からテキストを抽出し、img絵文字をalt属性で復元する"""
        js = """
        function extractText(el) {
            var result = '';
            for (var i = 0; i < el.childNodes.length; i++) {
                var node = el.childNodes[i];
                if (node.nodeType === 3) {
                    result += node.textContent;
                } else if (node.tagName === 'IMG') {
                    result += node.alt || '';
                } else if (node.tagName === 'BR') {
                    result += '\\n';
                } else {
                    result += extractText(node);
                }
            }
            return result;
        }
        return extractText(arguments[0]);
        """
        try:
            return self.driver.execute_script(js, element)
        except Exception:
            return element.text

    def get_profile_info(self, username: str) -> Optional[dict]:
        """
        ユーザーのプロフィール情報（表示名・bio）を取得する。
        """
        try:
            self.driver.get(f"https://twitter.com/{username}")
        except TimeoutException:
            try:
                self.driver.execute_script("window.stop();")
            except Exception:
                pass

        # ページ遷移後にURLが正しいか確認（前のページが残ってないか）
        time.sleep(1)
        current_url = self.driver.current_url or ""
        if username.lower() not in current_url.lower():
            # URLが違う = ページ遷移が完了していない、少し待つ
            time.sleep(3)
            current_url = self.driver.current_url or ""
            if username.lower() not in current_url.lower():
                print(f"  [WARN] @{username}: ページ遷移失敗 (URL: {current_url})")
                return None

        try:
            # 表示名を取得（プロキシ経由は遅いので長めに待つ）
            wait_sec = 10 if self.proxy_url else 5
            display_name = username
            try:
                name_el = WebDriverWait(self.driver, wait_sec).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR,
                        '[data-testid="UserName"] span'
                    ))
                )
                display_name = self._extract_text(name_el)
            except TimeoutException:
                # UserNameが見つからない = アカウントが存在しない/凍結/鍵垢
                return None

            # bioを取得
            bio = ""
            try:
                bio_el = self.driver.find_element(
                    By.CSS_SELECTOR, '[data-testid="UserDescription"]'
                )
                bio = self._extract_text(bio_el)
            except NoSuchElementException:
                pass

            # フォロワー数を取得
            followers_count = 0
            try:
                # "N Followers" リンクからフォロワー数を取得
                follow_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href$="/verified_followers"]')
                if not follow_links:
                    follow_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href$="/followers"]')
                if follow_links:
                    text = follow_links[0].text  # e.g. "20.1K Followers" or "2,583 フォロワー"
                    # K(千), M(百万) 対応
                    m = re.search(r"([\d,.]+)\s*([KkMm])?", text)
                    if m:
                        num_str = m.group(1).replace(",", "")
                        multiplier = m.group(2)
                        num = float(num_str)
                        if multiplier and multiplier.upper() == "K":
                            num *= 1000
                        elif multiplier and multiplier.upper() == "M":
                            num *= 1000000
                        followers_count = int(num)
            except Exception:
                pass

            # プロフィール画像URL取得
            # スクロールして画像のlazy loadを発火させ、ページソースから取得
            profile_image_url = ""
            try:
                self.driver.execute_script("window.scrollTo(0, 500)")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, 0)")
                time.sleep(2)

                source = self.driver.page_source
                urls = re.findall(
                    r'https://pbs\.twimg\.com/profile_images/[^"\s\)&]+',
                    source,
                )
                seen = set()
                for u in urls:
                    clean = u.split("&quot;")[0].split('"')[0]
                    if clean in seen:
                        continue
                    seen.add(clean)
                    # ログインユーザーのアバターを除外
                    if self._login_avatar_url and self._login_avatar_url in clean:
                        continue
                    profile_image_url = clean.replace("_normal.", "_400x400.").replace("_bigger.", "_400x400.")
                    break
            except Exception:
                pass

            return {
                "display_name": display_name,
                "bio": bio,
                "followers_count": followers_count,
                "profile_image_url": profile_image_url,
            }

        except Exception as e:
            print(f"  [ERROR] @{username}: プロフィール取得失敗 - {e}")
            return None

    def get_pinned_tweet(self, username: str) -> Optional[dict]:
        """
        現在表示中のプロフィールページから固定ツイートを取得する。
        get_profile_info の直後に呼ぶこと（同じページにいる前提）。
        """
        try:
            # implicitly_wait を短くしてfind_elementsの待機を減らす
            self.driver.implicitly_wait(1)
            articles = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')
            self.driver.implicitly_wait(3)  # 元に戻す

            for article in articles:
                try:
                    # 固定ツイートかどうか判定
                    social_context = article.find_element(
                        By.CSS_SELECTOR, '[data-testid="socialContext"]'
                    )
                    context_text = social_context.text.lower()
                    if "pinned" in context_text or "固定" in context_text:
                        # ツイート本文を取得
                        tweet_text_el = article.find_element(
                            By.CSS_SELECTOR, '[data-testid="tweetText"]'
                        )
                        tweet_text = self._extract_text(tweet_text_el)

                        # ツイートURLからIDを取得
                        tweet_links = article.find_elements(
                            By.CSS_SELECTOR, 'a[href*="/status/"]'
                        )
                        tweet_id = None
                        for link in tweet_links:
                            href = link.get_attribute("href")
                            if "/status/" in href:
                                tweet_id = href.split("/status/")[-1].split("?")[0].split("/")[0]
                                break

                        return {
                            "text": tweet_text,
                            "tweet_id": tweet_id or "unknown",
                        }
                except NoSuchElementException:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] @{username}: 固定ツイート取得失敗 - {e}")
            return None

    def get_following_list(self, username: str, max_scrolls: int = MAX_FOLLOW_SCROLL) -> list[str]:
        """ユーザーのフォロー一覧からユーザー名を収集する"""
        return self._scrape_follow_page(username, "following", max_scrolls)

    def get_followers_list(self, username: str, max_scrolls: int = MAX_FOLLOW_SCROLL) -> list[str]:
        """ユーザーのフォロワー一覧からユーザー名を収集する"""
        return self._scrape_follow_page(username, "followers", max_scrolls)

    @staticmethod
    def _is_valid_username(uname: str) -> bool:
        """有効なTwitterユーザー名かどうか判定する"""
        if not uname:
            return False
        # ユーザー名は英数字とアンダースコアのみ、1〜15文字
        if not re.match(r"^[A-Za-z0-9_]{1,15}$", uname):
            return False
        # 明らかに非ユーザーのパスを除外
        invalid = {"home", "explore", "notifications", "messages", "search",
                   "settings", "i", "compose", "intent", "hashtag"}
        if uname.lower() in invalid:
            return False
        return True

    def _scrape_follow_page(self, username: str, page_type: str, max_scrolls: int) -> list[str]:
        """フォロー/フォロワーページをスクロールしてユーザー名を収集する"""
        # verified_followers ページを優先（フォロワーの場合）
        if page_type == "followers":
            url = f"https://twitter.com/{username}/verified_followers"
        else:
            url = f"https://twitter.com/{username}/{page_type}"
        try:
            self.driver.get(url)
        except TimeoutException:
            try:
                self.driver.execute_script("window.stop();")
            except Exception:
                pass
        random_sleep(1, 2)

        # ページが読み込まれるまで待機
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="primaryColumn"]'))
            )
        except TimeoutException:
            print(f"  [WARN] @{username}/{page_type}: ページ読み込みタイムアウト")
            return []

        collected = set()
        last_count = 0
        stall_count = 0

        for scroll_i in range(max_scrolls):
            # UserCell からユーザー名リンクを抽出
            cells = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="UserCell"]')
            for cell in cells:
                try:
                    # UserCell内の最初のリンクがユーザーへのリンク
                    links = cell.find_elements(By.CSS_SELECTOR, 'a[role="link"]')
                    for link in links:
                        href = link.get_attribute("href") or ""
                        # https://x.com/username or https://twitter.com/username
                        if not href:
                            continue
                        if "/status/" in href or "search?" in href or "hashtag" in href:
                            continue
                        parts = href.rstrip("/").split("/")
                        uname = parts[-1]
                        if self._is_valid_username(uname) and uname != username:
                            collected.add(uname)
                except Exception:
                    continue

            # 新規が見つからなくなったら終了
            if len(collected) == last_count:
                stall_count += 1
                if stall_count >= 3:
                    break
            else:
                stall_count = 0
            last_count = len(collected)

            # スクロール
            self.driver.execute_script("window.scrollBy(0, 1200);")
            random_sleep(1.0, 2.0)

        print(f"  [INFO] @{username}/{page_type}: {len(collected)} アカウント発見")
        return list(collected)


# --- 収集ロジック ---

def collect_sokusuu_for_user(browser: TwitterBrowser, username: str) -> Optional[SokusuuRecord]:
    """
    1ユーザーについて、プロフィールと固定ツイートから即数を収集する。
    両方に即数がある場合はより大きい値を採用する。
    """
    print(f"[INFO] @{username} を処理中...")

    # プロフィール取得（ページ遷移）
    profile = browser.get_profile_info(username)
    if profile is None:
        return None

    display_name = profile["display_name"]

    # プロフィールから即数を抽出
    profile_sokusuu = extract_sokusuu(profile["bio"])
    profile_url = f"https://twitter.com/{username}"

    # 固定ツイートから即数を抽出（同じページ上）
    pinned = browser.get_pinned_tweet(username)
    pinned_sokusuu = None
    pinned_url = None
    if pinned:
        pinned_sokusuu = extract_sokusuu(pinned["text"])
        pinned_url = f"https://twitter.com/{username}/status/{pinned['tweet_id']}"

    # 両方に即数がある場合はより大きい値を採用
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
        print(f"  [SKIP] @{username}: 即数が見つかりませんでした")
        return None

    # 即数が10未満のアカウントは誤検出の可能性が高いため除外
    if sokusuu < 10:
        print(f"  [SKIP] @{username}: 即数が{sokusuu}（10未満のため除外）")
        return None

    followers_count = profile.get("followers_count", 0)
    bio = profile.get("bio", "")
    cats = detect_categories(bio, username)
    cats_str = ", ".join(cats) if cats else ""
    profile_image_url = profile.get("profile_image_url", "")
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


# --- 出力関数 ---

def save_json(records: list[SokusuuRecord], filepath: str) -> None:
    """結果を JSON ファイルに保存する"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    data = [asdict(r) for r in records]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[OUTPUT] {filepath} ({len(records)} 件)")


def save_csv(records: list[SokusuuRecord], filepath: str) -> None:
    """結果を CSV ファイルに保存する"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fieldnames = ["username", "display_name", "sokusuu", "source", "url", "followers_count", "alt_accounts", "categories", "bio", "profile_image_url"]
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(asdict(r))
    print(f"[OUTPUT] {filepath} ({len(records)} 件)")


def deduplicate(records: list[SokusuuRecord]) -> list[SokusuuRecord]:
    """同一ユーザーの重複を除去し、即数が大きい方を残す"""
    best: dict[str, SokusuuRecord] = {}
    for r in records:
        if r.username not in best or r.sokusuu > best[r.username].sokusuu:
            best[r.username] = r
    return list(best.values())


def merge_alt_accounts(records: list[SokusuuRecord]) -> list[SokusuuRecord]:
    """
    プロフィールに@メンションがあるアカウント同士を同一人物として統合する。
    即数が大きい方をメインとし、もう一方をサブ垢として記録する。
    フォロワー数も大きい方を採用する。
    """
    # 全レコードのユーザー名セット
    usernames = {r.username.lower() for r in records}

    # bio内の@メンションを抽出し、データセット内に存在するアカウントとの関連を検出
    # key=username(lower), value=record のマップ
    record_map: dict[str, SokusuuRecord] = {r.username.lower(): r for r in records}

    # 統合ペアを検出
    merged_into: dict[str, str] = {}  # サブ垢 → メイン垢
    for r in records:
        # bioから@メンションを抽出
        mentions = re.findall(r"@([A-Za-z0-9_]{1,15})", r.bio)
        for mention in mentions:
            mention_lower = mention.lower()
            if mention_lower in usernames and mention_lower != r.username.lower():
                # 両方データセット内にいる → 同一人物候補
                other = record_map[mention_lower]
                # 既に統合済みならスキップ
                if r.username.lower() in merged_into or mention_lower in merged_into:
                    continue
                # 即数が大きい方をメインにする
                if r.sokusuu >= other.sokusuu:
                    main, sub = r, other
                else:
                    main, sub = other, r
                merged_into[sub.username.lower()] = main.username.lower()
                # メインにサブ垢情報を追加
                existing_alts = main.alt_accounts.split(", ") if main.alt_accounts else []
                existing_alts.append(f"@{sub.username}")
                main.alt_accounts = ", ".join(existing_alts)
                # フォロワー数は大きい方を採用
                main.followers_count = max(main.followers_count, sub.followers_count)
                print(f"[MERGE] @{sub.username} → @{main.username} に統合（同一人物）")

    # 統合されたサブ垢を除外
    result = [r for r in records if r.username.lower() not in merged_into]
    return result


# --- ネットワーク探索 ---

# ナンパ界隈っぽいユーザー名のキーワード（2ホップ探索の対象判定用）
NANPA_KEYWORDS = re.compile(
    r"(pua|nanpa|soku|tinder|即|ナンパ|斬り|mote|renai|恋愛|講師|ゲット|street)",
    re.IGNORECASE,
)


def discover_accounts(browser: TwitterBrowser, seed_accounts: list[str], depth: int = 2) -> list[str]:
    """
    シードアカウントのフォロー/フォロワーからアカウントを発見する。
    depth=2 で2ホップ探索（シード → 発見アカウント → さらにそのフォロー/フォロワー）。
    発見済みアカウントは data/discovered_accounts.json に保存する。
    """
    discovered = set()
    explored = set()  # 既にフォロー/フォロワーを探索済みのアカウント

    # 前回の発見済みを読み込む
    if os.path.exists(DISCOVERED_FILE):
        with open(DISCOVERED_FILE, "r", encoding="utf-8") as f:
            prev = json.load(f)
            discovered.update(prev)
            print(f"[INFO] 前回発見済み: {len(discovered)} アカウント")

    current_targets = list(seed_accounts)

    for hop in range(depth):
        print(f"\n[INFO] === ホップ {hop + 1}/{depth} ({len(current_targets)} アカウント探索) ===")
        next_targets = []

        for username in current_targets:
            if username in explored:
                continue
            explored.add(username)

            print(f"[INFO] @{username} のネットワークを探索中...")

            # フォロー一覧
            following = browser.get_following_list(username)
            new_following = set(following) - discovered
            discovered.update(following)
            random_sleep(1, 2)

            # フォロワー一覧
            followers = browser.get_followers_list(username)
            new_followers = set(followers) - discovered
            discovered.update(followers)
            random_sleep(1, 2)

            # 新規発見のうちナンパ系っぽいアカウントを次ホップの対象にする
            for uname in new_following | new_followers:
                if NANPA_KEYWORDS.search(uname):
                    next_targets.append(uname)

        current_targets = next_targets
        if not current_targets:
            print(f"[INFO] ホップ {hop + 1}: 新規探索対象なし、探索終了")
            break

    # シードアカウント自身も含める
    discovered.update(seed_accounts)

    # 保存
    os.makedirs(os.path.dirname(DISCOVERED_FILE), exist_ok=True)
    with open(DISCOVERED_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(discovered), f, ensure_ascii=False, indent=2)

    print(f"\n[INFO] 合計 {len(discovered)} アカウントを発見")
    return sorted(discovered)


# --- ワーカー（並列収集用） ---

def get_worker_credentials() -> list[dict]:
    """環境変数から全ワーカーのログイン情報を取得する"""
    workers = []
    # メインアカウント
    u1 = os.getenv("TWITTER_USERNAME")
    p1 = os.getenv("TWITTER_PASSWORD")
    if u1 and p1:
        workers.append({"username": u1, "password": p1, "id": 1})
    # Worker 2〜6
    for i in range(2, 7):
        u = os.getenv(f"TWITTER_USERNAME_{i}")
        p = os.getenv(f"TWITTER_PASSWORD_{i}")
        if u and p:
            workers.append({"username": u, "password": p, "id": i})
    return workers


def worker_collect(worker_id: int, credentials: dict, accounts: list[str],
                   headless: bool, proxy_url: str = None) -> list[SokusuuRecord]:
    """ワーカー1つ分の即数収集処理"""
    print(f"[Worker-{worker_id}] 起動: {len(accounts)} アカウント担当")

    # Worker-1は直接接続、Worker-2以降はプロキシ経由
    use_proxy = proxy_url  # 全ワーカーでプロキシ使用
    browser = TwitterBrowser(headless=headless, proxy_url=use_proxy)
    browser.start()

    # ワーカー固有のCookieファイルを使う
    worker_cookie = f"data/.twitter_cookies_worker{worker_id}.json"

    records = []
    try:
        # Cookie候補パス（優先順）— 全ワーカーでメインCookieも試す
        # 各ワーカーは自分専用のCookieのみ使う（同じCookieの共有はセッション競合を起こす）
        cookie_candidates = [
            Path(worker_cookie),
            Path(r"C:\Users\ryohe\workspace\influencer_tweet_collector\data"
                 f"/.twitter_cookies_worker{worker_id}.json"),
        ]
        # Worker-1だけメインCookieをフォールバック
        if worker_id == 1:
            cookie_candidates.append(Path(COOKIE_FILE))
            cookie_candidates.append(
                Path(r"C:\Users\ryohe\workspace\influencer_tweet_collector\data\.twitter_cookies.json")
            )

        logged_in = False
        for cp in cookie_candidates:
            if not cp.exists():
                continue
            try:
                try:
                    browser.driver.get("https://twitter.com")
                except TimeoutException:
                    try:
                        browser.driver.execute_script("window.stop();")
                    except Exception:
                        pass
                random_sleep(3, 5)

                with open(cp, "r") as f:
                    cookies = json.load(f)
                for c in cookies:
                    try:
                        browser.driver.add_cookie(c)
                    except Exception:
                        pass

                try:
                    browser.driver.get("https://x.com/home")
                except TimeoutException:
                    try:
                        browser.driver.execute_script("window.stop();")
                    except Exception:
                        pass
                random_sleep(4, 6)

                try:
                    WebDriverWait(browser.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="primaryColumn"]'))
                    )
                    print(f"[Worker-{worker_id}] ログイン済み（Cookie: {cp.name}）")
                    browser._capture_login_avatar()
                    logged_in = True
                    break
                except TimeoutException:
                    print(f"[Worker-{worker_id}] Cookie無効: {cp.name}")
                    continue
            except Exception as e:
                print(f"[Worker-{worker_id}] Cookie読み込みエラー: {e}")
                continue

        if not logged_in:
            # 直接ログイン（環境変数を使わず直接credentials使用）
            print(f"[Worker-{worker_id}] Cookieなし、@{credentials['username']} でログイン中...")
            try:
                browser.driver.get("https://twitter.com")
                random_sleep(2, 4)
                browser.driver.get("https://twitter.com/i/flow/login")
                random_sleep(3, 5)

                # ユーザー名入力
                for sel in ['input[autocomplete="username"]', 'input[name="text"]']:
                    try:
                        username_input = WebDriverWait(browser.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                        )
                        break
                    except TimeoutException:
                        username_input = None
                if not username_input:
                    print(f"[Worker-{worker_id}] ログイン失敗: ユーザー名欄なし")
                    return records

                for char in credentials["username"]:
                    username_input.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.15))
                username_input.send_keys(Keys.RETURN)
                random_sleep(2, 4)

                # パスワード入力
                try:
                    pw_input = WebDriverWait(browser.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="password"]'))
                    )
                except TimeoutException:
                    print(f"[Worker-{worker_id}] ログイン失敗: パスワード欄なし")
                    return records

                for char in credentials["password"]:
                    pw_input.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.15))
                pw_input.send_keys(Keys.RETURN)
                random_sleep(3, 5)

                try:
                    WebDriverWait(browser.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="primaryColumn"]'))
                    )
                    print(f"[Worker-{worker_id}] ログイン成功")
                    browser._capture_login_avatar()
                    logged_in = True
                except TimeoutException:
                    print(f"[Worker-{worker_id}] ログイン失敗、スキップ")
                    return records
            except Exception as e:
                print(f"[Worker-{worker_id}] ログインエラー: {e}")
                return records

        # Cookie保存
        try:
            Path(worker_cookie).parent.mkdir(parents=True, exist_ok=True)
            with open(worker_cookie, "w") as f:
                json.dump(browser.driver.get_cookies(), f, indent=2)
        except Exception:
            pass

        # 即数収集
        consecutive_errors = 0
        for i, username in enumerate(accounts):
            try:
                record = collect_sokusuu_for_user(browser, username)
                if record:
                    records.append(record)
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                print(f"[Worker-{worker_id}] @{username} エラー: {type(e).__name__}")
                if consecutive_errors >= 5:
                    print(f"[Worker-{worker_id}] 連続エラー{consecutive_errors}回、停止")
                    break
                # ブラウザが死んでいたら復帰を試みる
                try:
                    browser.driver.current_url
                except Exception:
                    print(f"[Worker-{worker_id}] ブラウザセッション消失、復帰中...")
                    try:
                        browser.quit()
                    except Exception:
                        pass
                    browser = TwitterBrowser(headless=headless, proxy_url=use_proxy)
                    browser.start()
                    # Cookie再読み込み
                    try:
                        browser.driver.get("https://twitter.com")
                        random_sleep(3, 5)
                        if Path(worker_cookie).exists():
                            with open(worker_cookie, "r") as f:
                                for c in json.load(f):
                                    try:
                                        browser.driver.add_cookie(c)
                                    except Exception:
                                        pass
                        browser.driver.get("https://x.com/home")
                        random_sleep(4, 6)
                        print(f"[Worker-{worker_id}] ブラウザ復帰成功")
                    except Exception:
                        print(f"[Worker-{worker_id}] ブラウザ復帰失敗、停止")
                        break
            if i < len(accounts) - 1:
                random_sleep(1, 2)
            if (i + 1) % 30 == 0:
                print(f"[Worker-{worker_id}] {i + 1}/{len(accounts)} 処理済み ({len(records)} 件ヒット)")

    except Exception as e:
        print(f"[Worker-{worker_id}] 致命的エラー: {e}")
    finally:
        browser.quit()

    print(f"[Worker-{worker_id}] 完了: {len(records)} 件ヒット")
    return records


# --- メイン処理 ---

def main():
    import argparse
    from concurrent.futures import ThreadPoolExecutor, as_completed

    parser = argparse.ArgumentParser(description="即数収集スクリプト")
    parser.add_argument("--no-discover", action="store_true",
                        help="ネットワーク探索をスキップし、seed_accounts.txt のみを対象にする")
    parser.add_argument("--headless", action="store_true",
                        help="ヘッドレスモードで実行する")
    parser.add_argument("--workers", type=int, default=0,
                        help="並列ワーカー数（0=自動検出、1=シングル）")
    args = parser.parse_args()

    print("=" * 50)
    print("即数収集スクリプト")
    print("=" * 50)

    # 1. シードアカウントを読み込む
    seed_accounts = load_seed_accounts(SEED_FILE)
    if not seed_accounts:
        print("[ERROR] 処理対象のアカウントがありません。終了します。")
        return

    # 2. ワーカー数を決定
    worker_creds = get_worker_credentials()
    num_workers = args.workers if args.workers > 0 else len(worker_creds)
    num_workers = min(num_workers, len(worker_creds))
    print(f"[INFO] {num_workers} ワーカーで並列実行します")

    # プロキシURL構築（全処理で使用）
    proxy_server = os.getenv("PROXY_SERVER", "")
    proxy_user = os.getenv("PROXY_USER", "")
    proxy_pass = os.getenv("PROXY_PASS", "")
    proxy_url = None
    if proxy_server and proxy_user:
        from urllib.parse import urlparse
        parsed = urlparse(proxy_server)
        proxy_url = f"http://{proxy_user}:{proxy_pass}@{parsed.hostname}:{parsed.port}"
        print(f"[INFO] プロキシ設定: {parsed.hostname}:{parsed.port}")

    # 3. ネットワーク探索（シングルブラウザ）
    browser = TwitterBrowser(headless=args.headless, proxy_url=proxy_url)
    browser.start()

    try:
        if not browser.ensure_logged_in():
            print("[ERROR] ログインできませんでした。終了します。")
            return

        if args.no_discover:
            # 前回の発見済みアカウントがあればそれを使う
            if os.path.exists(DISCOVERED_FILE):
                with open(DISCOVERED_FILE, "r", encoding="utf-8") as f:
                    all_accounts = json.load(f)
                # シードも含める
                all_accounts = sorted(set(all_accounts) | set(seed_accounts))
                print(f"[INFO] 前回の発見済み {len(all_accounts)} アカウントを使用")
            else:
                all_accounts = seed_accounts
        else:
            all_accounts = discover_accounts(browser, seed_accounts)
    finally:
        browser.quit()

    print(f"\n[INFO] {len(all_accounts)} アカウントから即数を収集します\n")

    # 4. アカウントリストをワーカーに分割
    if num_workers <= 1:
        # シングルモード
        browser = TwitterBrowser(headless=args.headless, proxy_url=proxy_url)
        browser.start()
        try:
            if not browser.ensure_logged_in():
                print("[ERROR] ログインできませんでした。")
                return
            records: list[SokusuuRecord] = []
            for i, username in enumerate(all_accounts):
                record = collect_sokusuu_for_user(browser, username)
                if record:
                    records.append(record)
                if i < len(all_accounts) - 1:
                    random_sleep(1, 2)
                if (i + 1) % 50 == 0:
                    print(f"[PROGRESS] {i + 1}/{len(all_accounts)} 処理済み ({len(records)} 件ヒット)")
        finally:
            browser.quit()
    else:
        # マルチワーカーモード: アカウントを均等に分割
        chunks = [[] for _ in range(num_workers)]
        for i, account in enumerate(all_accounts):
            chunks[i % num_workers].append(account)

        print(f"[INFO] アカウント分割: {[len(c) for c in chunks]}")

        records: list[SokusuuRecord] = []
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {}
            for i in range(num_workers):
                cred = worker_creds[i]
                future = executor.submit(
                    worker_collect, cred["id"], cred, chunks[i], args.headless, proxy_url
                )
                futures[future] = cred["id"]
                # ChromeDriver のファイル競合を防ぐため、起動を時間差にする
                if i < num_workers - 1:
                    time.sleep(5)

            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    worker_records = future.result()
                    records.extend(worker_records)
                    print(f"[INFO] Worker-{worker_id} の結果を統合: {len(worker_records)} 件")
                except Exception as e:
                    print(f"[ERROR] Worker-{worker_id} でエラー: {e}")

    if not records:
        print("[WARN] 即数が見つかったアカウントがありません。")
        return

    # 5. 重複除去
    records = deduplicate(records)

    # 5.5. 同一人物のサブ垢を統合
    records = merge_alt_accounts(records)

    # 6. 既存データとマージ（上書きせず新規追加・更新のみ）
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
        existing_map = {r["username"]: r for r in existing}
        # 新規データで更新（既存を保持しつつ新規追加）
        for r in records:
            rec = asdict(r)
            if r.username in existing_map:
                old = existing_map[r.username]
                # 即数が大きい方を採用、画像URLは既存を保持
                if r.sokusuu >= old.get("sokusuu", 0):
                    if old.get("profile_image_url") and not rec.get("profile_image_url"):
                        rec["profile_image_url"] = old["profile_image_url"]
                    if old.get("categories") and not rec.get("categories"):
                        rec["categories"] = old["categories"]
                    if old.get("alt_accounts") and not rec.get("alt_accounts"):
                        rec["alt_accounts"] = old["alt_accounts"]
                    existing_map[r.username] = rec
                # 即数が小さい場合は既存を保持
            else:
                existing_map[r.username] = rec
        records_final = sorted(existing_map.values(), key=lambda r: r["sokusuu"], reverse=True)
        # SokusuuRecord に戻す必要はない、dictのまま保存
        os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(records_final, f, ensure_ascii=False, indent=2)
        print(f"[OUTPUT] {OUTPUT_JSON} ({len(records_final)} 件, マージ済み)")
        # CSV
        fieldnames = ["username", "display_name", "sokusuu", "source", "url",
                      "followers_count", "alt_accounts", "categories", "bio", "profile_image_url"]
        import csv as csv_mod
        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv_mod.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records_final:
                writer.writerow({k: r.get(k, "") for k in fieldnames})
        print(f"[OUTPUT] {OUTPUT_CSV} ({len(records_final)} 件)")
        records = [SokusuuRecord(**{k: r.get(k, "") for k in SokusuuRecord.__dataclass_fields__}) for r in records_final]
    else:
        # 初回実行
        records.sort(key=lambda r: r.sokusuu, reverse=True)
        save_json(records, OUTPUT_JSON)
        save_csv(records, OUTPUT_CSV)

    # 8. ランキング表示
    print()
    print("=" * 50)
    print(f"即数ランキング（{len(records)} 名）")
    print("=" * 50)
    for i, r in enumerate(records, 1):
        alt = f" (= {r.alt_accounts})" if r.alt_accounts else ""
        print(f"  {i}. @{r.username} - 即{r.sokusuu} ({r.source}){alt}")

    # 9. フォロワー数ランキング
    followers_ranked = sorted(records, key=lambda r: r.followers_count, reverse=True)
    print()
    print("=" * 50)
    print(f"フォロワー数ランキング（{len(followers_ranked)} 名）")
    print("=" * 50)
    for i, r in enumerate(followers_ranked, 1):
        print(f"  {i}. @{r.username} - {r.followers_count:,} followers (即{r.sokusuu})")

    print()
    print("[DONE] 完了しました")


if __name__ == "__main__":
    main()
