# 即数ランキング PLAN

最終更新: 2026-07-15

## 直近の目的

2026年6月の月間即数ランキングを収集し、`docs/index.html` に反映する。

1. `data/monthly_2026_06.json` を新規作成する
2. `data/monthly_ranking.json` を更新する
3. 月別タブ付きで `docs/index.html` を再生成する
4. ドラフト PR で公開前レビューする

## 2026年6月 収集サマリ

実行コマンド:

```bash
python monthly_collect.py --mode monthly --year 2026 --month 6 --global-search --prefetch-only --checkpoint-every 10
```

条件:

- 対象: 384アカウント
- モード: `--global-search --prefetch-only`（個別タイムライン走査なし）
- Cookie: 6個利用（2026-03 時点のものを再利用、スモーク収集で動作確認済み）

結果:

- 出力: `data/monthly_2026_06.json` = 23件
- `data/monthly_ranking.json` 更新あり
- HTML: `SHOW_PERIOD_TABS=1 SHOW_PERIOD_DETAIL_TABS=1 DEFAULT_TAB=monthlyselect DEFAULT_MONTH=202606 python generate_html.py`

| rank | username | count | source |
| ---: | --- | ---: | --- |
| 1 | `tora_maru005` | 51 | `global_search` |
| 2 | `kent_o_o` | 45 | `global_search` |
| 3 | `kukuru_nanpa` | 23 | `global_search` |
| 4 | `okarun_pua` | 15 | `global_search` |
| 5 | `bookmaker_2015` | 13 | `global_search` |
| 6 | `omamco_pua2` | 10 | `global_search` |
| 7 | `ururunpua` | 7 | `global_search` |
| 8 | `daigakusei_pua` | 7 | `global_search` |
| 9 | `chiroru_pua` | 5 | `global_search` |
| 10 | `taku__pua` | 5 | `global_search` |

補足:

- 全件 `match_source=global_search`（tweet evidence あり）
- 低スコア（1–2即）は誤検出リスクがあるので public merge 前に目視推奨
- `data/` は gitignore のため、PR 本体は `docs/index.html`（埋め込み済み）と `PLAN.md` / `requirements.txt`

## 現在の公開状態

- 公開URL: https://jim-auto.github.io/sokusuu-ranking/
- 本ブランチでは period タブ（月別 / 月間記録 など）を HTML に含めて再生成済み
- マージ方針はドラフト PR レビュー後に判断

## 現在の件数

2026-07-15 時点の実測。

- raw データ: `data/sokusuu_accounts.json` = 384件
- public 表示時ユニーク件数: 380件
  - `generate_html.py` の重複統合後
- public 表示時カテゴリ件数:
  - ストリート: 179件
  - クラブ: 50件
  - オンライン: 125件
- local 保持データ:
  - `data/monthly_ranking.json` = 72件
  - `data/yearly_ranking.json` = 53件
  - `data/monthly_2026_02.json` = 34件
  - `data/monthly_2026_03.json` = 18件
  - `data/monthly_2026_06.json` = 23件
  - `data/yearly_2025.json` = 35件

## 今回変更したコード

### `monthly_collect.py`

Playwright のブラウザ文脈を少し実ブラウザ寄りにした。

追加したもの:

- `PLAYWRIGHT_USER_AGENT`
  - 以前は短い `Mozilla/5.0 ... AppleWebKit/537.36` だけだった
  - 今回は Chrome / Safari まで含む通常の desktop UA にした
- `PLAYWRIGHT_STEALTH_SCRIPT`
  - `navigator.webdriver`
  - `navigator.languages`
  - `navigator.plugins`
  - `navigator.platform`
  - `window.chrome.runtime`
  - これらを軽く偽装する init script
- `create_playwright_context(...)`
  - context 作成処理を関数化
  - `locale="ja-JP"`
  - `timezone_id="Asia/Tokyo"`
  - `accept-language`
  - `--disable-blink-features=AutomationControlled`
  - `--lang=ja-JP`
- `--headful`
  - headless の SearchTimeline が弱い時に、表示あり Playwright で試すためのオプション

注意:

- これは「完全な stealth」ではない
- `playwright-stealth` のような外部 stealth ライブラリは導入していない
- fingerprint 全体を整えるものではなく、最低限の headless / webdriver 検出対策
- X 側の防御が強い場合は、headful + 実ユーザープロファイル方式のほうが効く可能性がある

### `requirements.txt`

- `playwright>=1.40` を追加

理由:

- `monthly_collect.py` はすでに Playwright を import している
- しかし `requirements.txt` には Playwright がなかった
- 新環境で再現すると import 失敗するため、依存関係に明記した

## 今回実行したコマンド

### 構文確認

```bash
python -m py_compile monthly_collect.py
```

結果:

- 成功

### 2026年3月 スモーク収集

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --limit 20 --global-search --prefetch-only --skip-ranking-update
```

結果:

- 対象: 20アカウント
- Cookie: 6個利用可能
- 個別タイムライン走査: スキップ
- SearchTimeline 事前ヒット: 2件
- 出力: `data/monthly_2026_03.json`
- この時点では `monthly_ranking.json` は更新しない設定

ヒット:

- `@tora_maru005`: 39即
- `@cx_lm5`: 8即

### 2026年3月 全件 prefetch-only 収集

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --global-search --prefetch-only --checkpoint-every 10
```

結果:

- 対象: 384アカウント
- 個別タイムライン走査: スキップ
- GraphQL UserTweets 直叩きもスキップ
- 実行ログ取得側は timeout したが、Python プロセスは継続して最終的に完了
- チェックポイントファイルは残っていない
- 出力: `data/monthly_2026_03.json` = 17件
- `data/monthly_ranking.json` も更新され、79件から87件になった

## 2026年3月 月間収集結果

`data/monthly_2026_03.json` は17件。

現時点の順位:

| rank | username | count | source | evidence |
| ---: | --- | ---: | --- | --- |
| 1 | `tora_maru005` | 39 | `global_search` | `https://x.com/tora_maru005/status/2039520788169318887` |
| 2 | `PUAINOKI` | 15 | `global_search` | `https://x.com/PUAINOKI/status/2043633776484655526` |
| 3 | `taka_DTnmp` | 10 | `global_search` | `https://x.com/taka_DTnmp/status/2038864156254941485` |
| 4 | `cx_lm5` | 8 | `global_search` | `https://x.com/cx_lm5/status/2038993251160780895` |
| 5 | `atannon_nampa` | 7 | `global_search` | `https://x.com/atannon_nampa/status/2039188199369781496` |
| 6 | `ao_nampa` | 7 | `global_search` | `https://x.com/ao_nampa/status/2039361725007618336` |
| 7 | `Nano486273` | 7 | `global_search` | `https://x.com/Nano486273/status/2038992271606169767` |
| 8 | `entpxxxxxx` | 7 | `global_search` | `https://x.com/entpxxxxxx/status/2038921485180699084` |
| 9 | `chiroru_pua` | 6 | `global_search` | `https://x.com/chiroru_pua/status/2038998601322692608` |
| 10 | `1jEvvc` | 6 | `profile_bio` | `https://x.com/1jEvvc` |
| 11 | `tsuyumushi777` | 4 | `global_search` | `https://x.com/tsuyumushi777/status/2039316017919455295` |
| 12 | `KgNoYou1` | 4 | `global_search` | `https://x.com/KgNoYou1/status/2039010460213842179` |
| 13 | `pua_yossy` | 3 | `global_search` | `https://x.com/pua_yossy/status/2038981792137679122` |
| 14 | `ak166121` | 3 | `global_search` | `https://x.com/ak166121/status/2038979835142472040` |
| 15 | `ManaBuHirokawa7` | 2 | `profile_bio` | `https://x.com/ManaBuHirokawa7` |
| 16 | `RobertPowerJ` | 1 | `profile_bio` | `https://x.com/RobertPowerJ` |
| 17 | `badasai_kush` | 1 | `global_search` | `https://x.com/badasai_kush/status/2042119741519884599` |

確認ポイント:

- `global_search` は SearchTimeline から拾ったもの
- `profile_bio` は `data/sokusuu_accounts.json` に保存済みの bio / display_name / location から拾ったもの
- `profile_bio` はツイート根拠が薄いので、public に出す前に人間確認したほうがよい
- 1即 / 2即のような低スコアは誤検出リスクが相対的に高い

## 収集方式の現状

### 月間 / 年間収集のメイン

`monthly_collect.py` が主役。

実際の流れ:

1. `data/sokusuu_accounts.json` を母集団にする
2. Cookie JSON を読み込む
3. GraphQL API 直叩き用の `requests.Session` を複数作る
4. Playwright context を作る
5. `--global-search` があれば SearchTimeline を先に広く拾う
6. `--prefetch-only` がなければ、各ユーザーの UserTweets を API / browser で個別探索する
7. 取れなければ SearchTimeline fallback
8. それでも取れなければ profile 系テキストから period count を抽出する
9. `data/monthly_YYYY_MM.json` または `data/yearly_YYYY.json` を出す
10. `--skip-ranking-update` がなければ `monthly_ranking.json` / `yearly_ranking.json` を更新する

### 今回使ったモード

今回は負荷を抑えるため、全件はこのモードで止めた。

```bash
--global-search --prefetch-only
```

このモードでやること:

- 広域 SearchTimeline
- ユーザー複数人をまとめた batch SearchTimeline
- 保存済み profile テキストからの抽出

このモードでやらないこと:

- 各アカウントの UserTweets API を個別に掘る
- 各アカウントのページを Playwright で個別スクロールする
- したがって Cookie / レート制限の消費は比較的軽い

## Playwright / stealth 対策の評価

### 変更前

変更前も Playwright は使っていた。

ただし使い方はかなり素朴だった。

- `headless=True`
- 短い user agent
- locale / timezone 指定なし
- `navigator.webdriver` 対策なし
- `window.chrome` 対策なし
- persistent context ではない
- 実ユーザープロファイルではない
- stealth 専用ライブラリなし

Playwright の主用途:

- 検索ページを開く
- `SearchTimeline` の network response を捕まえる
- ユーザーページを開く
- `UserTweets` の network response を捕まえる

つまり、DOM をがっつり読むというより、ブラウザに API を発火させてレスポンスを横取りする方式。

### 変更後

最低限の bot 判定対策は入れた。

- full UA
- locale / timezone
- accept-language
- `--disable-blink-features=AutomationControlled`
- `navigator.webdriver` 偽装
- `navigator.languages` 偽装
- `navigator.plugins` 偽装
- `navigator.platform` 偽装
- `window.chrome.runtime` 補完
- `--headful` 追加

ただし、これは強い stealth ではない。

足りないもの:

- persistent Chrome profile
- WebGL / canvas / audio fingerprint 調整
- permissions / notification 周りの整合性
- `sec-ch-ua` と実 Chromium version の厳密一致
- mouse movement / dwell time の人間らしさ
- `playwright-stealth` などの体系的 stealth
- proxy / IP reputation 管理

結論:

- Playwright は使っている
- stealth 対策は今回最低限追加した
- ただし X の強い防御を突破する設計ではない
- 今の月間収集の主力は、Playwright DOM scraping ではなく `requests + Cookie + GraphQL` と SearchTimeline 捕捉

## 従来スクレイピング方式の問題点

### Selenium / undetected-chromedriver 方式

メリット:

- ログインや Cookie 取得にはまだ有効
- 人間ブラウザに近いため Cloudflare 初期突破には強い
- total ranking のプロフィール収集には使いやすい

問題:

- 1件あたり重い
- DOM セレクタ変更に弱い
- 多数アカウントを回すと時間がかかる
- headless / automation 判定の影響を受ける
- X の UI 変更で壊れやすい
- フォロワー探索などはスクロール量が多く、失敗時の再開が面倒

この repo での位置づけ:

- `scraper.py`
  - total ranking 収集のメイン
  - `undetected-chromedriver + Selenium`
- `get_cookies.py`
  - Cookie 取得補助
  - ここでも `undetected-chromedriver`

### 素の HTTP scraping / OSS ライブラリ方式

現状ではかなり厳しい。

問題:

- Cloudflare / bot 判定で 403 になりやすい
- X の内部 GraphQL は doc_id / operation id が変わる
- `x-client-transaction` 系の要求が壊れると OSS ライブラリが止まる
- guest token / anonymous path は不安定
- twint / snscrape 系の古い公開エンドポイント依存はほぼ死んでいる
- twscrape / twikit 系も X 側変更に追随が必要

この repo では、`memory/project_scraper_landscape.md` に2026年3月時点の状況メモがある。

### GraphQL API 直叩き方式

現在の主力。

メリット:

- Selenium より速い
- DOM 変更に比較的強い
- Cookie と ct0 があればプロフィール / timeline を取れる
- 複数 Cookie で rate limit を分散できる

問題:

- 429 に当たる
- operation id が変わると壊れる
- Cookie が死ぬと止まる
- API レスポンス構造が変わると parser が壊れる
- X 側の内部仕様に依存している

今回の `monthly_collect.py` では6 Cookie を認識していた。

```text
APIフォールバック: 6 Cookie 利用可
```

ただし全件では `--prefetch-only` を使ったため、UserTweets API の個別走査は実行していない。

## 今回わかったこと

### SearchTimeline はまだ使える

2026年3月分で `global_search` が14件拾えている。

17件中:

- `global_search`: 14件
- `profile_bio`: 3件

つまり3月月報は、個別 timeline を掘らなくても SearchTimeline だけで一定数拾える。

### ただし件数はまだ薄い

3月分は17件。

2月分の34件より少ない。

理由として考えられるもの:

- 3月月報を明示している人が少ない
- SearchTimeline の取得漏れ
- query が保守的
- `--prefetch-only` のため個別 timeline 深掘りをしていない
- 3月月報が4月中旬以降に投稿された場合、現在の reporting window から漏れる

現在の monthly window:

- start: `2026-03-20`
- end: `2026-04-15`
- Search の until は `2026-04-16`

`monthly_collect.py` の `build_reporting_window()` で決まっている。

### 全件 global prefetch は時間がかかる

全件 `--global-search --prefetch-only` はログ取得側の timeout を超えた。

ただし Python プロセス自体は完走した。

原因:

- global search 2本
- batch search が 384 / 15 = 26 batch
- 各 search query が最大5-6 scroll
- X の search response 待ちで時間が伸びる
- prefetch phase はチェックポイント保存前なので、途中状況が見えにくい

改善案:

- `search_target_batches()` 内で batch ごとに進捗を print する
- prefetch phase の途中結果も state file に保存する
- `--batch-size` / `--batch-scrolls` を CLI 化する
- SearchTimeline が詰まったら query を短くして再試行する

## 次にやるなら

### 1. まず3月17件を目視確認

特に `profile_bio` の3件。

- `1jEvvc`: 6即
- `ManaBuHirokawa7`: 2即
- `RobertPowerJ`: 1即

確認観点:

- 2026年3月の数字か
- 月間即数か
- total / 累計 / 年間 / 目標ではないか
- 他人の実績紹介ではないか
- profile 内の月別 series が正しく読めているか

### 2. 3月分の個別深掘りを必要な範囲だけ実行

全384件をいきなり個別 timeline 走査すると重い。

候補:

- top total 100件だけ
- 2月月間ランキング掲載者だけ
- フォロワー数上位だけ
- 既に period 実績がある人だけ
- Search で `3月` / `月間` / `今月` が見えている人だけ

現行CLIは offset がないので、subset を作るなら次のどちらか。

1. 一時的に `data/sokusuu_accounts.json` を触らず、別スクリプトで `OUTPUT_JSON` 相当の小さい入力を作る
2. `monthly_collect.py` に `--start` / `--offset` / `--usernames-file` を足す

おすすめは `--usernames-file`。

理由:

- 対象者リストを明示できる
- retry がしやすい
- data 本体を触らなくてよい

### 3. full individual collection をやる場合

実行候補:

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --global-search --search-fallback --checkpoint-every 10
```

ただし注意:

- X/Twitter に多数アクセスする
- GraphQL rate limit を消費する
- 6 Cookie あっても 429 はあり得る
- 途中で長時間止まったように見える可能性がある
- 先に prefetch phase の進捗保存を改善したほうがよい

軽く確認するなら:

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --limit 50 --global-search --search-fallback --skip-ranking-update
```

### 4. SearchTimeline だけ再試行する場合

今ある3月結果を壊さず、また広域 Search を試す。

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --global-search --prefetch-only --checkpoint-every 10
```

ただし `monthly_ranking.json` を更新したくない場合:

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --global-search --prefetch-only --checkpoint-every 10 --skip-ranking-update
```

### 5. headful で試す場合

headless の SearchTimeline が弱いと感じる場合:

```bash
python monthly_collect.py --mode monthly --year 2026 --month 3 --global-search --prefetch-only --headful --skip-ranking-update
```

注意:

- ブラウザ表示あり
- 実行中にウィンドウが開く
- 自動化判定には少し強くなる可能性がある
- ただし速度は落ちる

## public に戻す判断

まだ戻さないほうがよい。

理由:

- 3月月間は17件で薄い
- 2月月間34件もまだ薄い
- `profile_bio` 根拠の低スコアが混じっている
- 月別 / 年別タブを戻すには、継続的な精査フローがまだ弱い

戻すなら最低条件:

- 月間ランキングが複数月で30-50件程度ある
- 上位20件程度は evidence URL がある
- profile-derived は注記または除外方針を決める
- 誤検出しやすい低スコア帯の扱いを決める
- `generate_html.py` 側の period 表示を再確認する

## public 表示で統合している重複

`generate_html.py` の `DUPLICATE_ACCOUNT_CANONICALS` で統合。

- `emuchi_pua -> puro_nanpa`
- `sub_chilll -> pua_chilll`
- `gureran_m3 -> gureran_m`
- `inpsub -> ryepua`

補足:

- raw データから削除しているわけではない
- public 表示時だけ canonical 側に寄せている
- `alt_accounts` に duplicate username を足す

## 今回触っていないもの

- `docs/index.html`
- `generate_html.py`
- `data/sokusuu_accounts.json`
- `data/yearly_2025.json`
- `data/yearly_ranking.json`
- Cookie ファイル
- `.env`
- `accounts.db`

今回生成 / 更新された可能性があるもの:

- `data/monthly_2026_03.json`
- `data/monthly_ranking.json`

ただし `data/` は generated / semi-generated として扱う。

## Git status 注意

作業前から untracked / local WIP が多い。

今回 Codex が明示的に触った tracked file:

- `monthly_collect.py`
- `requirements.txt`
- `PLAN.md`

今回生成・更新された data:

- `data/monthly_2026_03.json`
- `data/monthly_ranking.json`

作業前から存在していた untracked 例:

- `AGENTS.md`
- `bulk_check.py`
- `check_locations.py`
- `club_discovery.py`
- `get_cookies.py`
- `memory/`
- `probe_period_search.py`
- `scan_uncollected_period_profiles.py`
- `test_playwright*.py`
- `test_twikit*.py`
- `test_twscrape.py`
- debug png

方針:

- unrelated WIP は revert しない
- Cookie / `.env` / local state は commit しない
- data を commit するかは用途次第

## Copilot への引き継ぎメモ

最初に見るファイル:

1. `monthly_collect.py`
2. `PLAN.md`
3. `data/monthly_2026_03.json`
4. `data/monthly_ranking.json`
5. `memory/project_scraper_landscape.md`

おすすめの次タスク:

1. `monthly_collect.py` に `--usernames-file` を追加
2. prefetch phase の進捗表示と checkpoint 保存を追加
3. `--batch-size` / `--batch-scrolls` を CLI 化
4. `profile_bio` source の結果を別扱いできるようにする
5. `data/monthly_2026_03.json` の17件を目視精査する
6. 必要なら3月分の限定 individual timeline 収集を行う

実装時の注意:

- X/Twitter にアクセスするコマンドは、実行前に明示する
- full collector は安易に回さない
- まず `python -m py_compile monthly_collect.py`
- 小さい `--limit` で smoke
- その後に必要範囲だけ本実行

## 直近の結論

- 2026年3月月間ランキングは再収集できた
- local data としては17件
- SearchTimeline はまだ有効
- Playwright は使っているが、stealth は今回最低限を追加した段階
- 従来の Selenium DOM scraping だけで period を厚くするのは重い
- 今後は `SearchTimeline + GraphQL API + 限定個別深掘り + 手動精査` の組み合わせが現実的
- public へ月別 / 年別を戻すのはまだ早い
