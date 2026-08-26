# 2026-07 取りこぼし調査レポート

調査日: 2026-08-02

## 方法
1. H1(1-6月)で5即以上の実績があるが7月に無いアカウントを抽出
2. 6月5+で7月無しを優先リスト化
3. GraphQL UserTweets で直近〜250投稿を走査し `extract_monthly_count(..., 7)` + 「7月」本文有無を確認
4. 人手で誤抽出を除外して backfill

## 補完して掲載したもの（5即以上）

| account | 即数 | 根拠 |
|---------|-----:|------|
| @17go_pua | 17 | https://x.com/17go_pua/status/2083415220517843403 |
| @kimu__himitsu2 | 16 | https://x.com/kimu__himitsu2/status/2083330629845094722 |
| @tsutsumi_ye4pe | 9 | https://x.com/tsutsumi_ye4pe/status/2083479659505910175 |
| @SFgzKAHifDvjfVu | 7 | https://x.com/SFgzKAHifDvjfVu/status/2083438939726352601 |

→ 7月 5+ は **21 → 25件**

## 誤抽出・不採用

| account | 自動抽出 | 判定 |
|---------|---------:|------|
| @taku__pua | 5 | **却下**（本文「7月は0即」「6月は5即」） |
| @makoto__pua | 20 | **却下**（別月の「今月20即」ツイ） |
| @homura_tin | 9 | **却下**（他ユーザー総括のRT/誤帰属） |

## 高優先だが7月総括が見つからなかった（網羅走査済み）

### 複数月5+の常連（特に怪しい）
| account | H1実績(5+) | 7月 |
|---------|------------|-----|
| @ot_aza | 1-6月ほぼ毎月（max14） | 総括なし |
| @River_p823 | 1-5月（max14） | 途中「7月N即目」のみ・最終総括なし |
| @taruchan100 | 3-6（max13） | なし |
| @sugi_ichiban | 1,3,5,6（max10） | なし（BBQ等のみ） |
| @Tinder_god_2 | 2,6（max13） | なし（429多発） |
| @puro_nanpa | 6:15 | なし（429多発） |
| @sub_chilll | 1:21 3:16 | なし |
| @atannon_nampa | 3,4 | なし |
| @training_pua | 3,4:14 | なし |

### 単発ハイだが7月なし
@shime_pua(1:25) @sandorafc(1:24) @oyasugaoo(4:22) @mostkkweek(4:17) など

### 補足
- `sokusuu_accounts.json` に載っていない月次常連が複数（`ot_aza`, `17go_pua`, `SFgzKAHifDvjfVu`, `tsutsumi_ye4pe` 等）。prefetch 対象外になりやすい
- 累計上位（流星/こうや/えむち等）も多くが7月総括なし or 非公開

## 推奨次アクション
1. 未登録常連を seed/accounts に追加してから再収集
2. `@ot_aza` `@River_p823` `@taruchan100` は個別 deep scroll / 検索 `from:user 7月 総括`
3. `extract_monthly_count` の「他月のN即を対象月に誤る」を硬化（taku 事例）


## 2026-08-02 追加対応

### 1. seed / accounts 登録
未登録だった月次常連を追加:

- `ot_aza`, `SFgzKAHifDvjfVu`, `17go_pua`, `chinpan870141`, `tsutsumi_ye4pe`, `qh0kum`, `kimu__himitsu2`
- seed に上記 + `taruchan100` を追記（`River_p823` は seed 既存）
- `data/sokusuu_accounts.json` は gitignore のためローカルのみ（391件）

### 2. 優先3アカウント深掘り（UserTweets 最大8ページ）

| account | 結果 |
|---------|------|
| `@ot_aza` | UID取得可。**TL 0件**（鍵/API非公開の可能性大）。7月総括は取得不可 |
| `@River_p823` | 最終総括なし。途中報告の最大は **7月5即目**（7/26時点）。5即以上の下限は見えるが最終未確定のため未掲載 |
| `@taruchan100` | 7月総括なし。6/30の上半期まとめのみ（誤抽出35は抽出器修正で抑止） |

### 3. extract_monthly_count 修正
- 複数月併記時は `N月はM即` を最優先（`6月は5即…7月は0即` → 7月は None）
- 複数月文では裸の `合計/計` を使わない（年累計58誤爆防止）
- マッチ範囲に他月が挟まるケースを除外
- 対象月なしの上半期/下半期/年間サマリを月次抽出しない


## 明示なし（絵文字リスト）総括の横断調査 (2026-08-02)

対象: H1で5+だが7月未掲載の常連 + ネト/アプリ寄りのアカウント約40件を TL 走査。

### 結論
- **フェイタン型（🍐🍎🔥 1行1即・「N即」なし）は収集済みデータ上ほぼ `@Tinder_god_2` 専用**
  - H1の絵文字リスト総括も 2月/6月とも `@Tinder_god_2` のみ
- 追加の5即以上は **見つからず**（rate limit で一部未達だが、ネト寄り重点20件は完了）
- 誤ヒット例（不採用）:
  - `@homura_tin` … `@rei_app_pua` 総括のRT/同内容
  - `@daigakusei_pua` / `@socool55555` … 他月・目標文の誤抽出

### 抽出器
- `extract_monthly_count` にフェイタン型（対象月総括 + アプリ絵文字行≥5）の行数カウントを追加
