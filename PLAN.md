# 即数ランキング PLAN

最終更新: 2026-11-03

## 現在の公開状態

- 公開URL: https://jim-auto.github.io/sokusuu-ranking/
- public は総合DBのみ公開中
- public タブ: `総合 / ストリート / クラブ / オンライン / フォロワー数`
- `月別 / 年別 / 月間記録 / 年間記録` は public から一時退避済み

## 2026年10月月別ランキング（ドラフト）

- ターゲット: `data/monthly_2026_10.json` または `data/public_monthly_2026_10.json`
- **クッキー無し（推奨・クラウド向け）**: `python public_monthly_collect.py --year 2026 --month 10`
  - Google `site:x.com` 検索 + fxtwitter で候補を抽出
  - 精度は低いがシークレット不要
- **認証あり（完全版）**: `python monthly_collect.py --mode monthly --year 2026 --month 10`
  - ローカルの cookies または GitHub Secret `TWITTER_COOKIES_JSON` が必要
- **クラウド実行**: Actions タブの「Collect Monthly Ranking」ワークフローで mode=`public` を選ぶとクッキー無しで動きます

## 主なスクリプト

- `monthly_collect.py` — 認証ありの本体収集
- `public_monthly_collect.py` — クッキー無しの軽量候補収集（新規）

次のステップは、Actionsで `public` モードを実行して候補を見てから精査するのが安全です。
