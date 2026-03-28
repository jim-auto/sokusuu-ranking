# 即数ランキング PLAN

最終更新: 2026-03-29

## 現在の公開状態

- 公開URL: https://jim-auto.github.io/sokusuu-ranking/
- public は総合DBのみ公開中
- public タブ: `総合 / ストリート / クラブ / オンライン / フォロワー数`
- `月別 / 年別 / 月間記録 / 年間記録` は public から一時退避済み
- 一時退避の注意書きも削除済み

## 現在の件数

- raw データ: `data/sokusuu_accounts.json` = 386件
- public 表示件数: 382件
- public 内訳:
  - ストリート 181件
  - クラブ 51件
  - オンライン 125件
- local 保持データ:
  - `data/monthly_ranking.json` = 79件
  - `data/yearly_ranking.json` = 53件
  - `data/monthly_2026_02.json` = 34件
  - `data/yearly_2025.json` = 35件

## 今回までに public へ反映済みの内容

- `f506cfc`: public から月別・年別タブを一時退避
- `3d8363a`: public 表示で重複アカウントを統合
- `a95d182`: 「月別・年別ランキングは精査中のため一時退避しています。」の注意書きを削除

## public 表示で統合している重複

- `emuchi_pua -> puro_nanpa`
- `sub_chilll -> pua_chilll`
- `gureran_m3 -> gureran_m`
- `inpsub -> ryepua`

補足:

- raw データは 386件のまま保持
- 重複統合は `generate_html.py` の表示時ロジックで実施
- public 上のユニーク件数は 382件

## 月別・年別を public から外している理由

- `2025年年別` は 35件で、public で前面に出すにはまだ薄い
- `2026年2月月別` は 34件で、public で前面に出すにはまだ薄い
- 今の難しさは収集器の弱さより、根拠データ自体の薄さにある

### 主な詰まり

- 本人が年報・月報を明示していないケースが多い
- 文面が曖昧で、緩く拾うと誤検出・厳しく拾うと取りこぼしになる
- X の Search / GraphQL が不安定で、`429 Too Many Requests` に当たる
- profile は total には強いが、period 抽出には弱い

## 再開するときの優先順位

1. `total` の重複を data レイヤーでも整理する
2. `total` 母集団をさらに増やす
3. `2025年年別` の中位層を手動寄りに精査する
4. `2026年2月月別` の 5即超え候補を個別精査する
5. period データが十分に厚くなったら public に戻す

## 主なローカル補助スクリプト

- `monthly_collect.py`
  - 月別・年別探索の主スクリプト
- `probe_period_search.py`
  - 期間検索の補助
- `scan_uncollected_period_profiles.py`
  - 未回収プロフィール走査
- `get_cookies.py`
  - Cookie 取得補助
- `club_discovery.py`
  - クラブ寄り候補探索

## 保留メモ

- `worker6` の Cookie 取得は未解決
- Google Form の掲載申請フォーム作成は未着手
- local には `monthly_collect.py` と補助スクリプト群の未push差分が残っている

## 次回再開メモ

- public はいったん安定状態
- 次は `period を public に戻す作業` ではなく、`data の厚みを増やす作業` から再開する
- 再開時はまず `PLAN.md` を見て、`raw件数 / public件数 / period件数` を確認する
