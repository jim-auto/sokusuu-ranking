# 即数ランキング - Sokusuu Ranking

**ランキングページ: https://jim-auto.github.io/sokusuu-ranking/**

## プロジェクトの目的

ナンパ界隈において自己申告されている**即数（そくすう）**を X（Twitter）から収集し、ランキング用データセットを作成するツール。

## 即数とは

**即数（そくすう）**とは、ナンパ・出会いをきっかけに関係を持った人数の累計を指す指標。当日か後日かは問わない。

## 収集方法

以下の2箇所のみを対象に即数を抽出する。ツイート全体の検索は行わない。

- **プロフィール（bio）** - ユーザーの自己紹介文
- **固定ツイート（pinned tweet）** - ユーザーが固定しているツイート

### 検出パターン

```
即120 / 即 120
通算即120 / 通算 即 120
即数120 / 即数 120
経験人数1400
体験人数50
斬り80 / 斬り数150 / 人斬り200
ゲット数100 / GET100 / GN50
S数30
```

同一ユーザーのプロフィールと固定ツイートの両方に即数がある場合、**より大きい数値を採用**する。

## 使い方

### 1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 2. シードアカウントの設定

`seed_accounts.txt` に収集対象のユーザー名を1行1アカウントで記載する。

```
userA
userB
userC
```

### 3. スクリプトの実行

```bash
python scraper.py
```

### 4. 出力

- `data/sokusuu_accounts.json` - JSON形式
- `data/sokusuu_accounts.csv` - CSV形式

## データの注意点

- 即数は**自己申告ベース**であり、正確性は保証されない
- プロフィールや固定ツイートの内容は随時変更される可能性がある
- スクレイピングは X の利用規約に抵触する可能性があるため、利用は自己責任で行うこと
- 収集データの取り扱いには十分注意すること

## フォルダ構造

```
sokusuu-ranking/
├── scraper.py          # メインスクリプト（収集・抽出）
├── generate_html.py    # HTMLレポート生成
├── seed_accounts.txt   # シードアカウント一覧
├── requirements.txt    # 依存パッケージ
├── .env                # Xログイン情報（git管理外）
├── data/               # 出力ディレクトリ（git管理外）
│   ├── sokusuu_accounts.json
│   ├── sokusuu_accounts.csv
│   └── discovered_accounts.json
├── docs/               # GitHub Pages
│   └── index.html
└── README.md
```
