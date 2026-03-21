"""
即数ランキング HTML レポート生成スクリプト

data/sokusuu_accounts.json から GitHub Pages 用の
index.html を docs/ に生成する。

ランキング4種:
  1. 総合（全員）
  2. ストリートナンパ
  3. クラブナンパ
  4. オンライン（マッチングアプリ等）
"""

import json
import os
from datetime import datetime


INPUT_JSON = "data/sokusuu_accounts.json"
OUTPUT_DIR = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "index.html")

CATEGORY_LABELS = {
    "all": "総合",
    "street": "ストリート",
    "club": "クラブ",
    "online": "オンライン",
}


def load_data(filepath: str) -> list[dict]:
    """JSONデータを読み込む"""
    if not os.path.exists(filepath):
        print(f"[ERROR] {filepath} が見つかりません。先に scraper.py を実行してください。")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_by_category(records: list[dict], category: str) -> list[dict]:
    """カテゴリでフィルタする。'all' なら全件返す。"""
    if category == "all":
        return records
    return [r for r in records if category in (r.get("categories") or "")]


def build_ranking_rows(records: list[dict], show_category: bool = False) -> str:
    """ランキングテーブルの行HTMLを生成する"""
    rows = ""
    for i, r in enumerate(records, 1):
        medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")

        source_badge = (
            '<span class="badge badge-profile">プロフィール</span>'
            if r["source"] == "profile"
            else '<span class="badge badge-pinned">固定ツイート</span>'
        )

        followers = r.get("followers_count", 0)
        followers_str = f"{followers:,}" if followers else "-"

        alt_html = ""
        alt = r.get("alt_accounts", "")
        if alt:
            alt_html = f'<span class="alt-badge">= {alt}</span>'

        avatar_url = r.get("profile_image_url", "")
        avatar_html = f'<img class="avatar" src="{avatar_url}" alt="">' if avatar_url else '<div class="avatar avatar-placeholder"></div>'

        cat_html = ""
        if show_category:
            cats = r.get("categories", "")
            if cats:
                cat_badges = ""
                for c in cats.split(", "):
                    label = CATEGORY_LABELS.get(c, c)
                    cat_badges += f'<span class="badge badge-cat-{c}">{label}</span> '
                cat_html = f'<td>{cat_badges}</td>'
            else:
                cat_html = '<td><span class="badge badge-cat-none">未分類</span></td>'

        rows += f"""
            <tr>
                <td class="rank">{medal}{i}</td>
                <td class="user-cell">
                    {avatar_html}
                    <div class="user-info">
                        <a href="{r['url']}" target="_blank" rel="noopener">@{r['username']}</a>
                        {alt_html}
                    </div>
                </td>
                <td class="display-name">{r['display_name']}</td>
                <td class="sokusuu">{r['sokusuu']:,}{"+" if r.get("approximate") else ""}</td>
                <td>{source_badge}</td>
                {cat_html}
                <td class="followers">{followers_str}</td>
            </tr>"""
    return rows


def generate_html(records: list[dict]) -> str:
    """ランキングHTMLを生成する"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # カテゴリ別テーブルを生成
    tab_buttons = ""
    tab_contents = ""

    categories = ["all", "street", "club", "online"]
    for idx, cat in enumerate(categories):
        label = CATEGORY_LABELS[cat]
        filtered = filter_by_category(records, cat)
        active = " active" if idx == 0 else ""
        count = len(filtered)

        tab_buttons += f'        <div class="tab{active}" onclick="switchTab(\'{cat}\')">{label} ({count})</div>\n'

        show_cat = cat == "all"
        cat_header = '<th>カテゴリ</th>' if show_cat else ''
        rows = build_ranking_rows(filtered, show_category=show_cat)

        tab_contents += f"""
    <div id="tab-{cat}" class="tab-content{active}">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>即数</th>
                    <th>ソース</th>
                    {cat_header}
                    <th>フォロワー</th>
                </tr>
            </thead>
            <tbody>{rows}
            </tbody>
        </table>
    </div>
"""

    # フォロワー数ランキング
    followers_sorted = sorted(records, key=lambda r: r.get("followers_count", 0), reverse=True)
    followers_rows = ""
    rank = 0
    for r in followers_sorted:
        followers = r.get("followers_count", 0)
        if followers == 0:
            continue
        rank += 1
        medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(rank, "")
        avatar_url = r.get("profile_image_url", "")
        av_html = f'<img class="avatar" src="{avatar_url}" alt="">' if avatar_url else '<div class="avatar avatar-placeholder"></div>'
        followers_rows += f"""
            <tr>
                <td class="rank">{medal}{rank}</td>
                <td class="user-cell">
                    {av_html}
                    <div class="user-info">
                        <a href="https://twitter.com/{r['username']}" target="_blank" rel="noopener">@{r['username']}</a>
                    </div>
                </td>
                <td class="display-name">{r['display_name']}</td>
                <td class="followers">{followers:,}</td>
                <td class="sokusuu">{r['sokusuu']:,}</td>
            </tr>"""

    tab_buttons += '        <div class="tab" onclick="switchTab(\'followers\')">フォロワー数</div>\n'
    tab_contents += f"""
    <div id="tab-followers" class="tab-content">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>フォロワー</th>
                    <th>即数</th>
                </tr>
            </thead>
            <tbody>{followers_rows}
            </tbody>
        </table>
    </div>
"""

    # 月間ランキング
    monthly_file = "data/monthly_ranking.json"
    if os.path.exists(monthly_file):
        with open(monthly_file, "r", encoding="utf-8") as f:
            monthly_data = json.load(f)
        monthly_rows = ""
        for i, r in enumerate(monthly_data, 1):
            medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")
            avatar_url = r.get("profile_image_url", "")
            av_html = f'<img class="avatar" src="{avatar_url}" alt="">' if avatar_url else '<div class="avatar avatar-placeholder"></div>'
            achieved_m = r.get("achieved_date")
            date_str = f'<span style="color:#888">{achieved_m}</span>' if achieved_m else '<span style="color:#444">-</span>'
            monthly_rows += f"""
            <tr>
                <td class="rank">{medal}{i}</td>
                <td class="user-cell">
                    {av_html}
                    <div class="user-info">
                        <a href="https://twitter.com/{r['username']}" target="_blank" rel="noopener">@{r['username']}</a>
                    </div>
                </td>
                <td class="display-name">{r.get('display_name', '')}</td>
                <td class="sokusuu">{r['monthly_best']:,}{"+" if r.get("approximate") else ""}</td>
                <td>{date_str}</td>
            </tr>"""

        tab_buttons += f'        <div class="tab" onclick="switchTab(\'monthly\')">月間記録 ({len(monthly_data)})</div>\n'
        tab_contents += f"""
    <div id="tab-monthly" class="tab-content">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>月間最多</th>
                    <th>達成時期</th>
                </tr>
            </thead>
            <tbody>{monthly_rows}
            </tbody>
        </table>
    </div>
"""

    # 年間ランキング
    yearly_file = "data/yearly_ranking.json"
    if os.path.exists(yearly_file):
        with open(yearly_file, "r", encoding="utf-8") as f:
            yearly = json.load(f)
        yearly_rows = ""
        for i, r in enumerate(yearly, 1):
            medal = {1: "🥇 ", 2: "🥈 ", 3: "🥉 "}.get(i, "")
            avatar_url = r.get("profile_image_url", "")
            av_html = f'<img class="avatar" src="{avatar_url}" alt="">' if avatar_url else '<div class="avatar avatar-placeholder"></div>'
            achieved = r.get("achieved_year")
            year_str = f'<span style="color:#888">{achieved}年</span>' if achieved else '<span style="color:#444">-</span>'
            yearly_rows += f"""
            <tr>
                <td class="rank">{medal}{i}</td>
                <td class="user-cell">
                    {av_html}
                    <div class="user-info">
                        <a href="https://twitter.com/{r['username']}" target="_blank" rel="noopener">@{r['username']}</a>
                    </div>
                </td>
                <td class="display-name">{r.get('display_name', '')}</td>
                <td class="sokusuu">{r['yearly_best']:,}</td>
                <td>{year_str}</td>
            </tr>"""

        tab_buttons += f'        <div class="tab" onclick="switchTab(\'yearly\')">年間記録 ({len(yearly)})</div>\n'
        tab_contents += f"""
    <div id="tab-yearly" class="tab-content">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>アカウント</th>
                    <th>表示名</th>
                    <th>年間最多</th>
                    <th>達成年</th>
                </tr>
            </thead>
            <tbody>{yearly_rows}
            </tbody>
        </table>
    </div>
"""

    # 統計
    max_sokusuu = records[0]['sokusuu'] if records else 0
    avg_sokusuu = sum(r['sokusuu'] for r in records) // len(records) if records else 0
    sorted_vals = sorted(r['sokusuu'] for r in records)
    n = len(sorted_vals)
    median_sokusuu = (sorted_vals[n//2-1] + sorted_vals[n//2]) // 2 if n % 2 == 0 else sorted_vals[n//2] if n else 0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>即数ランキング</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f;
            color: #e0e0e0;
            padding: 20px;
            max-width: 1000px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            font-size: 2em;
            margin: 20px 0 5px;
            color: #fff;
        }}
        .subtitle {{
            text-align: center;
            color: #888;
            margin-bottom: 30px;
            font-size: 0.9em;
        }}
        .stats {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 12px;
            padding: 15px 25px;
            text-align: center;
        }}
        .stat-card .number {{
            font-size: 2em;
            font-weight: bold;
            color: #ff6b6b;
        }}
        .stat-card .label {{
            font-size: 0.85em;
            color: #888;
            margin-top: 5px;
        }}
        .tabs {{
            display: flex;
            gap: 8px;
            margin-bottom: 20px;
            justify-content: center;
            flex-wrap: wrap;
        }}
        .tab {{
            padding: 8px 18px;
            border: 1px solid #333;
            border-radius: 8px;
            background: #1a1a1a;
            color: #aaa;
            cursor: pointer;
            font-size: 0.9em;
            transition: all 0.2s;
        }}
        .tab:hover {{ border-color: #555; color: #fff; }}
        .tab.active {{ background: #ff6b6b; border-color: #ff6b6b; color: #fff; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #1a1a1a;
            border-radius: 12px;
            overflow: hidden;
        }}
        th {{
            background: #252525;
            padding: 12px 15px;
            text-align: left;
            font-weight: 600;
            color: #aaa;
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        td {{
            padding: 12px 15px;
            border-top: 1px solid #2a2a2a;
        }}
        tr:hover {{ background: #222; }}
        .rank {{ font-weight: bold; width: 60px; color: #fff; }}
        .user-cell {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .avatar {{
            width: 36px;
            height: 36px;
            border-radius: 50%;
            flex-shrink: 0;
            object-fit: cover;
        }}
        .avatar-placeholder {{
            background: #333;
        }}
        .user-info a {{ color: #1d9bf0; text-decoration: none; }}
        .user-info a:hover {{ text-decoration: underline; }}
        .display-name {{ color: #999; font-size: 0.9em; }}
        .sokusuu {{ font-weight: bold; color: #ff6b6b; font-size: 1.1em; }}
        .followers {{ font-weight: bold; color: #1d9bf0; }}
        .badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 0.75em;
            font-weight: 600;
            margin-right: 3px;
        }}
        .badge-profile {{ background: #1a3a2a; color: #4ade80; }}
        .badge-pinned {{ background: #3a2a1a; color: #fbbf24; }}
        .badge-cat-street {{ background: #1a2a3a; color: #60a5fa; }}
        .badge-cat-club {{ background: #2a1a3a; color: #c084fc; }}
        .badge-cat-online {{ background: #1a3a3a; color: #2dd4bf; }}
        .badge-cat-none {{ background: #2a2a2a; color: #666; }}
        .alt-badge {{
            display: block;
            font-size: 0.75em;
            color: #888;
            margin-top: 2px;
        }}
        .disclaimer {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 15px;
            margin-top: 30px;
            font-size: 0.8em;
            color: #888;
        }}
        .footer {{
            text-align: center;
            color: #555;
            margin-top: 40px;
            font-size: 0.8em;
            line-height: 1.8;
        }}
        @media (max-width: 600px) {{
            body {{ padding: 10px; }}
            .stats {{ flex-direction: column; align-items: center; }}
            th, td {{ padding: 8px 10px; font-size: 0.85em; }}
            .display-name {{ display: none; }}
            .tabs {{ gap: 5px; }}
            .tab {{ padding: 6px 12px; font-size: 0.8em; }}
        }}
    </style>
</head>
<body>
    <h1>即数ランキング</h1>
    <p class="subtitle">最終更新: {now}</p>

    <div class="stats">
        <div class="stat-card">
            <div class="number">{len(records)}</div>
            <div class="label">集計数</div>
        </div>
        <div class="stat-card">
            <div class="number">{max_sokusuu:,}</div>
            <div class="label">最高即数</div>
        </div>
        <div class="stat-card">
            <div class="number">{avg_sokusuu:,}</div>
            <div class="label">平均即数</div>
        </div>
        <div class="stat-card">
            <div class="number">{median_sokusuu:,}</div>
            <div class="label">中央値</div>
        </div>
    </div>

    <div style="text-align:center;margin-bottom:20px">
        <input type="text" id="searchBox" placeholder="ユーザー名で検索..." oninput="filterRows()"
            style="padding:8px 16px;border:1px solid #333;border-radius:8px;background:#1a1a1a;color:#e0e0e0;font-size:0.95em;width:300px;outline:none;">
    </div>

    <div class="tabs">
{tab_buttons}    </div>

{tab_contents}

    <div class="disclaimer">
        <strong>注意事項:</strong>
        即数は全て自己申告ベースであり、正確性は保証されません。
        プロフィールおよび固定ツイートから自動抽出した値です。
        カテゴリはプロフィール記載のキーワードから自動判定しています。
    </div>

    <div class="footer">
        <p>Data collected from X (Twitter) profiles and pinned tweets</p>
        <p>Built with Python</p>
    </div>

    <script>
        function switchTab(tab) {{
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
            document.getElementById('tab-' + tab).classList.add('active');
            event.target.classList.add('active');
            document.getElementById('searchBox').value = '';
            filterRows();
        }}
        function filterRows() {{
            const q = document.getElementById('searchBox').value.toLowerCase();
            const active = document.querySelector('.tab-content.active');
            if (!active) return;
            active.querySelectorAll('tbody tr').forEach(tr => {{
                const text = tr.textContent.toLowerCase();
                tr.style.display = text.includes(q) ? '' : 'none';
            }});
        }}
    </script>
</body>
</html>"""
    return html


def main():
    records = load_data(INPUT_JSON)
    if not records:
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html = generate_html(records)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    # カテゴリ別件数を表示
    all_count = len(records)
    street = len([r for r in records if "street" in (r.get("categories") or "")])
    club = len([r for r in records if "club" in (r.get("categories") or "")])
    online = len([r for r in records if "online" in (r.get("categories") or "")])
    print(f"[OUTPUT] {OUTPUT_HTML} を生成しました")
    print(f"  総合: {all_count} 名 / ストリート: {street} 名 / クラブ: {club} 名 / オンライン: {online} 名")


if __name__ == "__main__":
    main()
