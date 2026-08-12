# -*- coding: utf-8 -*-
from monthly_collect import extract_yearly_count, is_in_reporting_window
from yearly_deep_recaps import tweet_in_year_window


def test_shin9suke_late_recap():
    text = "【2025年総括】\n57即(🐶19🦁8🦉3📦17遠征10(パス1))\n※今更ですが"
    assert extract_yearly_count(text, 2025) == 57
    assert tweet_in_year_window("Thu Jun 04 16:37:14 +0000 2026", 2025, text)
    assert is_in_reporting_window(
        "Thu Jun 04 16:37:14 +0000 2026", "yearly", 2025, text=text
    )


def test_sugi_yearly():
    text = "【2025年総括】92即\n🔥46 🌹15🥥13 🍐8🍎8 🟥1📦1\n1月15即"
    assert extract_yearly_count(text, 2025) == 92


def test_reject_december_monthly():
    text = "12月　計24 即\n\nネト　22即\n🦾      1即"
    assert extract_yearly_count(text, 2025) is None


def test_makoto_year_month_series():
    text = (
        "2025年スト開始からのトータル\n"
        "3月 2即\n4月 2即\n5月 0即\n6月 0即\n7月 3即\n"
        "8月 4即\n9月 2即\n10月 5即\n11月 2即\n12月 10即"
    )
    assert extract_yearly_count(text, 2025) == 30


def test_reject_old_year_without_2025():
    assert not tweet_in_year_window(
        "Mon Jan 01 00:00:00 +0000 2024", 2025, "2023年総括 100即"
    )


if __name__ == "__main__":
    test_shin9suke_late_recap()
    test_sugi_yearly()
    test_reject_december_monthly()
    test_reject_old_year_without_2025()
    print("ok")
