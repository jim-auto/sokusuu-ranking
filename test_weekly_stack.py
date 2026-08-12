# -*- coding: utf-8 -*-
"""Weekly stack detection coverage tests."""
from datetime import date

from weekly_collect import (
    count_stack_units,
    extract_day_recap_n,
    is_live_soku_announce,
    is_meta_or_third_party_soku_talk,
    stack_weekly_from_tweets,
)

START, END = date(2026, 7, 27), date(2026, 8, 2)


def test_emoji_so_age():
    assert count_stack_units("🍐そ21🦏タトゥーネキ\nセク値過去一ぐらい高くて大満足") == 1
    assert count_stack_units("🪩そ21🦏タトゥーネキ") == 1
    assert is_live_soku_announce("🍐そ19🦏夜職📮狂")


def test_times_multiplier():
    assert count_stack_units("本日🗼🍛即×2‼️\n\n2人ともシコい") == 2
    assert extract_day_recap_n("本日🗼🍛即×2‼️") == ("today", 2)


def test_pass_so_and_chonso():
    assert count_stack_units("TAKAMIさんパスそあざす‼️") == 1
    assert count_stack_units("KBK某ちょんそ\n某るんさんあざす") == 1
    assert count_stack_units("🧚乞食ちょんそ\n流石に2即目いく") == 1
    assert is_live_soku_announce("パスそあざす‼️")


def test_app_soku_and_nn():
    assert count_stack_units("ひっさびさにapp即\n新規即気持ちええんじゃあ") == 1
    text = "39🦏/来多ー/タイプ値5.5/🦐\n駅前で…NS即"
    assert count_stack_units(text) == 1


def test_nocount_and_future():
    assert is_meta_or_third_party_soku_talk("🗼🍛即\n\nスト中の下なので即数はノーカウント🥲")
    assert count_stack_units("🗼🍛即\n\nスト中の下なので即数はノーカウント🥲") == 0
    # 未来の「2即目いく」だけなら 0（ちょんそが無い場合）
    assert count_stack_units("流石に2即目いく") == 0


def test_greed_week_stack():
    tweets = [
        {
            "id": "1",
            "text": "🍐そ19🦏夜職📮狂\nカルバンクラインえろかった",
            "created_at": "Mon Jul 27 11:17:09 +0000 2026",
        },
        {
            "id": "2",
            "text": "🪩そ21🦏タトゥーネキ\nセク値過去一ぐらい高くて大満足",
            "created_at": "Mon Jul 27 16:24:52 +0000 2026",
        },
        {
            "id": "3",
            "text": "🍐そ21🦏🎯🍾店員\nリーセグダあったけどなんとかいけた",
            "created_at": "Tue Jul 28 11:27:04 +0000 2026",
        },
    ]
    hit = stack_weekly_from_tweets(tweets, "greed_pua", START, END)
    assert hit and hit["count"] == 3, hit


def test_today_two_bang():
    assert extract_day_recap_n("本日2即！ 🎩いくぜ！") == ("today", 2)


def test_mic_week_stack():
    tweets = [
        {
            "id": "1",
            "text": "久々復帰🎤\n本日🗼🍛即×2‼️\n\n2人ともシコい\nマインド回復❤️‍🩹",
            "created_at": "Tue Jul 28 14:57:27 +0000 2026",
        },
        {
            "id": "2",
            "text": "本日🗼🍛2即目",
            "created_at": "Wed Jul 29 09:49:14 +0000 2026",
        },
    ]
    hit = stack_weekly_from_tweets(tweets, "mic_pua", START, END)
    assert hit and hit["count"] == 3, hit


if __name__ == "__main__":
    test_emoji_so_age()
    test_times_multiplier()
    test_pass_so_and_chonso()
    test_app_soku_and_nn()
    test_nocount_and_future()
    test_greed_week_stack()
    test_mic_week_stack()
    print("ok")
