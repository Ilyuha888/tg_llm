# ─────────────────────────────────────────────────────────────
import datetime as dt
import os
from typing import List

from . import tg_etl
from . import topic_extractor
from . import topic_resumator_chat


ROOT       = os.getenv("YT_ROOT", "//tmp/ia-nartov/hackathon")
TBL_MSG    = f"{ROOT}/tg_raw_enriched"     # сырьё
TBL_TOPICS = f"{ROOT}/daily_topics"        # результат topic_extractor


def _channels_with_msgs(start: dt.date, end: dt.date) -> List[int]:
    """возвращает list(chat_id), у которых есть сообщения в диапазоне."""
    df = tg_etl.query_yql(
        f"""
        SELECT DISTINCT chat_id
        FROM hahn.`{TBL_MSG}`
        WHERE DateTime::MakeDate(DateTime::ParseIso8601(dttm)) BETWEEN Date("{start}") AND Date("{end}");
        """
    )
    return df["chat_id"].tolist()


def _topic_ids(channel_id: int, day: dt.date) -> List[str]:
    """возвращает topic_id-ы, созданные extractor'ом по (канал,дата)."""
    try:
        df = tg_etl.query_yql(
            f"""
            SELECT topic_id
            FROM hahn.`{TBL_TOPICS}`
            WHERE channel_id = {channel_id}
              AND date = "{day}";
            """
        )
        return df["topic_id"].tolist()
    except Exception as e:
        # Таблица может не существовать, если extractor не создал тем
        print(f"⏭  Нет таблицы daily_topics или тем для {channel_id} на {day}: {e}")
        return []


# ─────────────────────────────────────────────────────────────
def process_interval(
    start: dt.date,
    end: dt.date,
    *,
    channel_id: int = None,
    model: str = "yandex",
    verify: bool | str = True,
) -> None:
    """
    Прогоны topic_extractor  ➜  topic_resumator_chat
    для всех каналов (или конкретного канала), где были сообщения в [start; end] (включительно).
    """
    if channel_id:
        # Обрабатываем только указанный канал
        channels = [channel_id]
        print(f"🎯 Обрабатываем только канал {channel_id}")
    else:
        # Обрабатываем все каналы с сообщениями
        channels = _channels_with_msgs(start, end)
        print(f"📊 Найдено каналов с сообщениями: {len(channels)}")
    
    if not channels:
        print("⏭  Сообщений в интервале нет — делать нечего")
        return

    days = [
        start + dt.timedelta(days=i)
        for i in range((end - start).days + 1)
    ]

    for ch in channels:
        print(f"\n🔄 Обрабатываем канал {ch}")
        for day in days:
            # 1) daily extractor
            try:
                topic_extractor.run_topic_extractor(
                    date=day,
                    channel_id=ch,
                    model=model,
                    verify=verify,
                )
            except Exception as e:
                print(f"⚠️ extractor fail {ch=} {day}: {e}")
                continue

            # 2) resumator для новых тем
            topic_ids = _topic_ids(ch, day)
            for tid in topic_ids:
                try:
                    topic_resumator_chat.run_topic_resumator(
                        topic_id=tid,
                        model=model,
                        verify=verify,
                    )
                except Exception as e:
                    print(f"⚠️ resumator fail {tid}: {e}")

    print("✅ interval processing finished")
