# custom_date_digester.py
# ─────────────────────────────────────────────────────────────
# «Шаг-4»: собирает темы-чата + посты канала за произвольный период,
# группирует их в ≤10 «сюжетов», делает один готовый пост-дайджест
# и кладёт в //tmp/ia-nartov/hackathon/custom_date_digest  (upsert)

from __future__ import annotations
import datetime as dt, json, os, re
from typing import Any, Dict, List

import pandas as pd
from dateutil import tz

from . import tg_etl
from . import eliza_client


# ─────────── YT таблицы ───────────
ROOT            = os.getenv("YT_ROOT", "//tmp/ia-nartov/hackathon")
TBL_TOPICS      = f"{ROOT}/topic_analysis"
TBL_POSTS       = f"{ROOT}/post_summaries"
TBL_CHATS       = f"{ROOT}/tg_chats"
TBL_OUT         = f"{ROOT}/custom_date_digest"

SCHEMA_OUT = [
    {"name": "digest_id",   "type": "string", "sort_order": "ascending"},  # channel_id+start+end
    {"name": "channel_id",  "type": "int64"},
    {"name": "start_date",  "type": "string"},
    {"name": "end_date",    "type": "string"},
    {"name": "digest_text", "type": "string"},
]

MSK = tz.gettz("Europe/Moscow")

# ─────────── PROMPTS ───────────
PERIOD_PROMPT = """
Ты — ведущий редактор периодических дайджестов.
За указанный период ты получаешь **единый список элементов**, где каждый элемент —
либо пересказ темы-обсуждения из чата (`type = "topic"`),
либо сжатый пост канала (`type = "post"`).
Твоя задача — сгруппировать связанные элементы в «сюжеты» и выдать
**до 10** наиболее значимых сюжетных линий периода.

────────────────────────────────────────
🛈 ВХОД
Период: {start_date} → {end_date}
Канал: {channel_name} — {channel_description}

Данные:
json
{items_json}


────────────────────────────────────────
📋 ЗАДАЧА
• Сгруппируй элементы в сюжет, если они продолжают одну тему
(похожее название, общая цель, одни и те же ключевые участники, или логическое продолжение обсуждения).
• ВАЖНО: Учитывай, что status и conclusions одного дня могут перетекать в следующий день, если обсуждение продолжается.
• Для каждого сюжета сформируй итог с учётом динамики по дням и эволюции обсуждения.
• Отсортируй сюжеты по убыванию значимости и выбери максимум 10.

────────────────────────────────────────
📤 ВЫХОД — JSON-массив (1–10 объектов) в формате:

jsonc
[
  {{
    "rank": 1,
    "title": "…",
    "days_covered": ["YYYY-MM-DD", …],
    "summary": "…",
    "evolution": ["…", "…"],
    "final_status": "решено|спор|отложили|неясно|null",
    "key_participants": [
      {{"name":"…","role":"…"}}
    ],
    "resume": "Одна итоговая фраза"
  }},
  /* rank 2 … */
]


⚠️ Ограничения
• Используй только входные данные, не придумывай фактов.
• Поля title/summary/evolution/resume — русский, нейтрально-деловой, без markdown.
• Итог — валидный JSON, ≤ 600 токенов.
────────────────────────────────────────
Сформируй массив ровно в указанном формате (без комментариев вокруг).
"""

POST_PROMPT = """
Сделай один готовый пост-дайджест для канала:

Формат:

1. Заголовок вида «Дайджест:  {start} – {end}»
2. Затем для каждого сюжета bullet-пункт «• [дни] title: summary»
   где [дни] - это интервал дней, когда обсуждалась тема (например, [21-23])

Правила:
• Больше ничего: никакого markdown, эмодзи, подписей.
• Пункты идут по рангу сюжетов.
• До 10 пунктов.
• Обязательно указывай интервал дней для каждого пункта

Данные сюжетов (JSON):

json
{stories_json}


────────────────────────────────────────
Верни ОДИН текстовый блок-пост без обрамления.
"""

# ─────────── helpers ───────────

def _channel_row(channel_id: int) -> pd.Series:
    return tg_etl.query_yql(
    f"""SELECT chat, description
    FROM hahn.`{TBL_CHATS}`
    WHERE chat_id = {channel_id}
    LIMIT 1;"""
    ).iloc[0]

def _load_items(channel_id: int,
    start: dt.date,
    end: dt.date) -> list[dict]:
    """возвращает объединённый список items за период"""
    topics = tg_etl.query_yql(
    f"""
    SELECT
    "topic" AS type, date, summary, status, conclusions
    FROM hahn.`{TBL_TOPICS}`
    WHERE channel_id = {channel_id}
    AND date BETWEEN "{start}" AND "{end}"
    """
    )
    
    items = []
    if not topics.empty:
        items.extend(topics.to_dict("records"))
    return items

def _prompt_period(start: dt.date,
    end: dt.date,
    channel: pd.Series,
    items: list[dict]) -> List[Dict[str, str]]:
    
    txt = PERIOD_PROMPT.format(
    start_date=start,
    end_date=end,
    channel_name=channel["chat"],
    channel_description=channel["description"],
    items_json=json.dumps({"items": items}, ensure_ascii=False, indent=2)
    )
    return [{"role": "user", "content": txt}]

def _parse_stories(raw: str) -> list[dict]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        cleaned = re.sub(r"`json|`", "", raw, flags=re.I).strip()
        data = json.loads(cleaned)
        
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError("LLM output must be non-empty JSON array")
    return data[:10]

def _prompt_post(start: dt.date, end: dt.date,
    stories: list[dict]) -> List[Dict[str, str]]:
    txt = POST_PROMPT.format(
    start=start.strftime("%d %b"),
    end=end.strftime("%d %b"),
    stories_json=json.dumps(stories, ensure_ascii=False, indent=2)
    )
    return [{"role": "user", "content": txt}]

# ─────────── main entry ───────────

def run_custom_date_digester(
    start_date: dt.date,
    end_date: dt.date,
    channel_id: int,
    *,
    model: str = "yandex",
    verify: bool | str = True,
    ) -> None:
    """
    Делает дайджест за произвольный период (текст-пост) и кладёт одну строку в custom_date_digest.
    """
    channel = _channel_row(channel_id)
    items   = _load_items(channel_id, start_date, end_date)
    
    
    if not items:
        print("⏭  Нет контента за этот период")
        return
    
    # step-1: LLM группирует сюжеты
    rsp1 = eliza_client.eliza_chat(
        _prompt_period(start_date, end_date, channel, items),
        model=model,
        verify=verify
    )
    stories = _parse_stories(rsp1["response"]["Responses"][0]["Response"])

    # step-2: LLM формирует финальный пост
    rsp2 = eliza_client.eliza_chat(
        _prompt_post(start_date, end_date, stories),
        model=model,
        verify=verify
    )
    digest_text = rsp2["response"]["Responses"][0]["Response"].strip()
    
    # Красивый вывод результата
    print(f"\n{'='*60}")
    print(f"📊 ДАЙДЖЕСТ ЗА ПЕРИОД {start_date} – {end_date}")
    print(f"📢 Канал: {channel['chat']}")
    print(f"{'='*60}")
    print(f"\n{digest_text}")
    print(f"\n{'='*60}")
    
    row = {
        "digest_id": f"{channel_id}_{start_date}_{end_date}",
        "channel_id": channel_id,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "digest_text": digest_text
    }
    tg_etl.upsert_df_to_yt(pd.DataFrame([row]), TBL_OUT, SCHEMA_OUT)
    print(f"✅ custom date digest {start_date}–{end_date} saved → {TBL_OUT}")
