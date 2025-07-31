"""
─────────────────────────────────────────────────────────────
• собирает input:
    topics → //…/topic_analysis
    posts  → //…/post_summaries
• формирует prompt (см. ТЗ) → вызывает LLM
• пишет TOP-5 в //…/daily_digest  (upsert)

DEPENDENCIES
  tg_etl.query_yql, tg_etl.upsert_df_to_yt
  eliza_client.eliza_chat
"""

from __future__ import annotations
import json, os, re, datetime as dt
from typing import Any, Dict, List

import pandas as pd
from dateutil import tz

from . import tg_etl
from . import eliza_client


# ─────────── YT таблицы ───────────
ROOT           = os.getenv("YT_ROOT", "//tmp/ia-nartov/hackathon")
TBL_TOPICS     = f"{ROOT}/topic_analysis"
TBL_POSTS      = f"{ROOT}/post_summaries"
TBL_CHATS      = f"{ROOT}/tg_chats"
TBL_OUT        = f"{ROOT}/daily_digest"

SCHEMA_OUT = [
    {"name": "digest_id",   "type": "string", "sort_order": "ascending"},
    {"name": "channel_id",  "type": "int64"},
    {"name": "date",        "type": "string"},
    {"name": "digest_text", "type": "string"}      # ← готовый пост
]

MSK = tz.gettz("Europe/Moscow")

# ─────────── PROMPT ───────────
PROMPT_TMPL = """
Ты — главный редактор ежедневного дайджеста канала.
На вход ты получаешь перечень тем-обсуждений из чата.
Тебе нужно создать структурированный дайджест с разбивкой на обсуждения и коммиты.

────────────────────────────────────────
🛈 ВХОД

Дата: {date}
Канал: {channel_name} — {channel_description}

Темы за день:
{input_payload}

────────────────────────────────────────
📋 ЗАДАЧА

1. Проанализируй все темы и раздели их на две категории:
   - **ОБСУЖДЕНИЯ**: темы, где люди обсуждали вопросы, проблемы, идеи
   - **КОММИТЫ**: темы, где кто-то обещал что-то сделать к определенному времени

2. Для каждой категории создай bullet-список наиболее важных пунктов
3. Используй данные из полей: status, conclusions, resume

────────────────────────────────────────
📤 ОБЯЗАТЕЛЬНЫЙ ВЫХОД

{{
  "discussions": [
    "• Краткое описание обсуждения 1",
    "• Краткое описание обсуждения 2"
  ],
  "commitments": [
    "• Кто обещал что сделать когда",
    "• Другой коммит с дедлайном"
  ]
}}

────────────────────────────────────────
⚠️ ОГРАНИЧЕНИЯ
• Используй только данные из входа
• Каждый bullet должен быть информативным и кратким
• Если нет коммитов или обсуждений, верни пустой массив
• Выход — валидный JSON без комментариев

────────────────────────────────────────
Создай структурированный дайджест в указанном JSON-формате.
"""


# ─────────── helpers ───────────

def _get_channel(channel_id: int) -> pd.Series:
    return tg_etl.query_yql(
    f"""SELECT chat, description
    FROM hahn.`{TBL_CHATS}`
    WHERE chat_id = {channel_id}
    LIMIT 1;"""
    ).iloc[0]

def _load_topics(channel_id: int, date: dt.date) -> list[dict]:
    """возвращает [{status, conclusions, resume}, …]"""
    df = tg_etl.query_yql(
        f"""
        SELECT status, conclusions, resume
        FROM hahn.`{TBL_TOPICS}`
        WHERE channel_id = {channel_id}
          AND date = "{date}"
        ORDER BY topic_id;
        """
    )
    return df.to_dict("records") if not df.empty else []

# def _load_posts(channel_id: int, date: dt.date) -> list[dict]:
#     df = query_yql(
#     f"""
#     SELECT time, short_summary, detailed_summary
#     FROM hahn.`{TBL_POSTS}`
#     WHERE channel_id = {channel_id}
#     AND toDate(post_ts) = Date("{date}")
#     ORDER BY post_ts;
#     """
#     )
#     if df.empty:
#         return []
        
#     df["detailed_summary"] = df["detailed_summary"].apply(json.loads)
#     return df.to_dict("records")

def _prompt(date: dt.date,
            channel: pd.Series,
            topics: list[dict]) -> List[Dict[str, str]]:
    
    payload = json.dumps(topics, ensure_ascii=False, indent=2)
    
    txt = PROMPT_TMPL.format(
        date=date,
        channel_name=channel["chat"],
        channel_description=channel["description"],
        input_payload=payload,
    )
    
    return [{"role": "user", "content": txt}]

def _parse_answer(raw: str) -> Dict[str, Any]:
    """Парсит ответ LLM и возвращает структурированный дайджест."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        cleaned = re.sub(r"```json|```", "", raw, flags=re.I).strip()
        data = json.loads(cleaned)

    return data

def _format_digest_text(digest_data: Dict[str, Any]) -> str:
    """Форматирует дайджест в красивый текст для сохранения в YT."""
    lines = []
    
    if digest_data.get("discussions"):
        lines.append("🗣️ ОБСУЖДЕНИЯ:")
        for item in digest_data["discussions"]:
            lines.append(f"  {item}")
        lines.append("")
    
    if digest_data.get("commitments"):
        lines.append("📋 КОММИТЫ:")
        for item in digest_data["commitments"]:
            lines.append(f"  {item}")
    
    return "\n".join(lines)

# ─────────── main entry ───────────

def run_daily_digester(
    date: dt.date,
    channel_id: int,
    *,
    model: str = "yandex",
    verify: bool | str = True,
    ) -> None:
    """Собирает дневной дайджест и кладёт в daily_digest."""
    channel = _get_channel(channel_id)
    topics  = _load_topics(channel_id, date)

    # Проверяем наличие данных для анализа
    if not topics:
        print("⏭  Нечего дайджестить: нет topics")
        return

    # Проверяем качество данных
    meaningful_topics = [t for t in topics if t.get('resume') and len(str(t.get('resume', '')).strip()) > 10]
    if not meaningful_topics:
        print("⏭  Нет содержательных тем для дайджеста; пропуск")
        return

    prompt = _prompt(date, channel, meaningful_topics)
    rsp = eliza_client.eliza_chat(prompt, model=model, verify=verify)
    raw_json = rsp["response"]["Responses"][0]["Response"]
    
    digest_data = _parse_answer(raw_json)
    
    # Проверяем, что получили содержательный дайджест
    if not digest_data or (not digest_data.get("discussions") and not digest_data.get("commitments")):
        print("⏭  Модель вернула пустой дайджест; пропуск сохранения в YT")
        return
    
    # Форматируем для сохранения в YT
    digest_text = _format_digest_text(digest_data)
    
    # Красивый вывод результата (показываем как хранится в табличке)
    print(f"\n{'='*60}")
    print(f"📊 ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ {date}")
    print(f"📢 Канал: {channel['chat']}")
    print(f"{'='*60}")
    print(f"\n{digest_text}")
    print(f"\n{'='*60}")
    
    row = {
        "digest_id": f"{channel_id}_{date}",
        "channel_id": channel_id,
        "date": str(date),
        "digest_text": digest_text
    }
    tg_etl.upsert_df_to_yt(pd.DataFrame([row]), TBL_OUT, SCHEMA_OUT)
    print(f"✅ daily digest ({date}) upsert → {TBL_OUT}")