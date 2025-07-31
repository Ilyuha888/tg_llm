#!/usr/bin/env python3
"""
Скрипт для инициализации тестовых данных в YTsaurus
Основан на блоке из Jupyter ноутбука upload_massages.ipynb
"""

import asyncio
import os
import pathlib
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла в директории модуля
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(env_path)

from . import tg_etl
from . import test_data

def init_test_data(days_back_start: int = 3, days_back_end: int = 0):
    """
    Инициализация тестовых данных в YT
    
    Parameters
    ----------
    days_back_start : int, default 3
        Количество дней назад от текущего момента для начала интервала
    days_back_end : int, default 0
        Количество дней назад от текущего момента для конца интервала (0 = сейчас)
    """
    
    # Получаем пути к таблицам из переменных окружения
    users_table = os.getenv("YT_USERS_TABLE", "//tmp/ia-nartov/hackathon/tg_users")
    chats_table = os.getenv("YT_CHATS_TABLE", "//tmp/ia-nartov/hackathon/tg_chats")
    messages_table = os.getenv("YT_MESSAGES_TABLE", "//tmp/ia-nartov/hackathon/tg_raw_enriched")
    
    # Получаем тестовые данные
    df_users = test_data.get_test_users()
    df_chats = test_data.get_test_chats()
    
    # Загружаем в YT
    print(f"Загружаем пользователей в {users_table}...")
    tg_etl.upload_df_to_yt(
        df_users,
        users_table,
        test_data.USER_SCHEMA,
        overwrite=True
    )
    
    print(f"Загружаем чаты в {chats_table}...")
    tg_etl.upload_df_to_yt(
        df_chats,
        chats_table,
        test_data.CHAT_SCHEMA,
        overwrite=True
    )
    
    # Выгружаем сообщения из Telegram чатов
    print(f"\n🔄 Выгружаем сообщения из Telegram за период:")
    print(f"   Начало: {days_back_start} дней назад")
    print(f"   Конец: {days_back_end} дней назад")
    print(f"Чаты для выгрузки: {len(test_data.TG_CHATS)}")
    
    try:
        # Используем asyncio для вызова асинхронной функции dump_chats
        df_messages = asyncio.run(tg_etl.dump_chats(
            test_data.TG_CHATS,
            days_back_start=days_back_start,
            days_back_end=days_back_end
        ))
        
        print(f"Получено сообщений: {len(df_messages)}")
        
        # Загружаем сообщения в YT
        print(f"Загружаем сообщения в {messages_table}...")
        tg_etl.upload_df_to_yt(
            df_messages,
            messages_table,
            test_data.MSG_SCHEMA,
            overwrite=True
        )
        
        print("✅ Сообщения из Telegram выгружены и загружены в YT!")
        
    except Exception as e:
        print(f"❌ Ошибка при выгрузке сообщений: {e}")
        print("Возможные причины:")
        print("- Не настроены TG_API_ID и TG_API_HASH в .env")
        print("- Нет доступа к указанным чатам")
        print("- Проблемы с авторизацией в Telegram")
    
    print("\n✅ Инициализация тестовых данных завершена!")
    print(f"Пользователей: {len(df_users)}")
    print(f"Чатов: {len(df_chats)}")
    
    # Показываем ID чатов для использования
    print("\nID чатов для тестирования:")
    for _, chat in df_chats.iterrows():
        print(f"  {chat['chat_id']} - {chat['chat']}")

if __name__ == "__main__":
    init_test_data()