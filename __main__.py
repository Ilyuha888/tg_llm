#!/usr/bin/env python3
"""
Telegram Digester - система анализа и суммаризации Telegram-переписок

Основные компоненты:
- topic_extractor: извлекает темы из сообщений за день
- topic_resumator_chat: анализирует каждую тему
- daily_digester: создает дневной дайджест
- custom_date_digester: создает дайджест за произвольный период
- orchestrator: координирует весь процесс
"""

import argparse
import datetime as dt
import os
from typing import Optional

# Загружаем переменные окружения в самом начале
from dotenv import load_dotenv
import pathlib

# Определяем путь к .env файлу относительно текущего модуля
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(env_path)

from . import orchestrator
from . import topic_extractor
from . import topic_resumator_chat
from . import daily_digester
from . import custom_date_digester
from . import init_test_data
from . import tg_etl
from . import test_data
import asyncio


def main():
    parser = argparse.ArgumentParser(description="Telegram Digester - анализ переписок")
    
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда process_interval
    interval_parser = subparsers.add_parser('interval', help='Обработать интервал дат')
    interval_parser.add_argument('--start', type=str, required=True, help='Начальная дата (YYYY-MM-DD)')
    interval_parser.add_argument('--end', type=str, required=True, help='Конечная дата (YYYY-MM-DD)')
    interval_parser.add_argument('--channel-id', type=int, help='ID канала (если не указан, обрабатываются все каналы)')
    interval_parser.add_argument('--model', type=str, default='yandex', choices=['yandex', 'deepseek'], help='Модель LLM')
    interval_parser.add_argument('--verify', type=str, default=str(pathlib.Path(__file__).parent / 'YandexInternalRootCA.pem'), help='Путь к CA-сертификату')
    
    # Команда topic_extractor
    extract_parser = subparsers.add_parser('extract', help='Извлечь темы за день')
    extract_parser.add_argument('--date', type=str, required=True, help='Дата (YYYY-MM-DD)')
    extract_parser.add_argument('--channel-id', type=int, required=True, help='ID канала')
    cert_path = str(pathlib.Path(__file__).parent / 'YandexInternalRootCA.pem')
    
    extract_parser.add_argument('--model', type=str, default='yandex', choices=['yandex', 'deepseek'], help='Модель LLM')
    extract_parser.add_argument('--verify', type=str, default=cert_path, help='Путь к CA-сертификату')
    
    # Команда topic_resumator
    resume_parser = subparsers.add_parser('resume', help='Проанализировать тему')
    resume_parser.add_argument('--topic-id', type=str, required=True, help='ID темы')
    resume_parser.add_argument('--model', type=str, default='yandex', choices=['yandex', 'deepseek'], help='Модель LLM')
    resume_parser.add_argument('--verify', type=str, default=cert_path, help='Путь к CA-сертификату')
    
    # Команда daily_digester
    daily_parser = subparsers.add_parser('daily', help='Создать дневной дайджест')
    daily_parser.add_argument('--date', type=str, required=True, help='Дата (YYYY-MM-DD)')
    daily_parser.add_argument('--channel-id', type=int, required=True, help='ID канала')
    daily_parser.add_argument('--model', type=str, default='yandex', choices=['yandex', 'deepseek'], help='Модель LLM')
    daily_parser.add_argument('--verify', type=str, default=cert_path, help='Путь к CA-сертификату')
    
    # Команда custom_date_digester
    custom_parser = subparsers.add_parser('custom', help='Создать дайджест за произвольный период')
    custom_parser.add_argument('--start-date', type=str, required=True, help='Начальная дата (YYYY-MM-DD)')
    custom_parser.add_argument('--end-date', type=str, required=True, help='Конечная дата (YYYY-MM-DD)')
    custom_parser.add_argument('--channel-id', type=int, required=True, help='ID канала')
    custom_parser.add_argument('--model', type=str, default='yandex', choices=['yandex', 'deepseek'], help='Модель LLM')
    custom_parser.add_argument('--verify', type=str, default=cert_path, help='Путь к CA-сертификату')
    
    # Команда init-data
    init_parser = subparsers.add_parser('init-data', help='Инициализировать тестовые данные в YT')
    init_parser.add_argument('--days-back-start', type=int, default=3, help='Количество дней назад от текущего момента для начала интервала (по умолчанию: 3)')
    init_parser.add_argument('--days-back-end', type=int, default=0, help='Количество дней назад от текущего момента для конца интервала (по умолчанию: 0 = сейчас)')
    
    # Команда dump
    dump_parser = subparsers.add_parser('dump', help='Выгрузить сообщения из Telegram чатов')
    dump_parser.add_argument('--chats', nargs='*', help='Список чатов (по умолчанию из переменной TG_CHATS)')
    dump_parser.add_argument('--output-table', help='Путь к таблице YT для сохранения (по умолчанию из YT_MESSAGES_TABLE)')
    dump_parser.add_argument('--days-back-start', type=int, default=3, help='Количество дней назад от текущего момента для начала интервала (по умолчанию: 3)')
    dump_parser.add_argument('--days-back-end', type=int, default=0, help='Количество дней назад от текущего момента для конца интервала (по умолчанию: 0 = сейчас)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'interval':
            start_date = dt.datetime.strptime(args.start, '%Y-%m-%d').date()
            end_date = dt.datetime.strptime(args.end, '%Y-%m-%d').date()
            orchestrator.process_interval(
                start=start_date,
                end=end_date,
                channel_id=args.channel_id,
                model=args.model,
                verify=args.verify
            )
            
        elif args.command == 'extract':
            date = dt.datetime.strptime(args.date, '%Y-%m-%d').date()
            topic_extractor.run_topic_extractor(
                date=date,
                channel_id=args.channel_id,
                model=args.model,
                verify=args.verify
            )
            
        elif args.command == 'resume':
            topic_resumator_chat.run_topic_resumator(
                topic_id=args.topic_id,
                model=args.model,
                verify=args.verify
            )
            
        elif args.command == 'daily':
            date = dt.datetime.strptime(args.date, '%Y-%m-%d').date()
            daily_digester.run_daily_digester(
                date=date,
                channel_id=args.channel_id,
                model=args.model,
                verify=args.verify
            )
            
        elif args.command == 'custom':
            start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d').date()
            custom_date_digester.run_custom_date_digester(
                start_date=start_date,
                end_date=end_date,
                channel_id=args.channel_id,
                model=args.model,
                verify=args.verify
            )
            
        elif args.command == 'init-data':
            init_test_data.init_test_data(
                days_back_start=args.days_back_start,
                days_back_end=args.days_back_end
            )
            
        elif args.command == 'dump':
            # Определяем список чатов
            chats = args.chats if args.chats else test_data.TG_CHATS
            
            # Определяем таблицу для сохранения
            output_table = args.output_table or os.getenv("YT_MESSAGES_TABLE", "//tmp/ia-nartov/hackathon/tg_raw_enriched")
            
            print(f"🔄 Выгружаем сообщения из {len(chats)} чатов...")
            for i, chat in enumerate(chats, 1):
                print(f"  {i}. {chat}")
            
            # Выгружаем сообщения
            df_messages = asyncio.run(tg_etl.dump_chats(
                chats,
                days_back_start=args.days_back_start,
                days_back_end=args.days_back_end
            ))
            print(f"✅ Получено сообщений: {len(df_messages)}")
            
            # Загружаем в YT
            print(f"📤 Загружаем в YT: {output_table}")
            tg_etl.upload_df_to_yt(
                df_messages,
                output_table,
                test_data.MSG_SCHEMA,
                overwrite=True
            )
            print("✅ Сообщения загружены в YT!")
            
    except Exception as e:
        print(f"Ошибка: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())