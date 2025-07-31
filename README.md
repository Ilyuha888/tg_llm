# Telegram Digester

Система анализа и суммаризации Telegram-переписок с использованием LLM для создания дайджестов.

## Описание

Проект создан для хакатона HR Tech 2025 и представляет собой комплексную систему обработки корпоративных чатов:

1. **Извлечение данных** из Telegram-каналов
2. **Анализ тем** обсуждений с помощью LLM
3. **Создание дайджестов** (дневных и недельных)
4. **Хранение результатов** в YTsaurus

## Архитектура

### Основные компоненты:

- **`tg_etl.py`** - ETL для Telegram (выгрузка сообщений, загрузка в YT)
- **`topic_extractor.py`** - извлечение тем из сообщений за день
- **`topic_resumator_chat.py`** - анализ каждой темы с помощью LLM
- **`daily_digester.py`** - создание дневного дайджеста топ-5 тем
- **`weekly_digester.py`** - создание недельного дайджеста
- **`eliza_client.py`** - клиент для работы с внутренним LLM API
- **`orchestrator.py`** - координация всего процесса

### Схема данных в YTsaurus:

```
//tmp/ia-nartov/hackathon/
├── tg_raw_enriched     # сырые сообщения
├── tg_users           # справочник пользователей
├── tg_chats           # справочник чатов
├── daily_topics       # темы по дням
├── topic_analysis     # анализ тем
├── daily_digest       # дневные дайджесты
└── weekly_digest      # недельные дайджесты
```

## Установка и запуск

### 1. Настройка окружения

```bash
# Скопируйте пример конфигурации
cp .env.example .env

# Отредактируйте .env файл, заполнив ваши токены и настройки
```

### 2. Сборка в Аркадии:

```bash
cd junk/ia-nartov/hackathon_project
ya make
```

### 3. Локально (для разработки):

```bash
pip install -r requirements.txt
```

### Переменные окружения:

```bash
# Telegram API (получить на https://my.telegram.org/apps)
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash

# YTsaurus
YT_TOKEN=your_yt_token
YT_CLUSTER=hahn

# YQL
YQL_TOKEN=your_yql_token

# LLM API
SOY_TOKEN=your_soy_token

# Telegram чаты для выгрузки (разделенные запятыми)
TG_CHATS=https://t.me/+lLMAq7bH0UE4ODhi,https://t.me/+WjMO5LI6yNxjMDcy,https://t.me/+dHbsTOh4zQgzY2Ji,https://t.me/+sn_o2pmGHUAzNTY6

# Пути к таблицам YT (настраиваемые)
YT_ROOT=//tmp/ia-nartov/hackathon
YT_USERS_TABLE=//tmp/ia-nartov/hackathon/tg_users
YT_CHATS_TABLE=//tmp/ia-nartov/hackathon/tg_chats
YT_MESSAGES_TABLE=//tmp/ia-nartov/hackathon/tg_raw_enriched
```

**Настройка чатов и таблиц:**
- **TG_API_ID/TG_API_HASH**: получите на https://my.telegram.org/apps
- **TG_CHATS**: список чатов для выгрузки (через запятую)
- **YT_*_TABLE**: пути к таблицам YTsaurus для разных типов данных
- Все настройки можно редактировать в файле `.env`

**Важно**:
- В файле `.env` уже есть тестовые значения. Для работы с реальными чатами замените `TG_API_ID` и `TG_API_HASH` на ваши реальные ключи.
- Сертификат `YandexInternalRootCA.pem` автоматически находится в директории проекта.
- LLM API имеет встроенную retry-логику с увеличенным таймаутом (3 минуты) для обработки долгих запросов.

## Использование

### Обработка интервала дат:
```bash
./telegram_digester interval --start 2025-01-20 --end 2025-01-25
```

### Извлечение тем за день:
```bash
./telegram_digester extract --date 2025-01-21 --channel-id 4963882870
```

### Анализ конкретной темы:
```bash
./telegram_digester resume --topic-id "4963882870_2025-01-21_1"
```

### Создание дневного дайджеста:
```bash
./telegram_digester daily --date 2025-01-21 --channel-id 4963882870
```

### Создание недельного дайджеста:
```bash
./telegram_digester weekly --start-date 2025-01-18 --end-date 2025-01-25 --channel-id 4963882870
```

## Примеры данных

### Тестовые каналы:
- `4963882870` - "'Что я пропустил?': дайджест | Хакатон HR Tech 2025"
- `4620970541` - "Чат обсуждения проекта доставки айсбергов в Африку"
- `2804073375` - "Тестовая группа 2" (чат с разработкой)
- `2417575520` - "Тестовая группа 1" (чат менеджеров)

### Участники:
- Сергей Кольцов (Аналитик)
- Никита Щербинин (Стажер-менеджер)
- Нартов Илья (Аналитик)
- Рома Черницын (CPO)
- И другие...

## Технические детали

### LLM модели:
- **yandex** - 32b_aligned_quantized_202506 (рекомендуется)
- **deepseek** - communal-deepseek-v3-0324-in-yt

### Особенности:
- Работа с московским часовым поясом (день = 04:00-04:00 MSK)
- Поддержка ответов на сообщения и реакций
- Автоматическое определение ролей участников
- Upsert-логика для обновления данных
- Обработка ошибок и логирование

## Структура проекта

```
junk/ia-nartov/hackathon_project/
├── ya.make                    # конфигурация сборки
├── __main__.py               # точка входа CLI
├── orchestrator.py           # координатор процессов
├── tg_etl.py                # ETL для Telegram
├── topic_extractor.py       # извлечение тем
├── topic_resumator_chat.py  # анализ тем
├── daily_digester.py        # дневные дайджесты
├── weekly_digester.py       # недельные дайджесты
├── eliza_client.py          # LLM клиент
├── requirements.txt         # зависимости Python
├── YandexInternalRootCA.pem # корпоративный сертификат
└── README.md               # этот файл
```

## Авторы

Проект создан командой хакатона HR Tech 2025:
- ia-nartov (Нартов Илья)

## Лицензия

Внутренний проект Яндекса.