from __future__ import annotations
import json, logging, os, requests, time, pathlib
from typing import Dict, Any, List, Iterable, Union
from dotenv import load_dotenv, find_dotenv

# ─────────────────────────── 2 поддерживаемые модели ──────────────────────────
_MODELS: Dict[str, Dict[str, Any]] = {
    # «большой» aligned-quantized
    "yandex": {
        "endpoint": "https://api.eliza.yandex.net/internal/zeliboba/32b_aligned_quantized_202506/generative",
        "payload_model": None,        # поле model НЕ передаём
    },
    # communal deepseek-v3
    "deepseek": {
        "endpoint": "https://api.eliza.yandex.net/internal/zeliboba/communal-deepseek-v3-0324-in-yt/v1/chat/completions",
        "payload_model": "deepseek_v3",
    },
}


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ──────────────────────────────── API ─────────────────────────────────────────
def eliza_chat(
    messages: List[Dict[str, str]],
    *,
    model: str = "yandex",           # "yandex" | "deepseek"
    stream: bool = False,
    timeout: int = 180,              # Увеличен таймаут до 3 минут
    extra: Dict[str, Any] | None = None,
    token: str | None = None,
    verify: Union[bool, str] = True,
    max_retries: int = 3,            # Максимальное количество попыток
    retry_delay: float = 5.0,        # Задержка между попытками
) -> Union[Dict[str, Any], Iterable[Dict[str, Any]]]:
    """
    Отправляет chat-prompt в Eliza и возвращает:
        • dict  (если stream=False)
        • iterator[dict] (если stream=True)

    model : "yandex"  → 32b_aligned_quantized_202506 (без поля "model")
            "deepseek" → communal-deepseek-v3-0324-in-yt  (+ "model": "deepseek_v3")
    """

    if model not in _MODELS:
        raise ValueError(f"model must be 'yandex' or 'deepseek', got {model}")

    token = token or os.getenv("SOY_TOKEN")
    if not token:
        raise RuntimeError("SOY_TOKEN not set")

    cfg = _MODELS[model]
    payload: Dict[str, Any] = {"messages": messages}
    if cfg["payload_model"]:
        payload["model"] = cfg["payload_model"]
    if extra:
        payload.update(extra)

    log.info("sent prompt → %s", model)
    
    # Обработка пути к сертификату
    if isinstance(verify, str) and not os.path.isabs(verify):
        # Если путь уже содержит структуру проекта, используем его как есть
        if verify.startswith('junk/ia-nartov/hackathon_project/'):
            # Извлекаем только имя файла и ищем его в текущей директории
            cert_filename = verify.split('/')[-1]
            verify = os.path.abspath(cert_filename)
        else:
            # Если путь относительный, делаем его относительно рабочей директории
            verify = os.path.abspath(verify)
        log.info(f"Resolved certificate path: {verify}")
    
    # Retry логика
    last_exception = None
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                cfg["endpoint"],
                json=payload,
                headers={
                    "Authorization": f"OAuth {token}",
                    "Content-Type": "application/json",
                },
                timeout=timeout,
                stream=stream,
                verify=verify,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"{resp.status_code}: {resp.text}")

            if not stream:
                return resp.json()

            def _chunks() -> Iterable[Dict[str, Any]]:
                for line in resp.iter_lines(decode_unicode=True):
                    if line.startswith("data:"):
                        yield json.loads(line.removeprefix("data:").strip())

            return _chunks()
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                log.warning(f"Attempt {attempt + 1} failed with {type(e).__name__}: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Экспоненциальная задержка
            else:
                log.error(f"All {max_retries} attempts failed. Last error: {e}")
                raise
        except Exception as e:
            # Для других ошибок не делаем retry
            log.error(f"Non-retryable error: {e}")
            raise
    
    # Этот код не должен выполняться, но на всякий случай
    if last_exception:
        raise last_exception