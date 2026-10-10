"""Общая часть для сохранения данных: единый формат записи и базовый класс хранилища."""
from abc import ABC, abstractmethod
from datetime import datetime, timezone

RECORD_FIELDS = ("url", "title", "text", "links", "metadata", "crawled_at", "status_code", "content_type")


class StorageError(Exception):
    """Не удалось сохранить данные (диск, база и т.п.)."""


def make_record(page: dict, status_code: int | None = None, content_type: str | None = None,
                crawled_at: datetime | None = None) -> dict:
    """Превращает результат парсинга страницы в запись стандартного формата.

    Возвращает словарь с полями: url, title, text, links (список строк), metadata (словарь),
    crawled_at (datetime), status_code (число или None), content_type (строка).
    Лишние поля парсера (images, headings и т.д.) в запись не попадают.
    """
    if not isinstance(page, dict) or not isinstance(page.get("url"), str) or not page["url"]:
        raise ValueError("в данных страницы должен быть непустой url")

    links = []
    for link in page.get("links") or []:
        if isinstance(link, dict):                  # парсер может отдавать ссылки словарями
            link = link.get("url") or link.get("href")
        if isinstance(link, str) and link:
            links.append(link)

    metadata = page.get("metadata")
    return {
        "url": page["url"],
        "title": str(page.get("title") or ""),
        "text": str(page.get("text") or ""),
        "links": links,
        "metadata": dict(metadata) if isinstance(metadata, dict) else {},
        "crawled_at": crawled_at or datetime.now(timezone.utc),
        "status_code": status_code,
        "content_type": content_type or "",
    }


def validate_record(data: dict) -> None:
    """Проверяет, что запись содержит все стандартные поля. Иначе ValueError."""
    if not isinstance(data, dict):
        raise ValueError("запись должна быть словарём")
    missing = [name for name in RECORD_FIELDS if name not in data]
    if missing:
        raise ValueError("в записи нет полей: " + ", ".join(missing))
    if not isinstance(data["url"], str) or not data["url"]:
        raise ValueError("url должен быть непустой строкой")
    if not isinstance(data["links"], list):
        raise ValueError("links должен быть списком")
    if not isinstance(data["metadata"], dict):
        raise ValueError("metadata должен быть словарём")


def record_to_jsonable(data: dict) -> dict:
    """Копия записи, которую можно записать в JSON: дата превращается в строку ISO."""
    result = dict(data)
    if isinstance(result.get("crawled_at"), datetime):
        result["crawled_at"] = result["crawled_at"].isoformat()
    return result


class DataStorage(ABC):
    """Основа для всех хранилищ. Конкретные классы (JSON, CSV, SQLite) дописывают save и close."""

    def __init__(self):
        self.saved_count = 0                  # сколько записей сохранено

    @abstractmethod
    async def save(self, data: dict) -> None:
        """Сохраняет одну запись в стандартном формате."""

    async def save_many(self, items: list[dict]) -> None:
        """Сохраняет несколько записей. Хранилища могут переопределить для ускорения."""
        for item in items:
            await self.save(item)

    async def flush(self) -> None:
        """Дописывает на диск то, что ещё лежит в буфере. По умолчанию ничего не делает."""

    @abstractmethod
    async def close(self) -> None:
        """Дописывает буфер и закрывает файл или соединение."""

    def get_stats(self) -> dict:
        return {"saved": self.saved_count}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class MemoryStorage(DataStorage):
    """Хранилище в памяти. Нужно для проверок. fail_times=N: первые N сохранений падают с ошибкой."""

    def __init__(self, fail_times: int = 0):
        super().__init__()
        self.records: list[dict] = []
        self.fail_times = fail_times
        self.attempts = 0
        self.closed = False

    async def save(self, data: dict) -> None:
        validate_record(data)
        self.attempts += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise StorageError("имитация сбоя записи")
        self.records.append(dict(data))
        self.saved_count += 1

    async def close(self) -> None:
        self.closed = True