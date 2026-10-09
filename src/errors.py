import asyncio

import aiohttp

# Статусы, после которых имеет смысл повторить запрос: сайт временно занят или просит подождать
TRANSIENT_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

# Ошибки aiohttp, после которых повторять бессмысленно: неверный адрес, сертификат, редирект не на http.
# Часть классов есть только в новых версиях aiohttp, поэтому берём те, что существуют.
PERMANENT_EXCEPTIONS = tuple(
    getattr(aiohttp, name)
    for name in ("InvalidURL", "NonHttpUrlClientError", "RedirectClientError", "ClientSSLError")
    if hasattr(aiohttp, name))


class CrawlerError(Exception):
    """Базовая ошибка краулера. Знает адрес, статус и сколько секунд сайт просит подождать."""

    label = "ошибка"      # название типа для логов и отчётов

    def __init__(self, message: str, url: str | None = None, status: int | None = None,
                 retry_after: float | None = None):
        super().__init__(message)
        self.message = message
        self.url = url
        self.status = status
        self.retry_after = retry_after       # из заголовка Retry-After, если сайт его прислал

    @property
    def kind(self) -> str:
        """Имя типа: 'TransientError', 'PermanentError', ..."""
        return type(self).__name__

    def __str__(self) -> str:
        return f"{self.kind}: {self.message}"


class TransientError(CrawlerError):
    """Временная ошибка: таймаут, 429, 503. Через некоторое время может пройти."""
    label = "временная"


class PermanentError(CrawlerError):
    """Постоянная ошибка: 404, 403, 401, неверный адрес. Повторять бессмысленно."""
    label = "постоянная"


class NetworkError(CrawlerError):
    """Сетевая ошибка: соединение отклонено, не найден домен, обрыв связи."""
    label = "сетевая"


class ParseError(CrawlerError):
    """Ошибка разбора: страница загрузилась, но содержимое не удалось обработать."""
    label = "разбор"


def classify_status(status: int, url: str | None = None, reason: str = "",
                    retry_after: float | None = None) -> CrawlerError:
    """HTTP-статус -> ошибка нужного типа (для статусов 400 и выше)."""
    message = f"HTTP {status} {reason}".strip()
    error_class = TransientError if status in TRANSIENT_STATUSES else PermanentError
    return error_class(message, url, status, retry_after)


def classify_exception(exc: BaseException, url: str | None = None) -> CrawlerError:
    """Любое исключение -> ошибка нужного типа.

    Порядок проверок важен: сначала более частные случаи, потом общие.
    """
    if isinstance(exc, CrawlerError):                      # уже классифицирована
        if exc.url is None:
            exc.url = url
        return exc
    if isinstance(exc, asyncio.TimeoutError):              # в том числе таймауты aiohttp
        return TransientError("превышено время ожидания", url)
    if isinstance(exc, aiohttp.ClientResponseError):       # ответ с кодом 4xx или 5xx
        return classify_status(exc.status, url, exc.message)
    if isinstance(exc, PERMANENT_EXCEPTIONS):              # неверный адрес, сертификат
        return PermanentError(f"{type(exc).__name__}: {exc}", url)
    if isinstance(exc, (aiohttp.ClientError, OSError)):    # отказ соединения, DNS, обрыв
        return NetworkError(f"{type(exc).__name__}: {exc}", url)
    return PermanentError(f"{type(exc).__name__}: {exc}", url)    # неизвестное: не повторяем