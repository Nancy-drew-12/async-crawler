import asyncio
import logging
import time

from src.errors import CrawlerError, NetworkError, PermanentError, TransientError, classify_exception
from src.politeness import backoff_delay

logger = logging.getLogger("crawler")


class RetryStrategy:
    """Выполняет асинхронную функцию и повторяет её при подходящих ошибках.

    - retry_on: какие типы ошибок повторять (по умолчанию TransientError и NetworkError);
    - max_retries: сколько повторов разрешено, если для ошибки нет особого правила;
    - backoff_factor: во сколько раз растёт пауза: base_delay, base_delay * factor, ...;
    - max_retries_by_type / max_retries_by_status: свой лимит повторов для типа ошибки или статуса;
    - backoff_by_type: свой множитель роста паузы для типа ошибки;
    - status_delay_multiplier: пауза для статуса умножается (по умолчанию 429 ждёт втрое дольше);
    - timeout_growth: во сколько раз растёт таймаут от попытки к попытке;
    - on_retry: необязательная функция (ошибка, пауза, номер повтора), её вызывают перед каждым ожиданием.
    Если сайт сам просит подождать (Retry-After), берётся его значение.
    """

    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0, retry_on: list | None = None,
                 base_delay: float = 1.0, max_delay: float = 30.0, jitter: float = 0.25,
                 max_retries_by_type: dict | None = None, max_retries_by_status: dict | None = None,
                 backoff_by_type: dict | None = None, status_delay_multiplier: dict | None = None,
                 timeout_growth: float = 1.5, max_timeout: float = 60.0, on_retry=None):
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries должен быть целым числом не меньше 0")
        for name, value in (("backoff_factor", backoff_factor), ("base_delay", base_delay),
                            ("max_delay", max_delay), ("max_timeout", max_timeout)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{name} должен быть положительным числом")
        if isinstance(jitter, bool) or not isinstance(jitter, (int, float)) or jitter < 0:
            raise ValueError("jitter должен быть числом не меньше 0")
        if isinstance(timeout_growth, bool) or not isinstance(timeout_growth, (int, float)) or timeout_growth < 1:
            raise ValueError("timeout_growth должен быть числом не меньше 1")

        self.retry_on = (TransientError, NetworkError) if retry_on is None else tuple(retry_on)
        if not all(isinstance(c, type) and issubclass(c, CrawlerError) for c in self.retry_on):
            raise TypeError("retry_on должен содержать классы ошибок краулера (TransientError и т.п.)")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.timeout_growth = timeout_growth
        self.max_timeout = max_timeout
        self.on_retry = on_retry
        self.max_retries_by_type = dict(max_retries_by_type or {})
        # 500 повторяем ограниченное число раз: чаще всего это ошибка на странице, а не временный сбой
        self.max_retries_by_status = {500: 2} if max_retries_by_status is None else dict(max_retries_by_status)
        self.backoff_by_type = dict(backoff_by_type or {})
        self.status_delay_multiplier = {429: 3.0} if status_delay_multiplier is None \
            else dict(status_delay_multiplier)
        self._reset_stats()

    # ---------- правила ----------

    def should_retry(self, error: CrawlerError) -> bool:
        """Относится ли эта ошибка к тем, что стоит повторять."""
        return isinstance(error, self.retry_on)

    def retry_limit(self, error: CrawlerError) -> int:
        """Сколько повторов разрешено для этой ошибки: правило статуса, потом типа, потом общее."""
        if error.status in self.max_retries_by_status:
            return self.max_retries_by_status[error.status]
        for cls in type(error).__mro__:                   # от конкретного типа к общему
            if cls in self.max_retries_by_type:
                return self.max_retries_by_type[cls]
        return self.max_retries

    def delay_for(self, error: CrawlerError, retry_number: int) -> float:
        """Пауза перед повтором номер retry_number (0 для первого повтора)."""
        if error.retry_after is not None:                 # сайт сам сказал, сколько ждать
            return min(error.retry_after, self.max_delay)
        factor = self.backoff_factor
        for cls in type(error).__mro__:
            if cls in self.backoff_by_type:
                factor = self.backoff_by_type[cls]
                break
        base = self.base_delay * self.status_delay_multiplier.get(error.status, 1.0)
        return backoff_delay(retry_number, base=base, factor=factor,
                             max_delay=self.max_delay, jitter=self.jitter)

    def timeout_for(self, attempt: int, base_timeout: float) -> float:
        """Таймаут для попытки номер attempt (0 для первой): растёт, но не выше max_timeout."""
        return min(max(base_timeout, self.max_timeout), base_timeout * self.timeout_growth ** attempt)

    # ---------- выполнение ----------

    async def execute_with_retry(self, func, *args, **kwargs):
        """Вызывает await func(*args, **kwargs); при подходящей ошибке ждёт и вызывает снова.

        Возвращает результат функции. Если не получилось, выбрасывает последнюю ошибку
        (уже классифицированную: TransientError, PermanentError, ...).
        """
        url = args[0] if args and isinstance(args[0], str) else kwargs.get("url")
        self._calls += 1
        retries = 0
        while True:
            try:
                result = await func(*args, **kwargs)
            except Exception as exc:
                error = classify_exception(exc, url)
                self._errors_by_type[error.kind] = self._errors_by_type.get(error.kind, 0) + 1
                limit = self.retry_limit(error)
                if not self.should_retry(error) or retries >= limit:
                    self._finish_failed(error, retries, url)
                    raise error from exc
                delay = self.delay_for(error, retries)
                logger.warning("Ошибка %s (%s) на %s: попытка %d из %d, повтор через %.1f с",
                               error.kind, error.message, url, retries + 1, limit + 1, delay)
                self._retries += 1
                self._retry_delay_total += delay
                retries += 1
                if self.on_retry is not None:
                    self.on_retry(error, delay, retries)
                await asyncio.sleep(delay)
            else:
                if retries:
                    self._successful_retries += 1
                    logger.info("Успех после %d повтор(ов): %s", retries, url)
                return result

    def _finish_failed(self, error: CrawlerError, retries: int, url) -> None:
        if retries:
            self._failed_after_retries += 1
        if isinstance(error, PermanentError) and url is not None and url not in self._permanent_urls:
            self._permanent_urls.append(url)
        if self.should_retry(error):
            logger.error("Ошибка %s на %s: повторы закончились после %d (%s)",
                         error.kind, url, retries, error.message)
        else:
            logger.error("Ошибка %s на %s: повторять не нужно (%s)", error.kind, url, error.message)

    # ---------- статистика ----------

    def _reset_stats(self) -> None:
        self._calls = 0
        self._errors_by_type = {}
        self._retries = 0
        self._retry_delay_total = 0.0
        self._successful_retries = 0
        self._failed_after_retries = 0
        self._permanent_urls = []

    def get_stats(self) -> dict:
        """Сколько было ошибок каждого типа, повторов, удачных повторов и среднее ожидание перед повтором."""
        return {
            "calls": self._calls,
            "errors_by_type": dict(self._errors_by_type),
            "total_errors": sum(self._errors_by_type.values()),
            "retries": self._retries,
            "successful_retries": self._successful_retries,
            "failed_after_retries": self._failed_after_retries,
            "avg_retry_delay": self._retry_delay_total / self._retries if self._retries else 0.0,
            "permanent_urls": list(self._permanent_urls),
        }