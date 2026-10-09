import logging
import time
from collections import deque

from src.errors import CrawlerError

logger = logging.getLogger("crawler")

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"


class CircuitOpenError(CrawlerError):
    """Домен временно заблокирован предохранителем: слишком много ошибок подряд."""
    label = "домен заблокирован"


class CircuitBreaker:
    """Предохранитель для каждого домена, как автомат в электрощитке.

    closed (замкнут)    - всё работает, запросы идут; ведём счёт удач и ошибок;
    open (разомкнут)    - слишком много ошибок: запросы к домену сразу отклоняются;
    half_open (проверка) - прошло recovery_timeout секунд: пропускаем один пробный запрос.
                          Удача: домен снова работает. Ошибка: ещё раз блокируем.

    Блокировка включается, когда среди последних window_size запросов (но не меньше min_calls)
    доля ошибок достигла failure_rate.
    """

    def __init__(self, failure_rate: float = 0.5, min_calls: int = 5, window_size: int = 10,
                 recovery_timeout: float = 30.0, half_open_max_calls: int = 1, clock=time.monotonic):
        if isinstance(failure_rate, bool) or not isinstance(failure_rate, (int, float)) \
                or not 0 < failure_rate <= 1:
            raise ValueError("failure_rate должен быть числом больше 0 и не больше 1")
        for name, value in (("min_calls", min_calls), ("window_size", window_size),
                            ("half_open_max_calls", half_open_max_calls)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} должен быть целым числом не меньше 1")
        if min_calls > window_size:
            raise ValueError("min_calls не может быть больше window_size")
        if isinstance(recovery_timeout, bool) or not isinstance(recovery_timeout, (int, float)) \
                or recovery_timeout <= 0:
            raise ValueError("recovery_timeout должен быть положительным числом")

        self.failure_rate = failure_rate
        self.min_calls = min_calls
        self.window_size = window_size
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self._clock = clock                  # можно подменить в тестах, чтобы не ждать по-настоящему
        self._domains = {}                   # домен -> его состояние
        self.rejected = 0                    # сколько запросов отклонено
        self.opened_total = 0                # сколько раз предохранитель срабатывал

    def _get(self, domain: str) -> dict:
        if domain not in self._domains:
            self._domains[domain] = {"state": CLOSED, "results": deque(maxlen=self.window_size),
                                     "opened_at": 0.0, "probes": 0, "opened_times": 0}
        return self._domains[domain]

    # ---------- проверка перед запросом ----------

    def allow(self, domain: str) -> None:
        """Вызывать перед каждым запросом. Если домен заблокирован, выбрасывает CircuitOpenError."""
        d = self._get(domain)
        now = self._clock()
        if d["state"] == OPEN:
            remaining = d["opened_at"] + self.recovery_timeout - now
            if remaining > 0:
                self._reject(domain, f"домен {domain} временно заблокирован (ещё {remaining:.0f} с)", remaining)
            d["state"], d["probes"] = HALF_OPEN, 0
            logger.info("Домен %s: пробуем снова (проверочный запрос)", domain)
        if d["state"] == HALF_OPEN:
            if d["probes"] >= self.half_open_max_calls:
                self._reject(domain, f"домен {domain}: проверочный запрос уже идёт", None)
            d["probes"] += 1

    def _reject(self, domain: str, message: str, retry_after) -> None:
        self.rejected += 1
        raise CircuitOpenError(message, retry_after=retry_after)

    # ---------- результат запроса ----------

    def record_success(self, domain: str) -> None:
        d = self._get(domain)
        if d["state"] == HALF_OPEN:
            d["state"], d["probes"] = CLOSED, 0
            d["results"].clear()
            logger.info("Домен %s снова работает: предохранитель замкнут", domain)
        elif d["state"] == CLOSED:
            d["results"].append(True)

    def record_failure(self, domain: str) -> None:
        d = self._get(domain)
        if d["state"] == HALF_OPEN:                      # проверка не удалась: блокируем ещё раз
            self._open(domain, d, "проверочный запрос не удался")
        elif d["state"] == CLOSED:
            d["results"].append(False)
            failures = d["results"].count(False)
            total = len(d["results"])
            if total >= self.min_calls and failures / total >= self.failure_rate:
                self._open(domain, d, f"ошибок {failures} из {total}")
        # в состоянии open запросы уже не идут, поздние ответы игнорируем

    def release(self, domain: str) -> None:
        """Запрос завершился ни удачей, ни сбоем сайта (например, неверный адрес): освобождаем пробный слот."""
        d = self._get(domain)
        if d["state"] == HALF_OPEN and d["probes"] > 0:
            d["probes"] -= 1

    def _open(self, domain: str, d: dict, reason: str) -> None:
        d["state"], d["opened_at"], d["probes"] = OPEN, self._clock(), 0
        d["opened_times"] += 1
        self.opened_total += 1
        logger.warning("Домен %s временно заблокирован на %.0f с (%s)", domain, self.recovery_timeout, reason)

    # ---------- состояние ----------

    def state(self, domain: str) -> str:
        """closed, open или half_open. Показывает half_open, если пора пробовать снова."""
        d = self._get(domain)
        if d["state"] == OPEN and self._clock() - d["opened_at"] >= self.recovery_timeout:
            return HALF_OPEN
        return d["state"]

    def get_stats(self) -> dict:
        domains = {}
        for domain, d in self._domains.items():
            results = d["results"]
            domains[domain] = {
                "state": self.state(domain),
                "failure_rate": results.count(False) / len(results) if results else 0.0,
                "opened_times": d["opened_times"],
            }
        return {"domains": domains, "rejected": self.rejected, "opened_total": self.opened_total}