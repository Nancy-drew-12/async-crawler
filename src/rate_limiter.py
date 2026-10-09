import asyncio
import random
import time
from collections import deque


class RateLimiter:
    """Ограничивает, как часто можно отправлять запросы.

    - requests_per_second: не чаще стольких запросов в секунду (1.0 = раз в секунду);
    - per_domain=True: у каждого сайта свой счётчик, False: один общий на все сайты;
    - min_delay: минимальная пауза между запросами в секундах;
    - jitter: к каждой паузе добавляется случайное время от 0 до jitter секунд.
    Реальная пауза между запросами = max(1 / requests_per_second, min_delay) + случайная добавка.
    """

    _GLOBAL = "__global__"

    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True,
                 min_delay: float = 0.0, jitter: float = 0.0):
        if isinstance(requests_per_second, bool) or not isinstance(requests_per_second, (int, float)) \
                or requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть положительным числом")
        for name, value in (("min_delay", min_delay), ("jitter", jitter)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
                raise ValueError(f"{name} должен быть числом не меньше 0")

        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter

        self._next_allowed = {}      # ключ -> момент, раньше которого следующий запрос нельзя
        self._extra_delay = {}       # домен -> своя минимальная пауза (например, из robots.txt)
        self._last_grant = {}        # ключ -> момент последнего разрешённого запроса
        self._intervals = []         # реальные паузы между соседними запросами
        self._waits = []             # сколько каждый запрос простоял в ожидании
        self._recent = deque()       # моменты последних запросов (для текущей скорости)
        self._total = 0

    def _key(self, domain):
        return domain if self.per_domain and domain else self._GLOBAL

    def set_domain_delay(self, domain: str, seconds: float) -> None:
        """Задаёт для домена свою минимальную паузу (например, Crawl-delay из robots.txt)."""
        if seconds > 0:
            key = self._key(domain)
            self._extra_delay[key] = max(seconds, self._extra_delay.get(key, 0.0))

    def delay_domain(self, domain: str, seconds: float) -> None:
        """Откладывает все следующие запросы к домену на seconds секунд (например, после ответа 429)."""
        if seconds > 0:
            key = self._key(domain)
            self._next_allowed[key] = max(self._next_allowed.get(key, 0.0), time.monotonic() + seconds)

    def interval_for(self, domain=None) -> float:
        """Минимальная пауза между запросами для этого домена, без случайной добавки."""
        key = self._key(domain)
        return max(1 / self.requests_per_second, self.min_delay, self._extra_delay.get(key, 0.0))

    async def acquire(self, domain: str = None) -> None:
        """Ждёт, пока запрос можно отправлять. Вызывать перед каждым запросом."""
        key = self._key(domain)
        now = time.monotonic()
        # Занимаем свой «слот» сразу, без await: так два запроса не получат одно и то же время
        start = max(now, self._next_allowed.get(key, 0.0))
        pause = self.interval_for(domain) + (random.uniform(0, self.jitter) if self.jitter else 0.0)
        self._next_allowed[key] = start + pause

        if start > now:
            await asyncio.sleep(start - now)

        granted = time.monotonic()
        if key in self._last_grant:
            self._intervals.append(granted - self._last_grant[key])
        self._last_grant[key] = granted
        self._waits.append(granted - now)
        self._recent.append(granted)
        self._total += 1

    def get_stats(self) -> dict:
        """Статистика: сколько запросов, текущая скорость, средняя пауза и среднее ожидание."""
        now = time.monotonic()
        while self._recent and now - self._recent[0] > 5.0:     # скорость считаем за последние 5 секунд
            self._recent.popleft()
        span = self._recent[-1] - self._recent[0] if len(self._recent) > 1 else 0.0
        return {
            "requests": self._total,
            "current_rps": (len(self._recent) - 1) / span if span > 0 else 0.0,
            "avg_delay": sum(self._intervals) / len(self._intervals) if self._intervals else 0.0,
            "avg_wait": sum(self._waits) / len(self._waits) if self._waits else 0.0,
        }