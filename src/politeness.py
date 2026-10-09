import itertools
import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# HTTP-статусы, после которых имеет смысл повторить запрос через некоторое время
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def backoff_delay(attempt: int, base: float = 1.0, factor: float = 2.0,
                  max_delay: float = 30.0, jitter: float = 0.25) -> float:
    """Пауза перед повторной попыткой (экспоненциальный backoff).

    attempt = 0 для первого повтора: base, затем base * factor, base * factor**2, ...
    Пауза не больше max_delay. jitter: добавка до jitter * 100% случайной длины,
    чтобы повторы разных запросов не происходили в один и тот же момент.
    """
    delay = min(max_delay, base * factor ** attempt)
    if jitter:
        delay *= 1 + random.uniform(0, jitter)
    return delay


def parse_retry_after(value: str | None) -> float | None:
    """Заголовок Retry-After: число секунд ('5') или дата. Возвращает секунды или None."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


class UserAgentRotator:
    """Выдаёт User-Agent по кругу. С одним значением просто всегда возвращает его."""

    def __init__(self, agents):
        if isinstance(agents, str):
            agents = [agents]
        agents = list(agents)
        if not agents or not all(isinstance(a, str) and a.strip() for a in agents):
            raise ValueError("нужен непустой список непустых строк")
        self.agents = agents
        self._cycle = itertools.cycle(agents)

    def next(self) -> str:
        return next(self._cycle)