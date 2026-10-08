import asyncio
from contextlib import asynccontextmanager
from urllib.parse import urlparse


class SemaphoreManager:
    """Управляет лимитами одновременных запросов.

    - не больше max_concurrent запросов одновременно во всём краулере;
    - не больше max_per_domain запросов одновременно к одному домену;
    - считает, сколько задач активно прямо сейчас.
    """

    def __init__(self, max_concurrent: int = 10, max_per_domain: int = 3):
        for name, value in (("max_concurrent", max_concurrent), ("max_per_domain", max_per_domain)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} должен быть целым числом не меньше 1")

        self.max_concurrent = max_concurrent
        self.max_per_domain = max_per_domain
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains = {}      # домен -> его семафор
        self._active = {}       # домен -> сколько запросов выполняется сейчас
        self._peak = 0          # максимум одновременных запросов за всё время

    @staticmethod
    def get_domain(url: str) -> str:
        """'https://Example.com:8080/a' -> 'example.com:8080'"""
        return urlparse(url).netloc.lower()

    def _domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domains:
            self._domains[domain] = asyncio.Semaphore(self.max_per_domain)
        return self._domains[domain]

    @property
    def active_count(self) -> int:
        """Сколько запросов выполняется прямо сейчас."""
        return sum(self._active.values())

    @asynccontextmanager
    async def acquire(self, url: str):
        """Использование: async with manager.acquire(url): ...

        Внутри блока запрос считается активным; на выходе слот освобождается
        (даже если внутри случилась ошибка).
        """
        domain = self.get_domain(url)
        # сначала ждём место на этом домене, потом общее:
        # так задача, которая ждёт свой домен, не занимает общий слот зря
        async with self._domain_semaphore(domain):
            async with self._global:
                self._active[domain] = self._active.get(domain, 0) + 1
                self._peak = max(self._peak, self.active_count)
                try:
                    yield
                finally:
                    self._active[domain] -= 1
                    if self._active[domain] == 0:
                        del self._active[domain]

    def get_stats(self) -> dict:
        return {
            "active": self.active_count,
            "active_by_domain": dict(self._active),
            "peak_active": self._peak,
            "max_concurrent": self.max_concurrent,
            "max_per_domain": self.max_per_domain,
        }