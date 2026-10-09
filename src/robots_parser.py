import asyncio
import logging
import time
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import aiohttp

logger = logging.getLogger("crawler")

MAX_ROBOTS_SIZE = 512 * 1024      # читаем не больше 512 КБ, как рекомендует стандарт


class RobotsParser:
    """Загружает и применяет правила из robots.txt.

    Для каждого сайта (схема + домен + порт) правила загружаются один раз и хранятся в кэше.
    - файла нет (4xx): ограничений нет, можно всё;
    - сайт недоступен (5xx, таймаут, обрыв): на время считаем, что нельзя ничего;
    - иначе применяются правила Disallow / Allow / Crawl-delay.
    """

    def __init__(self, fetch_agent: str = "AsyncCrawler/1.0", ttl: float = 3600.0,
                 error_ttl: float = 60.0, timeout: float = 10.0):
        self.fetch_agent = fetch_agent    # подпись, с которой скачиваем сам robots.txt
        self.ttl = ttl                    # сколько хранить удачно загруженные правила
        self.error_ttl = error_ttl        # сколько помнить, что сайт не отвечает
        self.timeout = timeout
        self._cache = {}                  # origin -> {"parser", "state", "status", "fetched_at", "ttl", ...}
        self._locks = {}                  # origin -> замок, чтобы не грузить один файл несколько раз
        self._last_origin = None
        self.blocked_urls = set()         # URL, которые запретил robots.txt

    @property
    def blocked_count(self) -> int:
        return len(self.blocked_urls)

    @staticmethod
    def get_origin(url: str) -> str:
        """'https://Example.com:8080/a/b?x=1' -> 'https://example.com:8080'"""
        parts = urlsplit(url)
        return f"{parts.scheme.lower()}://{parts.netloc.lower()}"

    def _fresh(self, origin: str) -> bool:
        entry = self._cache.get(origin)
        return entry is not None and time.monotonic() - entry["fetched_at"] < entry["ttl"]

    async def fetch_robots(self, base_url: str, session: aiohttp.ClientSession | None = None) -> dict:
        """Загружает robots.txt сайта (если ещё нет в кэше) и возвращает описание правил.

        base_url может быть любым адресом сайта. session можно передать готовую (из краулера);
        без неё будет создана временная.
        """
        origin = self.get_origin(base_url)
        self._last_origin = origin
        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:                              # остальные задачи подождут и возьмут из кэша
            if self._fresh(origin):
                return self._describe(origin, from_cache=True)
            await self._download(origin, session)
            return self._describe(origin, from_cache=False)

    async def _download(self, origin: str, session) -> None:
        robots_url = origin + "/robots.txt"
        parser = RobotFileParser()
        state, status, ttl = "rules", None, self.ttl
        own_session = session is None
        try:
            if own_session:
                session = aiohttp.ClientSession(headers={"User-Agent": self.fetch_agent})
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.get(robots_url, timeout=timeout) as response:
                status = response.status
                if status == 200:
                    text = (await response.text(errors="replace"))[:MAX_ROBOTS_SIZE]
                    parser.parse(text.splitlines())
                elif 400 <= status < 500 and status != 429:
                    state = "allow_all"               # файла нет: ограничений нет
                else:
                    state, ttl = "disallow_all", self.error_ttl      # 5xx и 429: подождём
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            state, ttl = "disallow_all", self.error_ttl
            logger.warning("Не удалось загрузить %s: %s: %s", robots_url, type(e).__name__, e)
        finally:
            if own_session and session is not None:
                await session.close()

        if state == "allow_all":
            parser.allow_all = True
        elif state == "disallow_all":
            parser.disallow_all = True
        self._cache[origin] = {"parser": parser, "state": state, "status": status,
                               "fetched_at": time.monotonic(), "ttl": ttl, "url": robots_url}
        logger.info("robots.txt %s -> %s (статус %s)", robots_url, state, status)

    def _describe(self, origin: str, from_cache: bool) -> dict:
        entry = self._cache[origin]
        parser = entry["parser"]
        return {
            "url": entry["url"],
            "status": entry["status"],
            "state": entry["state"],                 # rules / allow_all / disallow_all
            "crawl_delay": float(parser.crawl_delay("*") or 0),
            "sitemaps": parser.site_maps() or [],
            "from_cache": from_cache,
        }

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Можно ли заходить на url. Правила сайта должны быть загружены через fetch_robots."""
        origin = self.get_origin(url)
        entry = self._cache.get(origin)
        if entry is None:
            raise RuntimeError(f"robots.txt для {origin} ещё не загружен: сначала await fetch_robots(url)")
        allowed = entry["parser"].can_fetch(user_agent, url)
        if not allowed:
            if url not in self.blocked_urls:
                logger.warning("Запрещено robots.txt (%s): %s", entry["state"], url)
            self.blocked_urls.add(url)
        return allowed

    def get_crawl_delay(self, user_agent: str = "*", url: str | None = None) -> float:
        """Пауза между запросами из Crawl-delay для сайта этого url (0, если не указана).

        Без url берётся сайт, правила которого загружались последними.
        """
        origin = self.get_origin(url) if url else self._last_origin
        entry = self._cache.get(origin)
        if entry is None:
            return 0.0
        return float(entry["parser"].crawl_delay(user_agent) or 0)