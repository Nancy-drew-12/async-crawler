import asyncio
import logging 

import aiohttp

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10,
                 connect_timeout: float = 5.0,
                 read_timeout: float = 10.0, # значит, что серверу даём 10 секунд, чтобы он прислал данные, пока соединение уже установлено.
                 user_agent: str = "AsyncCrawler/1.0"):
        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(total=None,
                                             connect=connect_timeout,
                                             sock_read=read_timeout)
        self.user_agent = user_agent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session = None
        self.errors = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=self.max_concurrent,
                                             ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
            )
        return self._session

    async def close(self):
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def fetch_url(self, url: str) -> str | None:
        async with self._semaphore:
            session = await self._get_session()
            logger.info("Начало загрузки: %s", url)
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text(errors="replace")
            except aiohttp.ClientResponseError as e:
                return self._fail(url, f"HTTP {e.status} {e.message}")
            except asyncio.TimeoutError:
                return self._fail(url, "превышено время ожидания")
            except aiohttp.ClientError as e:
                return self._fail(url, f"{type(e).__name__}: {e}")

        self.errors.pop(url, None)
        logger.info("Готово: %s (%d символов)", url, len(text))
        return text

    def _fail(self, url: str, reason: str) -> None:
        self.errors[url] = reason
        logger.warning("Ошибка загрузки %s -> %s", url, reason)
        return None
    
    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        unique = list(dict.fromkeys(urls))
        bodies = await asyncio.gather(*(self.fetch_url(u) for u in unique))
        return {u: body for u, body in zip(unique, bodies) if body is not None}