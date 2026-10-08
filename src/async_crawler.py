import asyncio
import logging
import time

import aiohttp

from src.crawler_queue import CrawlerQueue
from src.html_parser import HTMLParser
from src.semaphore_manager import SemaphoreManager
from src.url_filter import URLFilter, normalize_domain, normalize_url

logger = logging.getLogger("crawler")

DEFAULT_USER_AGENT = "AsyncCrawler/0.1 (учебный проект)"


class AsyncCrawler:
    """Базовый асинхронный HTTP-клиент для параллельной загрузки страниц.

    - одна общая сессия aiohttp.ClientSession с пулом соединений (connection pooling);
    - не больше max_concurrent запросов одновременно (семафор);
    - таймауты на подключение и на чтение ответа;
    - ошибки не роняют программу: они логируются и сохраняются в self.errors.
    """

    def __init__(self, max_concurrent: int = 10, connect_timeout: float = 5.0,
                 read_timeout: float = 10.0, user_agent: str = DEFAULT_USER_AGENT,
                 max_depth: int = 2, max_per_domain: int = 3):
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent < 1:
            raise ValueError("max_concurrent должен быть целым числом не меньше 1")
        for name, value in (("connect_timeout", connect_timeout), ("read_timeout", read_timeout)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{name} должен быть положительным числом")

        self.max_concurrent = max_concurrent
        self.user_agent = user_agent
        # total=None: общего лимита нет, но подключение и ожидание данных ограничены
        self.timeout = aiohttp.ClientTimeout(total=None, connect=connect_timeout, sock_read=read_timeout)
        self._headers = {"User-Agent": user_agent}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session = None   # создаётся при первом запросе (уже внутри работающего цикла событий)
        self._closed = False
        self.errors = {}       # url -> описание последней ошибки
        self._parser = HTMLParser()

        # --- День 3: обход сайта ---
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth должен быть целым числом не меньше 0")
        self.max_depth = max_depth
        self._limits = SemaphoreManager(max_concurrent, max_per_domain)
        self.queue = None
        self.visited_urls = set()      # куда уже заходили
        self.failed_urls = {}          # url -> причина ошибки
        self.processed_urls = {}       # url -> результат парсинга
        self._crawl_started = None

    # ---------- сессия ----------

    def _get_session(self) -> aiohttp.ClientSession:
        if self._closed:
            raise RuntimeError("Краулер уже закрыт")
        if self._session is None:
            connector = aiohttp.TCPConnector(limit=self.max_concurrent, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                connector=connector, timeout=self.timeout, headers=self._headers)
        return self._session

    async def close(self):
        """Закрывает сессию и все соединения. Можно вызывать повторно."""
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    # ---------- загрузка ----------

    async def fetch_url(self, url: str) -> str | None:
        """Загружает одну страницу и возвращает её текст.

        При сетевой ошибке, таймауте или HTTP-ошибке (404, 500...) не падает:
        пишет предупреждение в лог, сохраняет причину в self.errors[url] и возвращает None.
        """
        session = self._get_session()
        async with self._semaphore:
            logger.info("Начало загрузки: %s", url)
            started = time.perf_counter()
            try:
                async with session.get(url) as response:
                    response.raise_for_status()                    # 4xx и 5xx -> ClientResponseError
                    text = await response.text(errors="replace")
            except aiohttp.ClientResponseError as e:               # HTTP-ошибки (подкласс ClientError, поэтому первым)
                return self._fail(url, f"{type(e).__name__}: HTTP {e.status} {e.message}")
            except asyncio.TimeoutError as e:                      # таймаут (раньше ClientError: ServerTimeoutError - оба)
                return self._fail(url, f"{type(e).__name__}: превышено время ожидания")
            except aiohttp.ClientError as e:                       # остальные сетевые ошибки
                return self._fail(url, f"{type(e).__name__}: {e}")

        self.errors.pop(url, None)
        logger.info("Успешно: %s (%d символов, %.2f с)", url, len(text), time.perf_counter() - started)
        return text

    def _fail(self, url: str, reason: str) -> None:
        self.errors[url] = reason
        logger.warning("Ошибка загрузки %s -> %s", url, reason)
        return None

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Параллельно загружает список URL.

        Возвращает {url: текст} только для успешных загрузок; причины неудач лежат в self.errors.
        Повторяющиеся URL загружаются один раз.
        """
        if not isinstance(urls, (list, tuple)) or not all(isinstance(u, str) for u in urls):
            raise TypeError("urls должен быть списком строк")
        unique = list(dict.fromkeys(urls))   # убираем дубли, сохраняя порядок
        bodies = await asyncio.gather(*(self.fetch_url(u) for u in unique))
        return {u: body for u, body in zip(unique, bodies) if body is not None}

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        if html is None:
            result = await self._parser.parse_html("", url)
            result["errors"].append(f"загрузка: {self.errors.get(url, 'неизвестная ошибка')}")
            return result
        return await self._parser.parse_html(html, url)

    # ---------- обход сайта ----------

    async def crawl(self, start_urls: list[str], max_pages: int = 100, same_domain_only: bool = False,
                    exclude_patterns: list[str] | None = None,
                    include_patterns: list[str] | None = None,
                    progress: bool = True, progress_interval: float = 1.0) -> dict[str, dict]:
        """Обходит сайт, начиная со start_urls. Возвращает {url: результат парсинга}."""
        if not isinstance(start_urls, (list, tuple)) or not all(isinstance(u, str) for u in start_urls):
            raise TypeError("start_urls должен быть списком строк")
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
            raise ValueError("max_pages должен быть целым числом не меньше 1")

        # чистое состояние для нового обхода
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.queue = CrawlerQueue()
        self._depths = {}                    # url -> на каком шаге от старта найден
        self._max_pages = max_pages
        self._crawl_started = time.perf_counter()
        self._filter = URLFilter(
            allowed_domains={normalize_domain(u) for u in start_urls} if same_domain_only else None,
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns)

        for url in start_urls:
            self._enqueue(url, depth=0)

        # несколько рабочих берут URL из одной общей очереди
        workers = [asyncio.create_task(self._crawl_worker()) for _ in range(self._limits.max_concurrent)]
        reporter = asyncio.create_task(self._progress_loop(progress_interval)) if progress else None
        try:
            await asyncio.gather(*workers)
        finally:
            if reporter is not None:
                reporter.cancel()                                   # останавливаем вывод прогресса
                await asyncio.gather(reporter, return_exceptions=True)
                print(self._progress_line() + "  | готово")
        return self.processed_urls

    async def _progress_loop(self, interval: float):
        """Раз в interval секунд печатает строку прогресса."""
        while True:
            await asyncio.sleep(interval)
            print(self._progress_line(), flush=True)

    def _progress_line(self) -> str:
        s = self.get_crawl_stats()
        return (f"[{s['elapsed']:5.1f} с] обработано: {s['processed']} | в очереди: {s['queued']} | "
                f"в работе: {s['in_progress']} | ошибок: {s['failed']} | {s['pages_per_sec']:.1f} стр/с")

    def _enqueue(self, url, depth: int) -> bool:
        """Кладёт ссылку в очередь, если она проходит все проверки."""
        if not isinstance(url, str) or not url:
            return False
        url = normalize_url(url)
        if url in self.visited_urls or not self._filter.is_allowed(url):
            return False
        if self.queue.get_stats()["total_added"] >= self._max_pages:
            return False                     # лимит страниц: больше в очередь не берём
        # приоритет -depth: страницы ближе к старту обрабатываются раньше
        if self.queue.add_url(url, priority=-depth):
            self._depths[url] = depth
            return True
        return False

    async def _crawl_worker(self):
        while (url := await self.queue.get_next()) is not None:
            self.visited_urls.add(url)
            try:
                async with self._limits.acquire(url):
                    page = await self.fetch_and_parse(url)
            except Exception as e:           # неожиданная ошибка не должна остановить весь обход
                self._record_failure(url, f"{type(e).__name__}: {e}")
                continue

            if url in self.errors:           # страницу не удалось загрузить
                self._record_failure(url, self.errors[url])
                continue

            self.processed_urls[url] = page
            self.queue.mark_processed(url)

            depth = self._depths[url]
            if depth < self.max_depth:       # глубже max_depth ссылки не собираем
                for link in page.get("links", []):
                    self._enqueue(self._link_url(link), depth + 1)

    def _record_failure(self, url: str, reason: str) -> None:
        self.failed_urls[url] = reason
        self.queue.mark_failed(url, reason)

    @staticmethod
    def _link_url(link):
        """Парсер может отдавать ссылки строками или словарями; берём сам адрес."""
        if isinstance(link, dict):
            return link.get("url") or link.get("href")
        return link

    def get_crawl_stats(self) -> dict:
        """Сводка по текущему обходу."""
        elapsed = time.perf_counter() - self._crawl_started if self._crawl_started else 0.0
        q = self.queue.get_stats() if self.queue else {"processed": 0, "failed": 0, "queued": 0, "in_progress": 0}
        done = q["processed"] + q["failed"]
        return {
            "processed": q["processed"],
            "failed": q["failed"],
            "queued": q["queued"],
            "in_progress": q["in_progress"],
            "elapsed": elapsed,
            "pages_per_sec": done / elapsed if elapsed > 0 else 0.0,
        }