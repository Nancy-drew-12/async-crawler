import asyncio
import logging
import time

import aiohttp

from src.circuit_breaker import CircuitBreaker
from src.crawler_queue import CrawlerQueue
from src.errors import (CrawlerError, NetworkError, ParseError, PermanentError, TransientError,
                        classify_exception, classify_status)
from src.html_parser import HTMLParser
from src.politeness import UserAgentRotator, parse_retry_after
from src.rate_limiter import RateLimiter
from src.retry import RetryStrategy
from src.robots_parser import RobotsParser
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
                 max_depth: int = 2, max_per_domain: int = 3,
                 requests_per_second: float | None = None, rate_per_domain: bool = True,
                 min_delay: float = 0.0, jitter: float = 0.0,
                 respect_robots: bool = False, user_agents: list[str] | None = None,
                 max_retries: int = 2, backoff_base: float = 1.0, backoff_max: float = 30.0,
                 total_timeout: float | None = None, retry_strategy: RetryStrategy | None = None,
                 circuit_breaker: CircuitBreaker | None = None):
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent < 1:
            raise ValueError("max_concurrent должен быть целым числом не меньше 1")
        for name, value in (("connect_timeout", connect_timeout), ("read_timeout", read_timeout)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{name} должен быть положительным числом")

        self.max_concurrent = max_concurrent
        self.user_agent = user_agent
        if total_timeout is not None and (isinstance(total_timeout, bool)
                                          or not isinstance(total_timeout, (int, float)) or total_timeout <= 0):
            raise ValueError("total_timeout должен быть положительным числом или None")
        # total_timeout=None: общего лимита нет, но подключение и ожидание данных ограничены
        self.timeout = aiohttp.ClientTimeout(total=total_timeout, connect=connect_timeout, sock_read=read_timeout)
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

        # --- День 4: вежливость ---
        if requests_per_second is not None and (
                isinstance(requests_per_second, bool) or not isinstance(requests_per_second, (int, float))
                or requests_per_second <= 0):
            raise ValueError("requests_per_second должен быть положительным числом или None")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries должен быть целым числом не меньше 0")
        for name, value in (("backoff_base", backoff_base), ("backoff_max", backoff_max)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{name} должен быть положительным числом")
        # без ограничения скорости лимитер всё равно работает: он собирает статистику
        self._rate_limiter = RateLimiter(requests_per_second or float("inf"), per_domain=rate_per_domain,
                                         min_delay=min_delay, jitter=jitter)
        self.respect_robots = bool(respect_robots)
        self.robots = RobotsParser(fetch_agent=user_agent)
        self._rotator = UserAgentRotator(user_agents or [user_agent])

        # --- День 5: ошибки и повторы ---
        # без своей стратегии создаём обычную из параметров max_retries, backoff_base, backoff_max
        self.retry_strategy = retry_strategy or RetryStrategy(
            max_retries=max_retries, base_delay=backoff_base, max_delay=backoff_max)
        if self.retry_strategy.on_retry is None:
            self.retry_strategy.on_retry = self._on_retry
        self.circuit_breaker = circuit_breaker      # None = предохранитель выключен
        self.error_details = {}                     # url -> ошибка (тип, статус, текст)

    @property
    def retries_done(self) -> int:
        """Сколько раз запрос повторяли после ошибки."""
        return self.retry_strategy.get_stats()["retries"]

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

        Перед запросом: проверка robots.txt (если включена). Сам запрос идёт через RetryStrategy:
        при временных и сетевых ошибках он повторяется с нарастающей паузой, при постоянных (404, 403)
        сразу заканчивается. При неудаче не падает: пишет предупреждение в лог, сохраняет ошибку
        в self.errors[url] (текст) и self.error_details[url] (объект) и возвращает None.
        """
        session = self._get_session()
        domain = SemaphoreManager.get_domain(url)
        user_agent = self._rotator.next()
        async with self._semaphore:
            if self.respect_robots:
                await self.robots.fetch_robots(url, session)
                if not self.robots.can_fetch(url, user_agent):
                    return self._fail(url, PermanentError("Запрещено robots.txt", url))
                # Crawl-delay из robots.txt превращается в минимальную паузу для этого сайта
                self._rate_limiter.set_domain_delay(domain, self.robots.get_crawl_delay(user_agent, url))

            logger.info("Начало загрузки: %s", url)
            started = time.perf_counter()
            attempts = {"count": 0}               # номер попытки нужен, чтобы увеличивать таймауты
            try:
                text = await self.retry_strategy.execute_with_retry(
                    self._fetch_attempt, url, domain, user_agent, attempts)
            except CrawlerError as error:
                return self._fail(url, error)

        self.errors.pop(url, None)
        self.error_details.pop(url, None)
        logger.info("Успешно: %s (%d символов, %.2f с)", url, len(text), time.perf_counter() - started)
        return text

    async def _fetch_attempt(self, url: str, domain: str, user_agent: str, attempts: dict) -> str:
        """Одна попытка загрузки. При неудаче выбрасывает ошибку нужного типа (TransientError и т.п.)."""
        attempt = attempts["count"]
        attempts["count"] += 1
        if self.circuit_breaker is not None:
            self.circuit_breaker.allow(domain)          # домен заблокирован: сразу CircuitOpenError
        logger.info("Попытка %d: %s", attempt + 1, url)
        try:
            await self._rate_limiter.acquire(domain)
            # с каждой попыткой таймауты растут: медленному сайту даём больше времени
            base, rs = self.timeout, self.retry_strategy
            timeout = aiohttp.ClientTimeout(
                total=None if base.total is None else rs.timeout_for(attempt, base.total),
                connect=rs.timeout_for(attempt, base.connect),
                sock_read=rs.timeout_for(attempt, base.sock_read))
            session = self._get_session()
            async with session.get(url, headers={"User-Agent": user_agent}, timeout=timeout) as response:
                if response.status >= 400:
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    raise classify_status(response.status, url, response.reason, retry_after)
                text = await response.text(errors="replace")
        except asyncio.CancelledError:
            if self.circuit_breaker is not None:
                self.circuit_breaker.release(domain)
            raise
        except Exception as exc:
            error = classify_exception(exc, url)
            self._note_outcome(domain, error)
            if error is exc:
                raise
            raise error from exc
        self._note_outcome(domain, None)
        return text

    def _note_outcome(self, domain: str, error: CrawlerError | None) -> None:
        """Сообщает предохранителю, как закончился запрос."""
        if self.circuit_breaker is None:
            return
        if error is None or (isinstance(error, PermanentError) and error.status is not None):
            self.circuit_breaker.record_success(domain)      # сайт ответил (даже 404 значит «сайт жив»)
        elif isinstance(error, (TransientError, NetworkError)):
            self.circuit_breaker.record_failure(domain)      # сайт перегружен или недоступен
        else:
            self.circuit_breaker.release(domain)

    def _on_retry(self, error: CrawlerError, delay: float, retry_number: int) -> None:
        """Вызывается перед повтором. Если сайт просит притормозить (429, 503), откладываем все запросы к нему."""
        if error.url and error.status in (429, 503):
            self._rate_limiter.delay_domain(SemaphoreManager.get_domain(error.url), delay)

    def _fail(self, url: str, error: CrawlerError) -> None:
        self.errors[url] = str(error)
        self.error_details[url] = error
        logger.warning("Ошибка загрузки %s -> %s", url, error)
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
        try:
            return await self._parser.parse_html(html, url)
        except Exception as exc:                  # страница загрузилась, но разобрать её не удалось
            error = ParseError(f"{type(exc).__name__}: {exc}", url)
            self._fail(url, error)
            return {"url": url, "title": "", "text": "", "links": [], "metadata": {}, "images": [],
                    "headings": [], "tables": [], "lists": [], "errors": [str(error)]}

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
        p = self.get_politeness_stats()
        return (f"[{s['elapsed']:5.1f} с] обработано: {s['processed']} | в очереди: {s['queued']} | "
                f"в работе: {s['in_progress']} | ошибок: {s['failed']} | {s['pages_per_sec']:.1f} стр/с | "
                f"запросов/с: {p['current_rps']:.1f} | пауза: {p['avg_delay']:.2f} с | robots: {p['blocked_by_robots']}")

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

    def get_politeness_stats(self) -> dict:
        """Скорость запросов, средняя пауза между ними, запреты robots.txt и повторы."""
        stats = self._rate_limiter.get_stats()
        stats["blocked_by_robots"] = self.robots.blocked_count
        stats["retries"] = self.retries_done
        return stats

    def get_error_stats(self) -> dict:
        """Сводка по ошибкам: по типам, повторы, список адресов с ошибками, состояние предохранителя."""
        stats = self.retry_strategy.get_stats()
        failed = {}
        for url, error in self.error_details.items():
            failed[url] = {"type": error.kind, "status": error.status, "message": str(error)}
        stats["failed_urls"] = failed
        stats["failed_count"] = len(failed)
        if self.circuit_breaker is not None:
            stats["circuit_breaker"] = self.circuit_breaker.get_stats()
        return stats

    def format_error_report(self) -> str:
        """Текст отчёта об ошибках, который можно показать на экране или сохранить в файл."""
        stats = self.get_error_stats()
        lines = ["ОТЧЁТ ОБ ОШИБКАХ", "=" * 40]
        lines.append(f"Запросов выполнено: {stats['calls']}")
        lines.append(f"Ошибок всего (считая каждую попытку): {stats['total_errors']}")
        for kind, count in sorted(stats["errors_by_type"].items()):
            lines.append(f"  {kind}: {count}")
        lines.append(f"Повторов: {stats['retries']}")
        lines.append(f"Повторов, после которых запрос удался: {stats['successful_retries']}")
        lines.append(f"Запросов, которые не удались даже после повторов: {stats['failed_after_retries']}")
        lines.append(f"Среднее ожидание перед повтором: {stats['avg_retry_delay']:.2f} с")
        lines.append("")
        lines.append(f"Адреса с постоянными ошибками ({len(stats['permanent_urls'])}):")
        for url in stats["permanent_urls"] or ["нет"]:
            lines.append(f"  {url}")
        lines.append("")
        lines.append(f"Все адреса, которые не удалось загрузить ({stats['failed_count']}):")
        for url, info in stats["failed_urls"].items():
            lines.append(f"  {url} -> {info['message']}")
        if not stats["failed_urls"]:
            lines.append("  нет")
        breaker = stats.get("circuit_breaker")
        if breaker:
            lines.append("")
            lines.append(f"Предохранитель: блокировок {breaker['opened_total']}, отклонено запросов {breaker['rejected']}")
            for domain, info in breaker["domains"].items():
                lines.append(f"  {domain}: {info['state']}, доля ошибок {info['failure_rate']:.0%}")
        return "\n".join(lines) + "\n"

    def save_error_report(self, path: str = "error_report.txt") -> str:
        """Сохраняет отчёт об ошибках в файл и возвращает путь."""
        with open(path, "w", encoding="utf-8") as file:
            file.write(self.format_error_report())
        return path