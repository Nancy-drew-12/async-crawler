"""Чтение sitemap.xml: обычных, индексных (sitemap со ссылками на другие sitemap) и сжатых (.xml.gz)."""
import asyncio
import contextlib
import gzip
import logging
import xml.etree.ElementTree as ET
import zlib
from urllib.parse import urljoin, urlsplit

import aiohttp

from src.errors import CrawlerError, PermanentError, classify_exception, classify_status
from src.rate_limiter import RateLimiter
from src.retry import RetryStrategy
from src.robots_parser import RobotsParser
from src.semaphore_manager import SemaphoreManager

logger = logging.getLogger("crawler")

MAX_SITEMAP_BYTES = 50 * 1024 * 1024        # по стандарту sitemap не больше 50 МБ (после распаковки)
DEFAULT_USER_AGENT = "AsyncCrawler/0.1 (учебный проект)"


class SitemapError(CrawlerError):
    """Sitemap не удалось прочитать: неверный формат или слишком большой файл."""


def _local_name(tag: str) -> str:
    """'{http://www.sitemaps.org/schemas/sitemap/0.9}loc' -> 'loc'"""
    return tag.rsplit("}", 1)[-1].lower()


class SitemapParser:
    """Загружает sitemap и достаёт из него адреса страниц.

    Понимает три вида файлов: обычный (<urlset>), индексный (<sitemapindex>, он обрабатывается
    рекурсивно) и простой текстовый (один адрес на строку). Сжатые .xml.gz распаковываются сами.
    Сбой одного вложенного sitemap не останавливает остальные: причина попадает в self.errors.

    Правила вежливости. Если передать компоненты краулера, sitemap загружается по тем же правилам, что и
    страницы: session (общий пул соединений), rate_limiter (пауза между запросами к сайту), limits
    (лимиты одновременных запросов на сайт и всего), robots (проверка robots.txt; None = не проверять).
    Проще всего получить такой парсер через AsyncCrawler.create_sitemap_parser().
    """

    def __init__(self, user_agent: str = DEFAULT_USER_AGENT, timeout: float = 15.0, max_depth: int = 5,
                 max_urls: int = 100_000, max_concurrent: int = 5, retry_strategy: RetryStrategy | None = None,
                 session: aiohttp.ClientSession | None = None, rate_limiter: RateLimiter | None = None,
                 limits: SemaphoreManager | None = None, robots: RobotsParser | None = None):
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth должен быть целым числом не меньше 0")
        if isinstance(max_urls, bool) or not isinstance(max_urls, int) or max_urls < 1:
            raise ValueError("max_urls должен быть целым числом не меньше 1")
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent < 1:
            raise ValueError("max_concurrent должен быть целым числом не меньше 1")
        self.user_agent = user_agent
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_depth = max_depth              # насколько глубоко идти по вложенным sitemap
        self.max_urls = max_urls                # столько адресов вернём максимум
        self.max_concurrent = max_concurrent    # сколько вложенных sitemap грузить одновременно
        self.retry_strategy = retry_strategy or RetryStrategy(max_retries=2, base_delay=0.5)
        self._session = session                 # чужую сессию (из краулера) не закрываем
        self.rate_limiter = rate_limiter        # пауза между запросами к одному сайту (общая с краулером)
        self.limits = limits                    # лимиты параллельности (общие с краулером)
        self.robots = robots                    # проверка robots.txt; None = не проверять
        self.errors: dict[str, str] = {}        # адрес sitemap -> причина неудачи
        self.sitemaps_fetched: list[str] = []   # какие sitemap удалось прочитать

    # ---------- загрузка ----------

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """Возвращает список адресов страниц из sitemap (и из всех вложенных). Без дублей, в порядке появления."""
        if not isinstance(sitemap_url, str) or urlsplit(sitemap_url).scheme not in ("http", "https"):
            raise ValueError("sitemap_url должен быть адресом http или https")
        self.errors = {}
        self.sitemaps_fetched = []
        own_session = self._session is None
        session = self._session or aiohttp.ClientSession(headers={"User-Agent": self.user_agent})
        urls: dict[str, None] = {}              # словарь вместо множества: сохраняет порядок
        try:
            await self._walk(session, sitemap_url, 0, set(), urls, asyncio.Semaphore(self.max_concurrent))
        finally:
            if own_session:
                await session.close()
        result = list(urls)
        logger.info("Sitemap %s: найдено %d адресов (прочитано файлов: %d, ошибок: %d)",
                    sitemap_url, len(result), len(self.sitemaps_fetched), len(self.errors))
        return result

    async def fetch_for_site(self, site_url: str, robots: RobotsParser | None = None) -> list[str]:
        """Находит sitemap сайта (строки Sitemap: в robots.txt, иначе /sitemap.xml) и читает все."""
        origin = RobotsParser.get_origin(site_url)
        own_session = self._session is None
        session = self._session or aiohttp.ClientSession(headers={"User-Agent": self.user_agent})
        try:
            robots = robots or self.robots or RobotsParser(fetch_agent=self.user_agent)
            info = await robots.fetch_robots(origin, session)
        finally:
            if own_session:
                await session.close()
        sitemaps = info.get("sitemaps") or [origin + "/sitemap.xml"]
        urls: dict[str, None] = {}
        errors: dict[str, str] = {}
        fetched: list[str] = []
        for sitemap in sitemaps:
            for url in await self.fetch_sitemap(sitemap):
                urls.setdefault(url)
            errors.update(self.errors)
            fetched.extend(self.sitemaps_fetched)
        self.errors, self.sitemaps_fetched = errors, fetched
        return list(urls)[:self.max_urls]

    async def _walk(self, session, sitemap_url, depth, seen, urls, gate) -> None:
        if sitemap_url in seen:                 # защита от циклов: sitemap ссылается сам на себя
            return
        seen.add(sitemap_url)
        try:
            async with gate, self._slot(sitemap_url):
                await self._check_robots(session, sitemap_url)
                data = await self.retry_strategy.execute_with_retry(self._download, session, url=sitemap_url)
            kind, locs = self._parse(data, sitemap_url)
        except CrawlerError as error:
            self.errors[sitemap_url] = str(error)
            logger.warning("Не удалось прочитать sitemap %s: %s", sitemap_url, error)
            return
        self.sitemaps_fetched.append(sitemap_url)

        if kind == "index":
            if depth >= self.max_depth:
                self.errors[sitemap_url] = f"слишком глубокая вложенность sitemap (больше {self.max_depth})"
                return
            await asyncio.gather(*(self._walk(session, child, depth + 1, seen, urls, gate) for child in locs))
        else:
            for loc in locs:
                if len(urls) >= self.max_urls:
                    return
                urls.setdefault(loc)

    def _slot(self, url: str):
        """Место в общих лимитах параллельности (если они заданы): не больше N запросов к одному сайту."""
        return self.limits.acquire(url) if self.limits is not None else contextlib.nullcontext()

    async def _check_robots(self, session, url: str) -> None:
        """Если включена проверка robots.txt, запрещённый sitemap не загружается (как и запрещённая страница)."""
        if self.robots is None:
            return
        await self.robots.fetch_robots(url, session)
        if not self.robots.can_fetch(url, self.user_agent):
            raise PermanentError("Запрещено robots.txt", url)
        if self.rate_limiter is not None:       # Crawl-delay из robots.txt становится паузой для этого сайта
            self.rate_limiter.set_domain_delay(SemaphoreManager.get_domain(url),
                                               self.robots.get_crawl_delay(self.user_agent, url))

    async def _download(self, session, url: str) -> bytes:
        """Скачивает файл целиком (не больше лимита) и распаковывает его, если он сжат."""
        if self.rate_limiter is not None:       # перед каждой попыткой, в том числе повторной
            await self.rate_limiter.acquire(SemaphoreManager.get_domain(url))
        try:
            async with session.get(url, timeout=self.timeout) as response:
                if response.status >= 400:
                    raise classify_status(response.status, url, response.reason)
                chunks, size = [], 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    size += len(chunk)
                    if size > MAX_SITEMAP_BYTES:
                        raise SitemapError("файл больше 50 МБ", url)
                    chunks.append(chunk)
        except CrawlerError:
            raise
        except Exception as exc:
            error = classify_exception(exc, url)
            raise error from exc
        data = b"".join(chunks)
        if data[:2] == b"\x1f\x8b":             # признак gzip: распаковываем с ограничением размера
            try:
                unpacker = zlib.decompressobj(wbits=31)
                data = unpacker.decompress(data, MAX_SITEMAP_BYTES + 1)
            except (zlib.error, EOFError, gzip.BadGzipFile) as exc:
                raise SitemapError(f"не удалось распаковать gzip: {exc}", url) from exc
            if len(data) > MAX_SITEMAP_BYTES:
                raise SitemapError("после распаковки файл больше 50 МБ", url)
        return data

    # ---------- разбор ----------

    def _parse(self, data: bytes, sitemap_url: str) -> tuple[str, list[str]]:
        """Возвращает ("urls", адреса страниц) или ("index", адреса вложенных sitemap)."""
        head = data[:2048].lstrip(b"\xef\xbb\xbf \t\r\n").lower()
        if not head.startswith(b"<"):
            return "urls", self._clean(data.decode("utf-8", errors="replace").splitlines(), sitemap_url)
        if b"<!doctype" in data[:4096].lower() or b"<!entity" in data[:4096].lower():
            raise SitemapError("в sitemap есть DOCTYPE или ENTITY, такие файлы не читаем", sitemap_url)
        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            raise SitemapError(f"неверный XML: {exc}", sitemap_url) from exc
        root_name = _local_name(root.tag)
        if root_name not in ("urlset", "sitemapindex"):
            raise SitemapError(f"это не sitemap (корневой тег <{root_name}>)", sitemap_url)
        item = "sitemap" if root_name == "sitemapindex" else "url"
        locs = []
        for element in root:
            if _local_name(element.tag) != item:
                continue
            for child in element:
                if _local_name(child.tag) == "loc" and child.text:
                    locs.append(child.text)
        return ("index" if root_name == "sitemapindex" else "urls"), self._clean(locs, sitemap_url)

    @staticmethod
    def _clean(locs: list[str], base_url: str) -> list[str]:
        """Убирает пустое и лишнее: пробелы, чужие схемы (mailto:, ftp:); относительные адреса достраивает."""
        result = []
        for loc in locs:
            loc = loc.strip()
            if not loc or loc.startswith("#"):
                continue
            absolute = urljoin(base_url, loc)
            if urlsplit(absolute).scheme in ("http", "https") and urlsplit(absolute).netloc:
                result.append(absolute)
        return result

    def get_stats(self) -> dict:
        return {"sitemaps_fetched": len(self.sitemaps_fetched), "errors": dict(self.errors)}