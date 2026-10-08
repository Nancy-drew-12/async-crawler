import asyncio
import os
import sys
from collections import Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.crawler_queue import CrawlerQueue
from src.semaphore_manager import SemaphoreManager
from src.url_filter import URLFilter, normalize_domain, normalize_url


def section(title):
    print(f"\n=== {title} ===")


# ---------- тестовый сайт на localhost: тесты не зависят от интернета ----------

PAGES = {
    "/":         '<a href="/a">a</a> <a href="/b">b</a> <a href="/">себя</a> '
                 '<a href="/file.pdf">pdf</a> <a href="http://localhost:{port}/other">чужой домен</a>',
    "/a":        '<a href="/c">c</a> <a href="/b">b</a> <a href="/#top">наверх</a>',
    "/b":        '<a href="/missing">битая</a> <a href="/a">a</a>',
    "/c":        '<a href="/d">d</a>',
    "/d":        '<a href="/e">e</a>',
    "/e":        'конец',
    "/other":    'страница чужого домена',
    "/file.pdf": 'pdf',
}


class LocalServer:
    """Сайт-граф: /  ->  /a, /b  ->  /c  ->  /d  ->  /e. Считает обращения и параллельные запросы."""

    def __init__(self):
        self.hits = Counter()
        self.active = 0
        self.peak = 0
        app = web.Application()
        app.router.add_get("/{tail:.*}", self.handle)
        self._runner = web.AppRunner(app)

    async def start(self):
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.port = self._runner.addresses[0][1]
        self.base = f"http://127.0.0.1:{self.port}"

    async def stop(self):
        await self._runner.cleanup()

    def reset(self):
        self.hits.clear()
        self.active = self.peak = 0

    async def handle(self, request):
        self.hits[request.path] += 1
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0.05)
            if request.path in PAGES:
                text = PAGES[request.path].replace("{port}", str(self.port))
                return web.Response(text=f"<html><body>{text}</body></html>", content_type="text/html")
            return web.Response(status=404, text="нет такой страницы")
        finally:
            self.active -= 1


async def crawl(server, max_depth=2, max_per_domain=3, max_concurrent=10, **kwargs):
    """Запускает краулер на тестовом сайте и возвращает (краулер, список путей обработанных страниц)."""
    server.reset()
    kwargs.setdefault("progress", False)
    async with AsyncCrawler(max_concurrent=max_concurrent, max_depth=max_depth,
                            max_per_domain=max_per_domain) as crawler:
        await crawler.crawl([server.base + "/"], **kwargs)
    paths = sorted(u.replace(server.base, "") for u in crawler.processed_urls)
    return crawler, paths


# ---------- тесты ----------

async def test_queue():
    section("1. Очередь с приоритетами")
    q = CrawlerQueue()
    assert q.add_url("a") and q.add_url("b", priority=5) and q.add_url("c") and q.add_url("d", priority=5)
    assert q.add_url("a") is False, "повторный URL не добавляется"
    order = []
    while (url := await q.get_next()) is not None:
        order.append(url)
        q.mark_processed(url)
    print("Порядок выдачи:", order)
    assert order == ["b", "d", "a", "c"], "сначала больший приоритет, при равном порядок добавления"

    q.add_url("x")
    url = await q.get_next()
    assert q.get_stats()["in_progress"] == 1
    q.mark_failed(url, "ошибка")
    stats = q.get_stats()
    print("Статистика:", stats)
    assert stats == {"queued": 0, "in_progress": 0, "processed": 4, "failed": 1,
                     "total_added": 5, "duplicates_skipped": 1}

    # get_next ждёт, пока другой обработчик не закончит и не добавит новый URL
    q2 = CrawlerQueue()
    q2.add_url("p")
    busy = await q2.get_next()

    async def finish_later():
        await asyncio.sleep(0.15)
        q2.add_url("q")
        q2.mark_processed(busy)

    task = asyncio.create_task(finish_later())
    assert await q2.get_next() == "q", "get_next дождался нового URL"
    await task
    for bad in (None, "", 5):
        try:
            q.add_url(bad)
            raise AssertionError("ожидалась ошибка")
        except TypeError:
            pass


def test_url_filter():
    section("2. Фильтрация URL")
    f = URLFilter(allowed_domains={"Example.com"})
    assert f.is_allowed("https://example.com/a")
    assert f.is_allowed("https://www.example.com/b"), "www не считается другим сайтом"
    assert not f.is_allowed("https://other.org/")
    assert not f.is_allowed("mailto:a@b.c") and not f.is_allowed("javascript:void(0)")

    f = URLFilter(exclude_patterns=[r"\.pdf$", r"/login"])
    assert not f.is_allowed("https://x.com/doc.pdf") and not f.is_allowed("https://x.com/login")
    assert f.is_allowed("https://x.com/page")

    f = URLFilter(include_patterns=[r"/blog/"])
    assert f.is_allowed("https://x.com/blog/1") and not f.is_allowed("https://x.com/shop")

    f = URLFilter(include_patterns=[r"/blog/"], exclude_patterns=[r"draft"])
    assert not f.is_allowed("https://x.com/blog/draft-1"), "исключение сильнее включения"
    assert f.is_allowed("https://x.com/blog/2")

    assert normalize_url("HTTPS://Example.com") == "https://example.com/"
    assert normalize_url("https://example.com/a#top") == "https://example.com/a"
    assert normalize_domain("https://www.Example.com/x") == "example.com"
    print("Фильтры работают")


async def test_semaphores():
    section("3. SemaphoreManager")

    async def job(manager, url):
        async with manager.acquire(url):
            await asyncio.sleep(0.05)

    m = SemaphoreManager(max_concurrent=10, max_per_domain=2)
    await asyncio.gather(*(job(m, f"https://a.com/{i}") for i in range(6)))
    print("Один домен, лимит 2:", m.get_stats()["peak_active"])
    assert m.get_stats()["peak_active"] == 2 and m.active_count == 0

    m = SemaphoreManager(max_concurrent=3, max_per_domain=2)
    await asyncio.gather(*(job(m, f"https://{d}/{i}") for d in ("a.com", "b.com", "c.com") for i in range(4)))
    print("Три домена, общий лимит 3:", m.get_stats()["peak_active"])
    assert m.get_stats()["peak_active"] == 3

    m = SemaphoreManager(1, 1)
    try:
        async with m.acquire("https://a.com/x"):
            raise RuntimeError("сбой внутри блока")
    except RuntimeError:
        pass
    assert m.active_count == 0, "слот освобождается даже после ошибки"
    await asyncio.wait_for(job(m, "https://a.com/y"), timeout=1)


async def test_depth(server):
    section("4. Ограничение глубины")
    for depth, expected in ((0, ["/"]),
                            (1, ["/", "/a", "/b", "/file.pdf"]),
                            (2, ["/", "/a", "/b", "/c", "/file.pdf"]),
                            (5, ["/", "/a", "/b", "/c", "/d", "/e", "/file.pdf"])):
        _, paths = await crawl(server, max_depth=depth, same_domain_only=True)
        print(f"max_depth={depth}: {paths}")
        assert paths == expected, f"глубина {depth}: получили {paths}"

    try:
        AsyncCrawler(max_depth=-1)
        raise AssertionError("ожидалась ошибка")
    except ValueError:
        pass


async def test_no_duplicates(server):
    section("5. Нет дубликатов")
    crawler, paths = await crawl(server, max_depth=5, same_domain_only=True)
    print("Обращения к серверу:", dict(server.hits))
    assert set(server.hits.values()) == {1}, "каждая страница запрошена ровно один раз"
    assert len(crawler.visited_urls) == sum(server.hits.values()), "visited_urls совпадает с запросами"
    assert all(url == normalize_url(url) for url in crawler.visited_urls)
    assert server.hits["/"] == 1, "ссылки на себя и на '/#top' не вызывают повторной загрузки"


async def test_limits_and_state(server):
    section("6. max_pages и состояние")
    crawler, paths = await crawl(server, max_depth=5, max_pages=3, same_domain_only=True)
    print("max_pages=3:", paths)
    assert sum(server.hits.values()) == 3 and len(paths) + len(crawler.failed_urls) == 3
    assert paths[0] == "/", "первой обрабатывается стартовая страница"

    crawler, paths = await crawl(server, max_depth=2, same_domain_only=True)
    missing = server.base + "/missing"
    print("failed_urls:", crawler.failed_urls)
    assert missing in crawler.failed_urls and "404" in crawler.failed_urls[missing]
    assert missing not in crawler.processed_urls
    assert missing in crawler.visited_urls
    stats = crawler.get_crawl_stats()
    assert stats["processed"] == len(crawler.processed_urls) and stats["failed"] == 1
    assert stats["queued"] == 0 and stats["in_progress"] == 0 and stats["pages_per_sec"] > 0

    # повторный запуск начинается с чистого состояния
    crawler2 = AsyncCrawler(max_depth=0)
    try:
        await crawler2.crawl([server.base + "/"], progress=False)
        first = set(crawler2.processed_urls)
        await crawler2.crawl([server.base + "/a"], progress=False)
        assert first == {server.base + "/"} and set(crawler2.processed_urls) == {server.base + "/a"}
    finally:
        await crawler2.close()


async def test_filters_in_crawl(server):
    section("7. Фильтры внутри crawl()")
    _, paths = await crawl(server, max_depth=2, same_domain_only=True)
    assert not any("other" in p for p in paths), "чужой домен не обходится"

    _, paths = await crawl(server, max_depth=2, same_domain_only=False)
    print("Без same_domain_only:", paths)
    assert any(p.endswith("/other") for p in paths)

    _, paths = await crawl(server, max_depth=2, same_domain_only=True, exclude_patterns=[r"\.pdf$"])
    assert "/file.pdf" not in paths

    _, paths = await crawl(server, max_depth=5, same_domain_only=True, include_patterns=[r"/$", r"/a$", r"/c$"])
    print("include:", paths)
    assert paths == ["/", "/a", "/c"]


async def test_concurrency(server):
    section("8. Лимит на домен при обходе")
    crawler, _ = await crawl(server, max_depth=5, same_domain_only=True, max_per_domain=1, max_concurrent=5)
    print("Максимум одновременных запросов к серверу:", server.peak)
    assert server.peak == 1
    assert crawler._limits.get_stats()["peak_active"] == 1

    crawler, _ = await crawl(server, max_depth=5, same_domain_only=True, max_per_domain=2, max_concurrent=5)
    assert server.peak <= 2


async def main():
    server = LocalServer()
    await server.start()
    try:
        await test_queue()
        test_url_filter()
        await test_semaphores()
        await test_depth(server)
        await test_no_duplicates(server)
        await test_limits_and_state(server)
        await test_filters_in_crawl(server)
        await test_concurrency(server)
    finally:
        await server.stop()
    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    asyncio.run(main())