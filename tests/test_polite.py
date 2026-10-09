import asyncio
import os
import sys
import time
from collections import Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.politeness import RETRY_STATUSES, UserAgentRotator, backoff_delay, parse_retry_after
from src.rate_limiter import RateLimiter
from src.robots_parser import RobotsParser

# Allow стоит перед Disallow: так правило работает одинаково в любой версии Python
ROBOTS = """User-agent: *
Allow: /private/open
Disallow: /private/
Disallow: /admin
Crawl-delay: 1

User-agent: BadBot
Disallow: /

Sitemap: http://example.test/sitemap.xml
"""

PAGES = {
    "/":        '<a href="/a">a</a> <a href="/b">b</a> <a href="/private/x">x</a> '
                '<a href="/admin/y">y</a> <a href="/c">c</a>',
    "/a": "страница a", "/b": "страница b", "/c": "страница c",
    "/private/x": "секрет", "/admin/y": "админка",
}


def section(title):
    print(f"\n=== {title} ===")


class LocalServer:
    """Тестовый сайт на localhost со своим robots.txt. Запоминает время и подпись каждого запроса."""

    def __init__(self):
        self.reset()
        app = web.Application()
        app.router.add_get("/{tail:.*}", self.handle)
        self._runner = web.AppRunner(app)

    def reset(self):
        self.log = []                 # (время, путь, User-Agent)
        self.hits = Counter()
        self.robots_status = 200
        self.robots_text = ROBOTS

    async def start(self):
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.port = self._runner.addresses[0][1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.alt = f"http://localhost:{self.port}"       # другой «домен», тот же сервер

    async def stop(self):
        await self._runner.cleanup()

    def page_requests(self):
        return [(t, p, ua) for t, p, ua in self.log if p != "/robots.txt"]

    def gaps(self):
        times = [t for t, _, _ in self.page_requests()]
        return [b - a for a, b in zip(times, times[1:])]

    async def handle(self, request):
        path = request.path
        self.hits[path] += 1
        self.log.append((time.monotonic(), path, request.headers.get("User-Agent")))
        if path == "/robots.txt":
            return web.Response(status=self.robots_status, text=self.robots_text if self.robots_status == 200 else "")
        if path == "/flaky":                     # два раза 503, на третий раз ответ
            return web.Response(status=503) if self.hits[path] < 3 else web.Response(text="ok")
        if path == "/retry-after":               # 429 с просьбой подождать секунду
            return web.Response(status=429, headers={"Retry-After": "1"}) if self.hits[path] < 2 \
                else web.Response(text="ok")
        if path == "/always503":
            return web.Response(status=503)
        if path == "/missing":
            return web.Response(status=404)
        return web.Response(text=PAGES.get(path, "обычная страница"), content_type="text/html")


async def timed(coro):
    start = time.monotonic()
    result = await coro
    return time.monotonic() - start, result


# ---------- 1. RateLimiter ----------

async def test_rate_one_domain():
    section("1. Rate limiting для одного домена")
    rl = RateLimiter(requests_per_second=10)
    granted = []

    async def go():
        await rl.acquire("a.com")
        granted.append(time.monotonic())

    elapsed, _ = await timed(asyncio.gather(*(go() for _ in range(5))))
    gaps = [b - a for a, b in zip(sorted(granted), sorted(granted)[1:])]
    print(f"5 запросов при 10/с: {elapsed:.2f} с, минимальная пауза {min(gaps):.3f} с")
    assert elapsed >= 0.39 and min(gaps) >= 0.085

    stats = rl.get_stats()
    print("Статистика:", {k: round(v, 3) for k, v in stats.items()})
    assert stats["requests"] == 5 and stats["avg_delay"] >= 0.09 and 8 <= stats["current_rps"] <= 12

    await asyncio.sleep(0.3)                      # после простоя ждать не нужно
    elapsed, _ = await timed(rl.acquire("a.com"))
    assert elapsed < 0.2


async def test_rate_many_domains():
    section("2. Rate limiting для разных доменов")
    domains = ("a.com", "b.com", "c.com")
    per_domain, shared = RateLimiter(10, per_domain=True), RateLimiter(10, per_domain=False)
    t_per, _ = await timed(asyncio.gather(*(per_domain.acquire(d) for d in domains for _ in range(3))))
    t_all, _ = await timed(asyncio.gather(*(shared.acquire(d) for d in domains for _ in range(3))))
    print(f"9 запросов: у каждого домена свой лимит {t_per:.2f} с, общий лимит {t_all:.2f} с")
    assert t_per >= 0.19 and t_all >= 0.79 and t_per < t_all

    rl = RateLimiter(100)
    rl.set_domain_delay("slow.com", 0.3)
    done = {}

    async def go(domain):
        await rl.acquire(domain)
        await rl.acquire(domain)
        done[domain] = time.monotonic()

    await asyncio.gather(go("slow.com"), go("fast.com"))
    print("Медленный домен не тормозит быстрый:", done["fast.com"] < done["slow.com"])
    assert done["fast.com"] < done["slow.com"]

    shared = RateLimiter(100, per_domain=False)
    shared.set_domain_delay("any.com", 0.2)       # при общем лимите Crawl-delay тоже действует
    t, _ = await timed(asyncio.gather(shared.acquire("x.com"), shared.acquire("y.com")))
    assert t >= 0.19

    rl = RateLimiter(100)
    rl.delay_domain("a.com", 0.3)
    t_a, _ = await timed(rl.acquire("a.com"))
    t_b, _ = await timed(rl.acquire("b.com"))
    print(f"delay_domain: a.com ждал {t_a:.2f} с, b.com {t_b:.2f} с")
    assert t_a >= 0.25 and t_b < 0.1


async def test_delays():
    section("3. Задержки: min_delay, jitter, настройки")
    rl = RateLimiter(requests_per_second=100, min_delay=0.1)
    t, _ = await timed(asyncio.gather(*(rl.acquire("a") for _ in range(4))))
    print(f"min_delay=0.1, 4 запроса: {t:.2f} с")
    assert t >= 0.29 and rl.interval_for("a") == 0.1

    rl = RateLimiter(requests_per_second=100, jitter=0.1)
    t, _ = await timed(asyncio.gather(*(rl.acquire("a") for _ in range(6))))
    print(f"jitter=0.1, 6 запросов: {t:.2f} с")
    assert 0.05 <= t < 2.0

    for kwargs in ({"requests_per_second": 0}, {"requests_per_second": -1}, {"requests_per_second": True},
                   {"min_delay": -1}, {"jitter": -0.5}):
        try:
            RateLimiter(**kwargs)
            raise AssertionError(f"ожидалась ошибка для {kwargs}")
        except ValueError:
            pass


def test_politeness_helpers():
    section("4. Backoff, Retry-After, ротация User-Agent")
    seq = [backoff_delay(i, jitter=0) for i in range(7)]
    print("Паузы backoff:", seq)
    assert seq == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    assert all(4.0 <= backoff_delay(2) <= 5.0 for _ in range(200)), "jitter добавляет не больше 25%"
    assert backoff_delay(10, max_delay=5, jitter=0) == 5

    assert parse_retry_after("5") == 5.0 and parse_retry_after(None) is None and parse_retry_after("мусор") is None
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0, "дата в прошлом: ждать не нужно"
    assert RETRY_STATUSES == {429, 500, 502, 503, 504} and 404 not in RETRY_STATUSES

    rotator = UserAgentRotator(["A/1", "B/1", "C/1"])
    assert [rotator.next() for _ in range(5)] == ["A/1", "B/1", "C/1", "A/1", "B/1"]
    assert UserAgentRotator("Solo/1").next() == "Solo/1"
    for bad in ([], [""], 5):
        try:
            UserAgentRotator(bad)
            raise AssertionError("ожидалась ошибка")
        except (ValueError, TypeError):
            pass


# ---------- 2. RobotsParser ----------

async def test_robots_parser(server):
    section("5. Парсинг robots.txt")
    server.reset()
    robots = RobotsParser()
    try:
        robots.can_fetch(server.base + "/a")
        raise AssertionError("ожидалась ошибка: правила ещё не загружены")
    except RuntimeError:
        pass

    info = await robots.fetch_robots(server.base + "/любая/страница")
    print("Описание:", info)
    assert info["state"] == "rules" and info["status"] == 200 and info["from_cache"] is False
    assert info["crawl_delay"] == 1.0 and info["sitemaps"] == ["http://example.test/sitemap.xml"]

    assert robots.can_fetch(server.base + "/a")
    assert not robots.can_fetch(server.base + "/private/x")
    assert robots.can_fetch(server.base + "/private/open/page"), "Allow сильнее общего запрета"
    assert not robots.can_fetch(server.base + "/admin/panel")
    assert not robots.can_fetch(server.base + "/a", "BadBot/1.0"), "для BadBot закрыто всё"
    assert robots.can_fetch(server.base + "/a", "GoodBot/1.0")
    assert robots.get_crawl_delay() == 1.0 and robots.get_crawl_delay("BadBot") == 0.0
    assert robots.get_crawl_delay(url=server.base + "/a") == 1.0
    assert robots.blocked_count == 3, robots.blocked_urls

    # кэш: много одновременных вызовов дают одну загрузку файла
    await asyncio.gather(*(robots.fetch_robots(server.base) for _ in range(10)))
    assert server.hits["/robots.txt"] == 1
    assert (await robots.fetch_robots(server.base))["from_cache"] is True
    print("Запросов robots.txt к серверу:", server.hits["/robots.txt"])

    # кэш устаревает через ttl
    short = RobotsParser(ttl=0.2)
    await short.fetch_robots(server.base)
    await asyncio.sleep(0.3)
    assert (await short.fetch_robots(server.base))["from_cache"] is False

    # у каждого сайта свои правила
    other = await robots.fetch_robots(server.alt)
    assert other["from_cache"] is False and server.hits["/robots.txt"] == 4

    section("6. robots.txt: нет файла, сбой, недоступный сайт")
    server.reset()
    server.robots_status = 404
    r404 = RobotsParser()
    assert (await r404.fetch_robots(server.base))["state"] == "allow_all"
    assert r404.can_fetch(server.base + "/private/x"), "файла нет: ограничений нет"
    server.robots_status = 500
    r500 = RobotsParser()
    assert (await r500.fetch_robots(server.base))["state"] == "disallow_all"
    assert not r500.can_fetch(server.base + "/a"), "сайт не отвечает: заходить нельзя"
    r_down = RobotsParser(timeout=1)
    assert (await r_down.fetch_robots("http://127.0.0.1:1"))["state"] == "disallow_all"
    print("404 -> можно всё; 500 и недоступный сайт -> нельзя ничего")


# ---------- 3. Краулер ----------

async def test_blocking(server):
    section("7. Блокировка запрещённых URL")
    server.reset()
    async with AsyncCrawler(respect_robots=True, max_depth=1) as crawler:
        results = await crawler.crawl([server.base + "/"], same_domain_only=True, progress=False)
        blocked = sorted(u.replace(server.base, "") for u in crawler.robots.blocked_urls)
        print("Обработано:", sorted(u.replace(server.base, "") for u in results))
        print("Заблокировано:", blocked)
        assert blocked == ["/admin/y", "/private/x"]
        assert server.hits["/private/x"] == 0 and server.hits["/admin/y"] == 0, "на запрещённые страницы не ходили"
        assert server.hits["/robots.txt"] == 1
        assert set(results) == {server.base + p for p in ("/", "/a", "/b", "/c")}
        reason = crawler.failed_urls[server.base + "/private/x"]
        assert "robots.txt" in reason
        stats = crawler.get_politeness_stats()
        assert stats["blocked_by_robots"] == 2 and stats["requests"] == 4
        assert "robots: 2" in crawler._progress_line()

    server.reset()                                # без respect_robots краулер ходит везде
    async with AsyncCrawler(max_depth=1) as crawler:
        results = await crawler.crawl([server.base + "/"], same_domain_only=True, progress=False)
        assert server.base + "/private/x" in results and server.hits["/robots.txt"] == 0

    server.reset()
    async with AsyncCrawler(respect_robots=True) as crawler:
        assert await crawler.fetch_url(server.base + "/admin/panel") is None
        assert "robots.txt" in crawler.errors[server.base + "/admin/panel"]
        assert server.hits["/admin/panel"] == 0
        assert await crawler.fetch_url(server.base + "/private/open/z") is not None


async def test_crawler_delays(server):
    section("8. Соблюдение задержек при обходе")
    server.reset()
    async with AsyncCrawler(max_concurrent=5, requests_per_second=10) as crawler:
        await crawler.crawl([server.base + "/"], same_domain_only=True, progress=False)
    gaps = server.gaps()
    print(f"requests_per_second=10: {len(gaps) + 1} запросов, минимальная пауза {min(gaps):.3f} с")
    assert len(gaps) == 5 and min(gaps) >= 0.085

    server.reset()
    async with AsyncCrawler(max_concurrent=5, min_delay=0.2) as crawler:
        await crawler.crawl([server.base + "/"], same_domain_only=True, max_pages=4, progress=False)
    print(f"min_delay=0.2: минимальная пауза {min(server.gaps()):.3f} с")
    assert min(server.gaps()) >= 0.19

    server.reset()                                 # Crawl-delay: 1 из robots.txt
    async with AsyncCrawler(max_concurrent=5, respect_robots=True, requests_per_second=100) as crawler:
        await crawler.crawl([server.base + "/"], same_domain_only=True, max_pages=3, progress=False)
        stats = crawler.get_politeness_stats()
    print(f"Crawl-delay=1: минимальная пауза {min(server.gaps()):.2f} с, средняя {stats['avg_delay']:.2f} с")
    assert len(server.page_requests()) == 3 and min(server.gaps()) >= 0.95
    assert stats["avg_delay"] >= 0.95 and stats["current_rps"] <= 1.1

    server.reset()                                 # разные домены не мешают друг другу
    async with AsyncCrawler(min_delay=0.3) as crawler:
        await asyncio.gather(crawler.fetch_url(server.base + "/p1"), crawler.fetch_url(server.alt + "/p2"))
        first, second = [t for t, _, _ in server.page_requests()]
        assert abs(first - second) < 0.25, "первый запрос к каждому домену идёт сразу"
        await asyncio.gather(crawler.fetch_url(server.base + "/p3"), crawler.fetch_url(server.base + "/p4"))
        times = [t for t, _, _ in server.page_requests()][2:]
        assert abs(times[0] - times[1]) >= 0.29, "два запроса к одному домену разведены паузой"

    for kwargs in ({"requests_per_second": 0}, {"max_retries": -1}, {"backoff_base": 0}, {"backoff_max": -1}):
        try:
            AsyncCrawler(**kwargs)
            raise AssertionError(f"ожидалась ошибка для {kwargs}")
        except ValueError:
            pass


async def test_retries(server):
    section("9. Повторы с экспоненциальным backoff")
    server.reset()
    async with AsyncCrawler(max_retries=3, backoff_base=0.1) as crawler:
        elapsed, body = await timed(crawler.fetch_url(server.base + "/flaky"))
        print(f"/flaky: {body!r}, запросов {server.hits['/flaky']}, повторов {crawler.retries_done}, {elapsed:.2f} с")
        assert body == "ok" and server.hits["/flaky"] == 3 and crawler.retries_done == 2
        assert elapsed >= 0.29, "паузы 0.1 и 0.2"
        assert "/flaky" not in "".join(crawler.errors)

    server.reset()
    async with AsyncCrawler(backoff_base=0.01) as crawler:        # сайт просит подождать секунду
        elapsed, body = await timed(crawler.fetch_url(server.base + "/retry-after"))
        print(f"Retry-After: 1: {body!r}, {elapsed:.2f} с")
        assert body == "ok" and elapsed >= 0.99

    server.reset()
    async with AsyncCrawler(max_retries=2, backoff_base=0.05) as crawler:
        assert await crawler.fetch_url(server.base + "/missing") is None
        assert server.hits["/missing"] == 1, "404 не повторяем"
        assert await crawler.fetch_url(server.base + "/always503") is None
        assert server.hits["/always503"] == 3, "первая попытка и два повтора"
        assert "503" in crawler.errors[server.base + "/always503"]
    server.reset()
    async with AsyncCrawler(max_retries=0) as crawler:
        assert await crawler.fetch_url(server.base + "/always503") is None
        assert server.hits["/always503"] == 1

    server.reset()                                 # 429 тормозит весь сайт, а не одну страницу
    async with AsyncCrawler(backoff_base=0.01) as crawler:
        async def later():
            await asyncio.sleep(0.2)               # к этому моменту сайт уже ответил 429
            await asyncio.gather(*(crawler.fetch_url(f"{server.base}/p{i}") for i in range(3)))

        await asyncio.gather(crawler.fetch_url(server.base + "/retry-after"), later())
        first = server.log[0][0]
        others = [t - first for t, p, _ in server.log if p.startswith("/p")]
        print("Остальные запросы ушли через (с):", [round(t, 2) for t in others])
        assert len(others) == 3 and min(others) >= 0.95


async def test_user_agents(server):
    section("10. User-Agent")
    server.reset()
    async with AsyncCrawler(user_agent="TestBot/1.0 (study)") as crawler:
        await crawler.fetch_url(server.base + "/a")
    assert server.log[-1][2] == "TestBot/1.0 (study)"

    server.reset()
    async with AsyncCrawler(user_agents=["BotA/1.0", "BotB/1.0"]) as crawler:
        for i in range(4):
            await crawler.fetch_url(f"{server.base}/p{i}")
    seen = [ua for _, _, ua in server.page_requests()]
    print("Подписи по очереди:", seen)
    assert seen == ["BotA/1.0", "BotB/1.0", "BotA/1.0", "BotB/1.0"]

    server.reset()                                 # правила robots.txt применяются к конкретной подписи
    async with AsyncCrawler(respect_robots=True, user_agent="BadBot/1.0", max_depth=1) as crawler:
        results = await crawler.crawl([server.base + "/"], progress=False)
        print("BadBot получил страниц:", len(results), "| запрещено:", crawler.robots.blocked_count)
        assert results == {} and server.hits["/"] == 0
    server.reset()
    async with AsyncCrawler(respect_robots=True, user_agent="GoodBot/1.0", max_depth=0) as crawler:
        results = await crawler.crawl([server.base + "/"], progress=False)
        assert list(results) == [server.base + "/"]


async def main():
    server = LocalServer()
    await server.start()
    try:
        await test_rate_one_domain()
        await test_rate_many_domains()
        await test_delays()
        test_politeness_helpers()
        await test_robots_parser(server)
        await test_blocking(server)
        await test_crawler_delays(server)
        await test_retries(server)
        await test_user_agents(server)
    finally:
        await server.stop()
    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    asyncio.run(main())