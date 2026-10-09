import asyncio
import os
import sys
import tempfile
import time
from collections import Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.circuit_breaker import CircuitBreaker, CircuitOpenError
from src.errors import NetworkError, ParseError, PermanentError, TransientError, classify_status
from src.retry import RetryStrategy


def section(title):
    print(f"\n=== {title} ===")


class LocalServer:
    """Маленький сайт с поломками. Считает, сколько раз к каждой странице обратились."""

    def __init__(self):
        self.hits = Counter()

    async def handle(self, request):
        path = request.path
        self.hits[path] += 1
        n = self.hits[path]
        if path == "/404":
            return web.Response(status=404)
        if path == "/403":
            return web.Response(status=403)
        if path == "/flaky":                  # 503, 503, потом нормально
            return web.Response(status=503) if n <= 2 else web.Response(text="ok")
        if path == "/always503":
            return web.Response(status=503)
        if path == "/always500":
            return web.Response(status=500)
        if path == "/too-many":               # 429 с Retry-After: 1
            if n == 1:
                return web.Response(status=429, headers={"Retry-After": "1"})
            return web.Response(text="ok")
        if path == "/slow":                   # первые два ответа слишком медленные
            if n <= 2:
                await asyncio.sleep(0.8)
            return web.Response(text="ok")
        if path == "/drop":
            request.transport.close()
            return web.Response(text="x")
        return web.Response(text="<h1>ok</h1>", content_type="text/html")

    async def start(self):
        app = web.Application()
        app.router.add_get("/{tail:.*}", self.handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def stop(self):
        await self.runner.cleanup()


def fast_crawler(**kwargs):
    """Краулер с короткими паузами, чтобы тесты шли быстро."""
    strategy = kwargs.pop("retry_strategy", None) or RetryStrategy(max_retries=3, base_delay=0.05, jitter=0)
    return AsyncCrawler(retry_strategy=strategy, **kwargs)


async def test_classification():
    section("Классификация ошибок")
    assert isinstance(classify_status(404), PermanentError)
    assert isinstance(classify_status(403), PermanentError)
    assert isinstance(classify_status(401), PermanentError)
    assert isinstance(classify_status(503), TransientError)
    assert isinstance(classify_status(429), TransientError)
    assert isinstance(classify_status(500), TransientError)
    print("404/403/401 постоянные, 503/429/500 временные")


async def test_timeout_retry(server):
    section("Повторы при таймауте")
    server.hits.clear()
    async with fast_crawler(read_timeout=0.4, connect_timeout=2) as crawler:
        body = await crawler.fetch_url(server.base + "/slow")
    assert body == "ok", body
    assert server.hits["/slow"] == 3, server.hits["/slow"]
    print(f"/slow: сервер отвечал медленно два раза, на третий успех. Запросов: {server.hits['/slow']}")

    server.hits.clear()
    strategy = RetryStrategy(max_retries=1, base_delay=0.05, jitter=0, timeout_growth=1.0)
    async with fast_crawler(read_timeout=0.4, connect_timeout=2, retry_strategy=strategy) as crawler:
        assert await crawler.fetch_url(server.base + "/slow") is None
        assert isinstance(crawler.error_details[server.base + "/slow"], TransientError)
    print("Если таймаут не растёт, страница так и не загрузилась: TransientError")


async def test_503_retry(server):
    section("Повторы при 503")
    server.hits.clear()
    async with fast_crawler() as crawler:
        assert await crawler.fetch_url(server.base + "/flaky") == "ok"
        assert crawler.retries_done == 2
    assert server.hits["/flaky"] == 3
    print("/flaky: 503, 503, потом успех: 3 запроса, 2 повтора")

    server.hits.clear()
    async with fast_crawler() as crawler:
        assert await crawler.fetch_url(server.base + "/always503") is None
        assert isinstance(crawler.error_details[server.base + "/always503"], TransientError)
    assert server.hits["/always503"] == 4          # первая попытка и 3 повтора
    print("/always503: 1 попытка + 3 повтора, потом сдались")


async def test_no_retry_on_404(server):
    section("Нет повторов при 404 и 403")
    server.hits.clear()
    async with fast_crawler() as crawler:
        for path in ("/404", "/403"):
            assert await crawler.fetch_url(server.base + path) is None
            assert isinstance(crawler.error_details[server.base + path], PermanentError)
            assert server.hits[path] == 1, (path, server.hits[path])
        assert crawler.retries_done == 0
        stats = crawler.get_error_stats()
        assert stats["permanent_urls"] == [server.base + "/404", server.base + "/403"]
    print("404 и 403: по одному запросу, повторов нет, адреса записаны как постоянные ошибки")


async def test_limits_and_429(server):
    section("Ограничения повторов и 429")
    server.hits.clear()
    strategy = RetryStrategy(max_retries=3, base_delay=0.05, jitter=0, max_retries_by_status={500: 1})
    async with fast_crawler(retry_strategy=strategy) as crawler:
        assert await crawler.fetch_url(server.base + "/always500") is None
    assert server.hits["/always500"] == 2          # первая попытка и всего один повтор
    print("500: только один повтор")

    server.hits.clear()
    async with fast_crawler() as crawler:
        started = time.monotonic()
        assert await crawler.fetch_url(server.base + "/too-many") == "ok"
        elapsed = time.monotonic() - started
    assert elapsed >= 1.0, elapsed                 # подождали, сколько просил сервер
    print(f"429 с Retry-After: 1: подождали {elapsed:.2f} с и получили страницу")

    server.hits.clear()
    strategy = RetryStrategy(retry_on=[NetworkError], base_delay=0.05, jitter=0)
    async with fast_crawler(retry_strategy=strategy) as crawler:
        assert await crawler.fetch_url(server.base + "/always503") is None
    assert server.hits["/always503"] == 1
    print("Если повторять только NetworkError, то 503 не повторяется")


async def test_network_error(server):
    section("Сетевые ошибки")
    async with fast_crawler() as crawler:
        assert await crawler.fetch_url("http://127.0.0.1:1/") is None
        assert await crawler.fetch_url(server.base + "/drop") is None
        for error in crawler.error_details.values():
            assert isinstance(error, NetworkError), error
        assert crawler.retries_done == 6
    print("Отказ в соединении и обрыв связи: NetworkError, по 3 повтора")


async def test_backoff():
    section("Экспоненциальный backoff")
    strategy = RetryStrategy(max_retries=4, backoff_factor=2.0, base_delay=0.05, jitter=0)
    delays = []
    strategy.on_retry = lambda error, delay, retry_number: delays.append(round(delay, 3))
    calls = []

    async def always_failing(url):
        calls.append(time.monotonic())
        raise TransientError("сломалось", url=url)

    try:
        await strategy.execute_with_retry(always_failing, "http://x.test/")
        assert False, "должна быть ошибка"
    except TransientError:
        pass
    assert delays == [0.05, 0.1, 0.2, 0.4], delays
    gaps = [b - a for a, b in zip(calls, calls[1:])]
    for gap, expected in zip(gaps, delays):
        assert gap >= expected - 0.005, (gap, expected)
    print("Паузы между попытками:", delays, "(каждая вдвое длиннее предыдущей)")

    strategy = RetryStrategy(max_retries=4, base_delay=1.0, max_delay=3.0, jitter=0)
    error = TransientError("сбой", url="http://x.test/")
    assert [strategy.delay_for(error, i) for i in range(4)] == [1.0, 2.0, 3.0, 3.0]
    print("Пауза ограничена сверху параметром max_delay")


async def test_per_type_backoff():
    section("Свои настройки для типов ошибок")
    strategy = RetryStrategy(max_retries=3, base_delay=0.05, jitter=0,
                             max_retries_by_type={NetworkError: 1})
    attempts = {"network": 0, "transient": 0}

    async def network_down(url):
        attempts["network"] += 1
        raise NetworkError("нет связи", url=url)

    async def transient(url):
        attempts["transient"] += 1
        raise TransientError("сбой", url=url)

    for func in (network_down, transient):
        try:
            await strategy.execute_with_retry(func, "http://x.test/")
        except (NetworkError, TransientError):
            pass
    assert attempts == {"network": 2, "transient": 4}, attempts
    print("Для NetworkError разрешён 1 повтор, для TransientError 3:", attempts)


async def test_statistics(server):
    section("Статистика ошибок")
    server.hits.clear()
    async with fast_crawler() as crawler:
        for path in ("/ok", "/404", "/flaky", "/always503"):
            await crawler.fetch_url(server.base + path)
        stats = crawler.get_error_stats()
    assert stats["calls"] == 4
    assert stats["errors_by_type"] == {"PermanentError": 1, "TransientError": 2 + 4}, stats["errors_by_type"]
    assert stats["retries"] == 5                    # 2 у /flaky и 3 у /always503
    assert stats["successful_retries"] == 1         # только /flaky починился
    assert stats["failed_after_retries"] == 1
    assert stats["permanent_urls"] == [server.base + "/404"]
    assert stats["failed_count"] == 2
    assert stats["avg_retry_delay"] > 0
    print("Типы ошибок:", stats["errors_by_type"])
    print(f"Повторов {stats['retries']}, удачных {stats['successful_retries']}, "
          f"средняя пауза {stats['avg_retry_delay']:.2f} с")

    path = os.path.join(tempfile.mkdtemp(), "error_report.txt")
    crawler.save_error_report(path)
    with open(path, encoding="utf-8") as file:
        text = file.read()
    assert "ОТЧЁТ ОБ ОШИБКАХ" in text and server.base + "/404" in text and "TransientError" in text
    print("Отчёт сохранён в файл и содержит нужные адреса")


async def test_circuit_breaker(server):
    section("Предохранитель")
    server.hits.clear()

    class Clock:
        now = 0.0

        def __call__(self):
            return self.now

    clock = Clock()
    breaker = CircuitBreaker(failure_rate=0.5, min_calls=3, window_size=4, recovery_timeout=30, clock=clock)
    async with AsyncCrawler(max_retries=0, circuit_breaker=breaker) as crawler:
        for i in range(3):
            await crawler.fetch_url(f"{server.base}/always503?n={i}")
        before = sum(server.hits.values())
        assert await crawler.fetch_url(server.base + "/ok") is None
        assert isinstance(crawler.error_details[server.base + "/ok"], CircuitOpenError)
        assert sum(server.hits.values()) == before, "заблокированный запрос не должен доходить до сервера"
        print("После трёх ошибок подряд домен заблокирован, запрос до сервера не дошёл")
        clock.now += 31
        assert await crawler.fetch_url(server.base + "/ok") is not None
        print("Через 30 с пробный запрос удался, домен снова открыт")

    breaker = CircuitBreaker(min_calls=3, window_size=3, clock=clock)
    async with AsyncCrawler(max_retries=0, circuit_breaker=breaker) as crawler:
        for _ in range(6):
            await crawler.fetch_url(server.base + "/404")
        assert breaker.get_stats()["opened_total"] == 0
    print("Шесть ответов 404 подряд предохранитель не включают: сайт же отвечает")


async def test_parse_error(server):
    section("Ошибка разбора страницы")

    class BrokenParser:
        async def parse_html(self, html, url):
            raise RuntimeError("сломанный парсер")

    async with fast_crawler() as crawler:
        crawler._parser = BrokenParser()
        page = await crawler.fetch_and_parse(server.base + "/ok")
        assert isinstance(crawler.error_details[server.base + "/ok"], ParseError)
        assert page["links"] == [] and page["errors"]
    print("Сломанный разбор: ParseError записан, краулер не упал")


async def test_crawl_with_errors(server):
    section("Обход сайта с ошибками")
    server.hits.clear()

    async with fast_crawler(max_depth=1) as crawler:
        results = await crawler.crawl([server.base + "/ok"], progress=False)
        assert len(results) == 1
    print("Обход с новой стратегией работает как раньше")


async def main():
    server = LocalServer()
    await server.start()
    try:
        await test_classification()
        await test_timeout_retry(server)
        await test_503_retry(server)
        await test_no_retry_on_404(server)
        await test_limits_and_429(server)
        await test_network_error(server)
        await test_backoff()
        await test_per_type_backoff()
        await test_statistics(server)
        await test_circuit_breaker(server)
        await test_parse_error(server)
        await test_crawl_with_errors(server)
    finally:
        await server.stop()
    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    asyncio.run(main())