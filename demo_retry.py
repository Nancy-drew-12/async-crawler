"""Демонстрация обработки ошибок: на маленьком сайте (он запускается на этом же компьютере)
есть страницы с разными поломками. Краулер сам повторяет то, что имеет смысл повторить,
а в конце показывает статистику и сохраняет отчёт в error_report.txt.
"""
import asyncio
from collections import Counter

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.circuit_breaker import CircuitBreaker
from src.retry import RetryStrategy

hits = Counter()


async def handle(request):
    path = request.path
    hits[path] += 1
    n = hits[path]                                   # какой по счёту запрос к этой странице
    if path == "/404":
        return web.Response(status=404)
    if path == "/403":
        return web.Response(status=403)
    if path == "/flaky":                             # два раза 503, на третий раз всё хорошо
        return web.Response(status=503) if n <= 2 else web.Response(text="починился")
    if path == "/always503":
        return web.Response(status=503)
    if path == "/always500":
        return web.Response(status=500)
    if path == "/too-many":                          # 429 с просьбой подождать секунду
        if n == 1:
            return web.Response(status=429, headers={"Retry-After": "1"})
        return web.Response(text="лимит снят")
    if path == "/slow":                              # первые два раза отвечает слишком долго
        if n <= 2:
            await asyncio.sleep(0.8)
        return web.Response(text="наконец-то")
    if path == "/drop":                              # сервер обрывает соединение
        request.transport.close()
        return web.Response(text="не дойдёт")
    return web.Response(text="<h1>Всё хорошо</h1>", content_type="text/html")


async def main():
    app = web.Application()
    app.router.add_get("/{tail:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"

    pages = ["/ok", "/404", "/403", "/flaky", "/too-many", "/slow", "/always503", "/always500", "/drop"]
    urls = [base + p for p in pages] + ["http://127.0.0.1:1/недоступный-сайт"]

    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.2, jitter=0.1,
                             max_retries_by_status={500: 1})
    print("=== Часть 1. Разные ошибки и автоматические повторы ===")
    print("Максимум 3 повтора, для ответа 500 только 1, начальная пауза 0.2 с\n")
    async with AsyncCrawler(max_retries=3, read_timeout=0.4, connect_timeout=2,
                            retry_strategy=strategy) as crawler:
        results = await crawler.fetch_urls(urls)

        print("\n=== Что получилось ===")
        for url in urls:
            short = url.replace(base, "") or "/"
            print(f"{'удалось' if url in results else 'не удалось':11} {short}")

        print("\n=== Отчёт ===")
        print(crawler.format_error_report())
        path = crawler.save_error_report("error_report.txt")
        print(f"Отчёт сохранён в файл {path}")

    print("\n=== Часть 2. Предохранитель: сайт стабильно ломается ===")
    hits.clear()
    breaker = CircuitBreaker(failure_rate=0.5, min_calls=3, window_size=4, recovery_timeout=30)
    async with AsyncCrawler(max_retries=0, circuit_breaker=breaker) as crawler:
        for i in range(6):
            await crawler.fetch_url(f"{base}/always503?n={i}")
        print(f"\nВсего шесть запросов, до сервера дошло только {sum(hits.values())}: "
              f"остальные предохранитель остановил сам")
        print(crawler.format_error_report())
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())