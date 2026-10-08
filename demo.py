import asyncio
import logging
import time

from src.async_crawler import AsyncCrawler

logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s | %(levelname)s | %(message)s")

URLS = [
    "https://example.com",
    "https://www.python.org",
    "https://httpbin.org/html",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/3",
    "https://httpbin.org/status/404",
]
assert len(set(URLS)) == len(URLS), "в URLS не должно быть повторов"


async def sequential(urls):
    crawler = AsyncCrawler()
    start = time.perf_counter()
    results = {}
    for url in urls:
        body = await crawler.fetch_url(url)
        if body is not None:
            results[url] = body
    elapsed = time.perf_counter() - start
    errors = dict(crawler.errors)
    await crawler.close()
    return elapsed, results, errors


async def parallel(urls):
    crawler = AsyncCrawler(max_concurrent=10)
    start = time.perf_counter()
    results = await crawler.fetch_urls(urls)
    elapsed = time.perf_counter() - start
    errors = dict(crawler.errors)
    await crawler.close()
    return elapsed, results, errors


async def main():
    print(f"Адресов: {len(URLS)} (все уникальные), один и тот же набор для обоих вариантов\n")

    t_seq, res_seq, err_seq = await sequential(URLS)
    t_par, res_par, err_par = await parallel(URLS)

    print(f"Последовательно: {t_seq:5.2f} с | загружено {len(res_seq)}, ошибок {len(err_seq)}")
    print(f"Параллельно:     {t_par:5.2f} с | загружено {len(res_par)}, ошибок {len(err_par)}")
    print(f"Ускорение: в {t_seq / t_par:.1f} раза")
    print("Ошибки:", err_par)


asyncio.run(main())