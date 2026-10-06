import asyncio
import logging
import time

from src.async_crawler import AsyncCrawler

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")


URLS = [
    "https://example.com",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
]


async def sequential(urls):
    crawler = AsyncCrawler()
    start = time.perf_counter()
    for url in urls:
        await crawler.fetch_url(url)
    await crawler.close()
    return time.perf_counter() - start


async def parallel(urls):
    crawler = AsyncCrawler(max_concurrent=5)
    start = time.perf_counter()
    results = await crawler.fetch_urls(urls)
    await crawler.close()
    return time.perf_counter() - start, results, crawler.errors


async def main():
    t_seq = await sequential(URLS)
    t_par, results, errors = await parallel(URLS)
    print(f"Последовательно: {t_seq:.2f} с")
    print(f"Параллельно:     {t_par:.2f} с")
    print(f"Ускорение: в {t_seq / t_par:.1f} раза")
    print(f"Загружено {len(results)} из {len(set(URLS))} уникальных страниц")
    print("Ошибки:", errors)


asyncio.run(main())