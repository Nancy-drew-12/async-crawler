import asyncio
import logging
import time

from src.async_crawler import AsyncCrawler

logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s | %(levelname)s | %(message)s")

URLS = [
    "https://example.com",
    "https://www.python.org",
    "https://en.wikipedia.org/wiki/Web_crawler",
    "https://docs.python.org/3/library/asyncio.html",
    "https://httpbin.org/html",
    "https://httpbin.org/status/404",
]


def summary(page: dict) -> dict:
    return {
        "url": page["url"],
        "title": page["title"],
        "text_length": len(page["text"]),
        "links_count": len(page["links"]),
        "images_count": len(page["images"]),
        "tables_count": len(page["tables"]),
        "errors": page["errors"],
    }


async def main():
    crawler = AsyncCrawler(max_concurrent=5)
    start = time.perf_counter()
    pages = await asyncio.gather(*(crawler.fetch_and_parse(u) for u in URLS))
    await crawler.close()
    total = time.perf_counter() - start

    for page in pages:
        print("-" * 60)
        for key, value in summary(page).items():
            print(f"{key:>13}: {value}")
        for link in page["links"][:3]:
            print(f"{'ссылка':>13}: {link}")

    ok = [p for p in pages if not p["errors"]]
    print("=" * 60)
    print(f"Страниц: {len(pages)}, без ошибок: {len(ok)}, за {total:.2f} с")
    print(f"Всего ссылок: {sum(len(p['links']) for p in pages)}, "
          f"всего символов текста: {sum(len(p['text']) for p in pages)}")


asyncio.run(main())