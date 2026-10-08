import asyncio
import json
from pathlib import Path

from src.async_crawler import AsyncCrawler
from src.url_filter import normalize_domain

START_URLS = ["https://docs.python.org/3/", "https://www.python.org/"]
OUTPUT = Path("output/crawl_result.json")


async def main():
    crawler = AsyncCrawler(max_concurrent=10, max_depth=2, max_per_domain=3, connect_timeout=15)
    try:
        results = await crawler.crawl(
            START_URLS,
            max_pages=30,
            same_domain_only=True,
            exclude_patterns=[r"\.(zip|pdf|gz|tar|exe|msi|dmg|pkg)$", r"/_sources/"],
            progress_interval=1.0,
        )
    finally:
        await crawler.close()

    # сколько страниц с каждого сайта
    by_domain = {}
    for url in results:
        domain = normalize_domain(url)
        by_domain[domain] = by_domain.get(domain, 0) + 1
    print("\nСтраниц по сайтам:", by_domain)
    print("Ошибки:", crawler.failed_urls)

    # сохраняем всё найденное в файл
    OUTPUT.parent.mkdir(exist_ok=True)
    data = {"stats": crawler.get_crawl_stats(), "failed": crawler.failed_urls, "pages": results}
    OUTPUT.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"Сохранено: {OUTPUT} ({OUTPUT.stat().st_size / 1024:.0f} КБ)")


if __name__ == "__main__":
    asyncio.run(main())