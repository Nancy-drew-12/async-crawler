import asyncio
from src.async_crawler import AsyncCrawler


async def main():
    crawler = AsyncCrawler(max_concurrent=5, max_depth=1, max_per_domain=2, connect_timeout=15)
    try:
        results = await crawler.crawl(["https://docs.python.org/3/"], max_pages=10, same_domain_only=True)
        print("Обработано:", len(results))
        for url, page in results.items():
            print(" ", url, "| ссылок:", len(page["links"]))
        print("Ошибки:", crawler.failed_urls)
        print("Статистика:", crawler.get_crawl_stats())
    finally:
        await crawler.close()




asyncio.run(main())