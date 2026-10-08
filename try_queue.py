import asyncio
from src.crawler_queue import CrawlerQueue


async def main():
    q = CrawlerQueue()
    print(q.add_url("a"), q.add_url("b", priority=5), q.add_url("c"), q.add_url("a"))
    order = []
    while (url := await q.get_next()) is not None:
        order.append(url)
        q.mark_processed(url)
    print(order)
    print(q.get_stats())


asyncio.run(main())