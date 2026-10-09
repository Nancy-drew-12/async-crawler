"""Демонстрация вежливого краулинга на маленьком сайте, который запускается на этом же компьютере.

У сайта есть robots.txt: закрыты /private/ и /admin, а между запросами нужно ждать 1 секунду.
"""
import asyncio
import time

from aiohttp import web

from src.async_crawler import AsyncCrawler

ROBOTS = """User-agent: *
Disallow: /private/
Disallow: /admin
Crawl-delay: 1

User-agent: BadBot
Disallow: /
"""

PAGES = {
    "/":          '<h1>Главная</h1> <a href="/about">О нас</a> <a href="/blog">Блог</a> '
                  '<a href="/private/secret">Секреты</a> <a href="/admin/panel">Админка</a>',
    "/about":     '<h1>О нас</h1> <a href="/contacts">Контакты</a>',
    "/blog":      '<h1>Блог</h1> <a href="/blog/1">Пост 1</a> <a href="/blog/2">Пост 2</a>',
    "/blog/1":    "<h1>Пост 1</h1>",
    "/blog/2":    "<h1>Пост 2</h1>",
    "/contacts":  "<h1>Контакты</h1>",
}

request_times = []       # когда сервер получил запрос на обычную страницу


async def handle(request):
    if request.path == "/robots.txt":
        return web.Response(text=ROBOTS)
    request_times.append(time.monotonic())
    return web.Response(text=PAGES.get(request.path, "закрытая страница"), content_type="text/html")


async def run_crawl(title, base, **settings):
    print(f"\n{'=' * 8} {title} {'=' * 8}")
    request_times.clear()
    crawler = AsyncCrawler(max_concurrent=5, max_depth=2, **settings)
    started = time.monotonic()
    try:
        results = await crawler.crawl([base + "/"], same_domain_only=True, progress_interval=1.0)
    finally:
        await crawler.close()
    elapsed = time.monotonic() - started

    gaps = [b - a for a, b in zip(request_times, request_times[1:])]
    stats = crawler.get_politeness_stats()
    print(f"\nСтраниц загружено: {len(results)} за {elapsed:.1f} с")
    for url in results:
        print("   ", url.replace(base, "") or "/")
    if crawler.robots.blocked_urls:
        print("Запрещено robots.txt (на эти страницы краулер не ходил):")
        for url in sorted(crawler.robots.blocked_urls):
            print("   ", url.replace(base, ""))
    if gaps:
        print(f"Паузы между запросами на сервере: минимум {min(gaps):.2f} с, в среднем {sum(gaps) / len(gaps):.2f} с")
    print(f"Скорость: {stats['current_rps']:.1f} запросов/с | средняя пауза: {stats['avg_delay']:.2f} с | "
          f"заблокировано robots.txt: {stats['blocked_by_robots']} | повторов: {stats['retries']}")


async def main():
    app = web.Application()
    app.router.add_get("/{tail:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    try:
        await run_crawl("1. Без вежливости: быстро, но ходит куда нельзя", base)
        await run_crawl("2. Вежливо: robots.txt + лимит скорости", base,
                        requests_per_second=5, respect_robots=True, min_delay=0.2,
                        user_agent="KateCrawler/1.0 (study project)")
        await run_crawl("3. Бот, которому robots.txt запрещает всё", base,
                        respect_robots=True, user_agent="BadBot/1.0")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())