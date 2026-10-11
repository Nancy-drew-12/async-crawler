"""Демонстрация из задания Дня 7: краулер целиком, настройки из config.yaml.

Запуск: python demo_advanced.py
Для настоящих сайтов замените адреса в demo_output/demo_config.yaml (или передайте свой --config в crawler.py).
"""

import asyncio
import os

from aiohttp import web

from crawler import AdvancedCrawler

PAGES = {
    "/":  '<title>Главная</title><a href="/books">Книги</a> <a href="/news">Новости</a>',
    "/books": '<title>Книги</title><a href="/books/1">1</a> <a href="/books/2">2</a>',
    "/books/1": '<title>Книга 1</title>текст',
    "/books/2": '<title>Книга 2</title>текст',
    "/news": '<title>Новости</title><a href="/news/1">1</a> <a href="/gone">битая</a>',
    "/news/1": '<title>Новость 1</title>текст',
    "/about": '<title>О нас</title>страница есть только в sitemap',
}


async def start_site():
    async def page(request):
        body = PAGES.get(request.path)
        if body is None:
            return web.Response(status=404, text="нет такой страницы")
        return web.Response(text=f"<html><head></head><body>{body}</body></html>", content_type="text/html")

    async def sitemap(request):
        base = f"http://127.0.0.1:{request.url.port}"
        xml = ('<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
               f'<url><loc>{base}/about</loc></url></urlset>')
        return web.Response(text=xml, content_type="application/xml")

    app = web.Application()
    app.router.add_get("/sitemap.xml", sitemap)
    app.router.add_get("/{tail:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, site._server.sockets[0].getsockname()[1]


def write_config(path, port):
    """Пишем настоящий YAML-файл, как это сделала бы ты сама."""
    text = f"""start_urls:
  - http://127.0.0.1:{port}/

sitemaps:
  - http://127.0.0.1:{port}/sitemap.xml

crawler:
  max_pages: 50
  max_depth: 3
  max_concurrent: 5
  rate_limit: 20
  max_retries: 1

storage:
  type: sqlite
  path: demo_output/demo/pages.db

logging:
  file: demo_output/demo/crawler.log
  level: INFO

report:
  json: demo_output/demo/stats.json
"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


async def main():
    runner, port = await start_site()
    write_config("demo_output/demo_config.yaml", port)
    try:
        # ---- ниже код из задания ----
        crawler = AdvancedCrawler.from_config("demo_output/demo_config.yaml")

        await crawler.crawl()

        stats = crawler.get_stats()
        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")

        crawler.export_to_html_report("demo_output/demo/report.html")
        await crawler.close()
        # -----------------------------
    finally:
        await runner.cleanup()
    print("\nОтчёт: demo_output/demo/report.html, база: demo_output/demo/pages.db, лог: demo_output/demo/crawler.log")


asyncio.run(main())