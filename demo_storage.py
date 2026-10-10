"""Демонстрация сохранения данных: краулер обходит маленький сайт (он запускается на этом же компьютере)
и сохраняет страницы в JSON, CSV и SQLite. Потом файлы читаются обратно, и выводится статистика.
Результаты лежат в папке demo_output/.
"""
import asyncio
import os
import time

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.csv_storage import CSVStorage, read_csv
from src.json_storage import JSONStorage, read_json
from src.sqlite_storage import SQLiteStorage

OUTPUT_DIR = "demo_output"

PAGES = {
    "/": '<title>Главная</title><h1>Магазин книг</h1><p>Привет, мир! Это "главная" страница.</p>'
         '<a href="/books">Книги</a> <a href="/about">О нас</a> <a href="/old-page">Старая страница</a>',
    "/books": '<title>Книги</title><h1>Каталог</h1><p>Три книги, и все интересные.</p>'
              '<a href="/books/1">Первая</a> <a href="/books/2">Вторая</a> <a href="/books/3">Третья</a>',
    "/books/1": "<title>Книга 1</title><p>Первая книга.\nВторая строка описания.</p>",
    "/books/2": "<title>Книга 2</title><p>Вторая книга; с точкой с запятой, запятой и «кавычками».</p>",
    "/books/3": "<title>Книга 3</title><p>Третья книга 📚</p>",
    "/about": "<title>О нас</title><p>Мы небольшой магазин.</p>",
}


async def handle(request):
    if request.path in PAGES:
        return web.Response(text=PAGES[request.path], content_type="text/html")
    return web.Response(status=404)


async def crawl_into(storage, base):
    """Обходит сайт, сохраняя страницы в переданное хранилище."""
    started = time.perf_counter()
    async with AsyncCrawler(max_concurrent=4, max_depth=3, max_retries=0, storage=storage) as crawler:
        await crawler.crawl([base + "/"], same_domain_only=True, progress=False)
        stats = crawler.get_storage_stats()
        failed = dict(crawler.failed_urls)
    return stats, failed, time.perf_counter() - started


def show(title, records, base):
    print(f"Прочитано из {title}: {len(records)} страниц")
    for record in sorted(records, key=lambda r: r["url"]):
        print(f"    {record['url'].replace(base, '') or '/':12} код {record['status_code']}  "
              f"{record['content_type']:10} ссылок: {len(record['links'])}  текст: {len(record['text'])} симв.")


async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    app = web.Application()
    app.router.add_get("/{tail:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"

    json_path = os.path.join(OUTPUT_DIR, "pages.json")
    csv_path = os.path.join(OUTPUT_DIR, "pages.csv")
    db_path = os.path.join(OUTPUT_DIR, "pages.db")
    for path in (json_path, csv_path, db_path):
        if os.path.exists(path):
            os.remove(path)

    try:
        print("=== 1. Обход сайта и сохранение в три формата ===")
        for name, storage in (("JSON", JSONStorage(json_path, indent=2, buffer_size=2)),
                              ("CSV", CSVStorage(csv_path, encoding="utf-8-sig", buffer_size=2)),
                              ("SQLite", SQLiteStorage(db_path, batch_size=2))):
            stats, failed, seconds = await crawl_into(storage, base)
            print(f"{name:7} сохранено: {stats['saved']}, не удалось: {stats['failed']}, "
                  f"не загрузилось: {len(failed)}, {seconds:.2f} с")
        print("Не загрузилась страница /old-page: сайт ответил 404, её сохранять нечего.")

        print("\n=== 2. Размеры файлов ===")
        for path in (json_path, csv_path, db_path):
            print(f"{os.path.basename(path):12} {os.path.getsize(path):6} байт")

        print("\n=== 3. Чтение сохранённых данных ===")
        json_records = await read_json(json_path)
        show("JSON", json_records, base)
        csv_records = await read_csv(csv_path, encoding="utf-8-sig")
        show("CSV", csv_records, base)
        async with SQLiteStorage(db_path) as db:
            show("SQLite", await db.find(limit=100), base)

            print("\n=== 4. Поиск и статистика в базе ===")
            print("Страниц в базе:", await db.count())
            print("Статистика по базе:", await db.get_db_stats())
            found = await db.find(title_contains="Книга")
            print("Заголовок содержит «Книга»:", sorted(r["url"].replace(base, "") for r in found))
            book = await db.get(base + "/books/2")
            print("Страница /books/2, текст:", book["text"])

        print("\n=== 5. Данные в трёх форматах одинаковые? ===")
        same = (sorted(r["url"] for r in json_records) == sorted(r["url"] for r in csv_records)
                and {r["url"]: r["text"] for r in json_records} == {r["url"]: r["text"] for r in csv_records})
        print("JSON и CSV содержат одни и те же адреса и тексты:", same)
        print(f"\nФайлы лежат в папке {OUTPUT_DIR}/: можно открыть pages.csv в Excel, pages.json в редакторе.")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())