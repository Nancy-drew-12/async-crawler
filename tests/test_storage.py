import asyncio
import csv
import json
import os
import sqlite3
import sys
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from aiohttp import web

from src.async_crawler import AsyncCrawler
from src.csv_storage import CSVStorage, read_csv
from src.json_storage import JSONStorage, read_json
from src.sqlite_storage import SQLiteStorage
from src.storage import DataStorage, MemoryStorage, StorageError, make_record, validate_record

TRICKY_TEXT = 'Запятая, "кавычки"; точка с запятой\nновая строка и эмодзи 😀'

PAGES = {
    "/": '<a href="/a">a</a> <a href="/b">b</a> <a href="/missing">m</a>',
    "/a": '<a href="/c">c</a>',
    "/b": "страница b",
    "/c": "страница c",
}


def section(title):
    print(f"\n=== {title} ===")


def make_page(number, host="example.test", status=200, text="Пример текста", title=None):
    page = {"url": f"http://{host}/{number}", "title": title or f"Страница {number}", "text": text,
            "links": ["http://example.test/a", "http://example.test/b"], "metadata": {"номер": number, "ключ": "значение"}}
    return make_record(page, status_code=status, content_type="text/html")


async def handle(request):
    if request.path in PAGES:
        return web.Response(text=PAGES[request.path], content_type="text/html")
    return web.Response(status=404)


class LocalServer:
    async def start(self):
        app = web.Application()
        app.router.add_get("/{tail:.*}", handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def stop(self):
        await self.runner.cleanup()


def test_record_format():
    section("Формат записи")
    record = make_record({"url": "http://a.test/", "title": "T", "links": ["http://a.test/x", {"url": "http://a.test/y"}, None],
                          "images": ["лишнее"]}, status_code=200, content_type="text/html")
    assert list(record) == ["url", "title", "text", "links", "metadata", "crawled_at", "status_code", "content_type"]
    assert record["links"] == ["http://a.test/x", "http://a.test/y"] and record["text"] == ""
    validate_record(record)
    for bad in ({}, {"url": ""}, None):
        try:
            make_record(bad)
            raise AssertionError("ожидалась ошибка")
        except ValueError:
            pass
    try:
        validate_record({"url": "x"})
        raise AssertionError("ожидалась ошибка")
    except ValueError:
        pass
    try:
        DataStorage()
        raise AssertionError("ожидалась ошибка")
    except TypeError:
        pass
    print("Запись содержит все поля, лишнее отбрасывается, плохие данные отклоняются")


async def test_json(tmp):
    section("Сохранение в JSON")
    path = os.path.join(tmp, "a.json")
    async with JSONStorage(path, buffer_size=3) as storage:
        for number in range(7):
            await storage.save(make_page(number, text=TRICKY_TEXT if number == 3 else "текст"))
        assert storage.get_stats()["buffered"] == 1
    with open(path, encoding="utf-8") as file:
        data = json.load(file)                       # обычный json.load читает файл
    assert [r["metadata"]["номер"] for r in data] == list(range(7)) and data[3]["text"] == TRICKY_TEXT
    records = await read_json(path)
    assert records[0]["crawled_at"].year >= 2026 and records[0]["links"] == ["http://example.test/a", "http://example.test/b"]
    print("Массив из 7 записей, кириллица и спецсимволы сохранены, дата читается как дата")

    async with JSONStorage(path, append=True, buffer_size=2) as storage:
        for number in range(10, 13):
            await storage.save(make_page(number))
    assert len(await read_json(path)) == 10
    print("Дозапись: 10 записей, файл остался корректным JSON")

    pretty = os.path.join(tmp, "pretty.json")
    async with JSONStorage(pretty, indent=2) as storage:
        await storage.save(make_page(1))
    with open(pretty, encoding="utf-8") as file:
        assert '\n  "url"' in file.read()
    lines = os.path.join(tmp, "a.jsonl")
    async with JSONStorage(lines, lines=True, buffer_size=2) as storage:
        for number in range(5):
            await storage.save(make_page(number))
    with open(lines, encoding="utf-8") as file:
        rows = file.read().splitlines()
    assert len(rows) == 5 and json.loads(rows[0])["url"].endswith("/0")
    assert len(await read_json(lines)) == 5
    print("Красивый вывод (indent) и построчный формат (JSON Lines) работают")

    empty = os.path.join(tmp, "empty.json")
    async with JSONStorage(empty):
        pass
    with open(empty) as file:
        assert json.load(file) == []
    print("Пустое хранилище даёт корректный пустой массив []")


async def test_csv(tmp):
    section("Сохранение в CSV")
    path = os.path.join(tmp, "a.csv")
    async with CSVStorage(path, buffer_size=3) as storage:
        for number in range(7):
            await storage.save(make_page(number, text=TRICKY_TEXT if number == 3 else "текст"))
    with open(path, encoding="utf-8", newline="") as file:
        header = next(csv.reader(file))
    assert header == ["url", "title", "text", "links", "metadata", "crawled_at", "status_code", "content_type"]
    rows = await read_csv(path)
    assert len(rows) == 7 and rows[3]["text"] == TRICKY_TEXT
    assert rows[0]["links"] == ["http://example.test/a", "http://example.test/b"] and rows[0]["status_code"] == 200
    assert rows[0]["metadata"] == {"номер": 0, "ключ": "значение"}
    print("Заголовок определён сам; запятые, кавычки, переносы строк и эмодзи сохранены")

    async with CSVStorage(path, append=True) as storage:
        await storage.save(make_page(99))
    with open(path, encoding="utf-8") as file:
        assert file.read().count("url,title,text") == 1
    assert len(await read_csv(path)) == 8
    print("Дозапись: заголовок не дублируется")

    excel = os.path.join(tmp, "excel.csv")
    async with CSVStorage(excel, encoding="utf-8-sig", delimiter=";") as storage:
        await storage.save(make_page(1))
    async with CSVStorage(excel, encoding="utf-8-sig", delimiter=";", append=True) as storage:
        await storage.save(make_page(2))
    with open(excel, "rb") as file:
        raw = file.read()
    assert raw.startswith(b"\xef\xbb\xbf") and raw.count(b"\xef\xbb\xbf") == 1
    assert len(await read_csv(excel, encoding="utf-8-sig", delimiter=";")) == 2
    cp = os.path.join(tmp, "cp1251.csv")
    async with CSVStorage(cp, encoding="cp1251") as storage:
        await storage.save(make_page(1, text="привет 😀"))
    assert (await read_csv(cp, encoding="cp1251"))[0]["text"] == "привет ?"
    print("Кодировки: utf-8-sig для Excel, cp1251 (символы вне кодировки заменяются на «?»)")

    big = os.path.join(tmp, "big.csv")
    text = "слово " * 100_000
    async with CSVStorage(big) as storage:
        await storage.save(make_page(1, text=text))
    assert (await read_csv(big))[0]["text"] == text
    print("Очень длинный текст (600 тысяч символов) читается обратно")


async def test_sqlite(tmp):
    section("Сохранение в SQLite")
    path = os.path.join(tmp, "a.db")
    async with SQLiteStorage(path, batch_size=4) as storage:
        await storage.init_db()
        await storage.init_db()                      # повторный вызов безопасен
        for number in range(10):
            await storage.save(make_page(number, text=TRICKY_TEXT if number == 3 else "текст"))
        assert storage.get_stats()["buffered"] == 2
        assert await storage.count() == 10           # чтение сначала дописывает буфер
        page = await storage.get("http://example.test/3")
        assert page["text"] == TRICKY_TEXT and page["metadata"] == {"номер": 3, "ключ": "значение"}
        assert page["links"] == ["http://example.test/a", "http://example.test/b"] and page["crawled_at"].year >= 2026
        assert await storage.get("http://example.test/нет") is None
        print("Вставка пачками, чтение по адресу, спецсимволы и списки сохранены")

        await storage.save(make_page(3, title="Новый заголовок"))
        await storage.flush()
        assert await storage.count() == 10 and (await storage.get("http://example.test/3"))["title"] == "Новый заголовок"
        print("Повторное сохранение того же адреса обновляет запись, дублей нет")

        for number in range(3):
            await storage.save(make_page(number, host="other.test", status=404 if number else 200))
        assert len(await storage.find(domain="OTHER.test")) == 3 and len(await storage.find(status_code=404)) == 2
        assert len(await storage.find(title_contains="Новый")) == 1 and len(await storage.find(limit=4)) == 4
        stats = await storage.get_db_stats()
        assert stats["pages"] == 13 and stats["by_domain"] == {"example.test": 10, "other.test": 3}
        assert stats["by_status"] == {200: 11, 404: 2}
        print("Поиск по домену, коду ответа и заголовку; статистика по базе:", stats["by_status"])

    connection = sqlite3.connect(path)
    indexes = {row[1] for row in connection.execute("PRAGMA index_list(pages)")}
    assert {"idx_pages_domain", "idx_pages_status", "idx_pages_crawled_at"} <= indexes
    for where, index in (("domain = 'example.test'", "idx_pages_domain"), ("status_code = 404", "idx_pages_status"),
                         ("url = 'http://example.test/1'", "autoindex")):
        plan = connection.execute(f"EXPLAIN QUERY PLAN SELECT * FROM pages WHERE {where}").fetchall()[0][3]
        assert "USING INDEX" in plan and index in plan, plan
    connection.close()
    print("Индексы созданы, и база использует их при поиске")


async def test_batches(tmp):
    section("Производительность: пачки")
    timings = {}
    for batch in (1, 100):
        path = os.path.join(tmp, f"speed{batch}.db")
        started = time.perf_counter()
        async with SQLiteStorage(path, batch_size=batch) as storage:
            for number in range(1000):
                await storage.save(make_page(number))
        timings[batch] = time.perf_counter() - started
    print(f"1000 записей: по одной {timings[1]:.2f} с, пачками по 100: {timings[100]:.2f} с")
    assert timings[100] < timings[1] * 0.7

    path = os.path.join(tmp, "many.json")
    async with JSONStorage(path, buffer_size=7) as storage:
        await asyncio.gather(*(storage.save(make_page(number)) for number in range(200)))
        assert storage.flushes >= 200 // 7
    assert sorted(r["metadata"]["номер"] for r in await read_json(path)) == list(range(200))
    path = os.path.join(tmp, "many.csv")
    async with CSVStorage(path, buffer_size=7) as storage:
        await asyncio.gather(*(storage.save(make_page(number)) for number in range(200)))
    assert sorted(r["metadata"]["номер"] for r in await read_csv(path)) == list(range(200))
    path = os.path.join(tmp, "many.db")
    async with SQLiteStorage(path, batch_size=7) as storage:
        await asyncio.gather(*(storage.save(make_page(number)) for number in range(200)))
        assert await storage.count() == 200
    print("200 одновременных сохранений в каждый формат: ничего не потеряно и не перемешано")


async def test_integrity(tmp):
    section("Целостность данных")
    pages = [make_page(number, text=TRICKY_TEXT if number % 2 else "обычный текст") for number in range(20)]
    json_path, csv_path, db_path = (os.path.join(tmp, name) for name in ("i.json", "i.csv", "i.db"))
    async with JSONStorage(json_path) as j, CSVStorage(csv_path) as c, SQLiteStorage(db_path) as s:
        for page in pages:
            await j.save(page)
            await c.save(page)
            await s.save(page)
        from_sql = await s.find(limit=100)
    from_json, from_csv = await read_json(json_path), await read_csv(csv_path)
    for name, records in (("JSON", from_json), ("CSV", from_csv), ("SQLite", from_sql)):
        assert len(records) == 20, name
        for original, saved in zip(pages, sorted(records, key=lambda r: r["metadata"]["номер"])):
            for field in ("url", "title", "text", "links", "metadata", "status_code", "content_type"):
                assert saved[field] == original[field], (name, field)
            assert saved["crawled_at"] == original["crawled_at"], name
    print("Все 20 записей вернулись из JSON, CSV и SQLite без изменений, включая дату")


async def test_errors(tmp):
    section("Ошибки записи")
    missing = os.path.join(tmp, "нет_такой_папки", "x")
    for storage in (JSONStorage(missing + ".json", buffer_size=1), CSVStorage(missing + ".csv", buffer_size=1)):
        try:
            await storage.save(make_page(1))
            raise AssertionError("ожидалась ошибка")
        except StorageError as error:
            print("Ошибка записи:", str(error)[:60])
    for storage in (SQLiteStorage(missing + ".db"), SQLiteStorage(tmp)):
        try:
            await storage.init_db()
            raise AssertionError("ожидалась ошибка")
        except StorageError:
            pass
    junk = os.path.join(tmp, "junk.db")
    with open(junk, "w") as file:
        file.write("это не база данных" * 100)
    try:
        await SQLiteStorage(junk).init_db()
        raise AssertionError("ожидалась ошибка")
    except StorageError:
        pass
    for storage in (JSONStorage(os.path.join(tmp, "c1.json")), CSVStorage(os.path.join(tmp, "c1.csv")),
                    SQLiteStorage(os.path.join(tmp, "c1.db"))):
        await storage.close()
        await storage.close()                        # повторное закрытие допустимо
        try:
            await storage.save(make_page(1))
            raise AssertionError("ожидалась ошибка")
        except StorageError:
            pass
        try:
            await storage.save({"url": "x"})
            raise AssertionError("ожидалась ошибка")
        except (ValueError, StorageError):
            pass
    print("Недоступный путь, чужой файл, закрытое хранилище, неверная запись: всё даёт понятную ошибку")

    # сбой при записи не приводит к дублям при повторе
    path = os.path.join(tmp, "flaky.json")
    storage = JSONStorage(path, buffer_size=2)
    await storage.save(make_page(1))
    await storage._open()
    real_write, calls = storage._file.write, {"n": 0}

    async def flaky(data):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("диск полон")
        return await real_write(data)

    storage._file.write = flaky
    try:
        await storage.save(make_page(2))
        raise AssertionError("ожидалась ошибка")
    except StorageError:
        pass
    await storage.save(make_page(2))
    await storage.close()
    assert [r["metadata"]["номер"] for r in await read_json(path)] == [1, 2]
    print("После сбоя повторное сохранение не создаёт дублей")


async def test_crawler_integration(server, tmp):
    section("Интеграция с AsyncCrawler")
    expected = [server.base + path for path in ("/", "/a", "/b", "/c")]
    for name, storage in (("JSON", JSONStorage(os.path.join(tmp, "k.json"), buffer_size=2)),
                          ("CSV", CSVStorage(os.path.join(tmp, "k.csv"), buffer_size=2)),
                          ("SQLite", SQLiteStorage(os.path.join(tmp, "k.db"), batch_size=2))):
        crawler = AsyncCrawler(max_retries=0, max_depth=3, storage=storage)
        results = await crawler.crawl([server.base + "/"], progress=False)
        stats = crawler.get_storage_stats()
        await crawler.close()                        # закрывает и хранилище
        assert len(results) == 4 and stats["saved"] == 4 and stats["failed"] == 0, stats
        if name == "JSON":
            records = await read_json(os.path.join(tmp, "k.json"))
        elif name == "CSV":
            records = await read_csv(os.path.join(tmp, "k.csv"))
        else:
            async with SQLiteStorage(os.path.join(tmp, "k.db")) as db:
                records = await db.find()
        assert sorted(r["url"] for r in records) == sorted(expected), name
        assert {r["status_code"] for r in records} == {200} and {r["content_type"] for r in records} == {"text/html"}
        print(f"{name}: обход сохранил 4 страницы с кодом 200 и типом text/html (страница 404 не сохраняется)")

    memory = MemoryStorage(fail_times=2)
    async with AsyncCrawler(max_retries=0, storage=memory, storage_retries=3, storage_retry_delay=0.01) as crawler:
        await crawler.crawl([server.base + "/b"], progress=False)
        stats = crawler.get_storage_stats()
    assert (stats["saved"], stats["failed"], stats["retries"]) == (1, 0, 2) and len(memory.records) == 1
    print("Два сбоя записи, третья попытка удалась: повторов 2")

    memory = MemoryStorage(fail_times=1000)
    async with AsyncCrawler(max_retries=0, max_depth=3, storage=memory, storage_retries=2,
                            storage_retry_delay=0.01) as crawler:
        results = await crawler.crawl([server.base + "/"], progress=False)
        stats = crawler.get_storage_stats()
        report = crawler.format_error_report()
    assert len(results) == 4 and stats["failed"] == 4 and stats["retries"] == 8 and stats["saved"] == 0
    assert "Ошибки сохранения (4)" in report
    print("Запись сломана совсем: обход дошёл до конца, 4 ошибки сохранения попали в отчёт")

    for kwargs in ({"storage": "results.json"}, {"storage_retries": -1}, {"storage_retry_delay": -1}):
        try:
            AsyncCrawler(**kwargs)
            raise AssertionError("ожидалась ошибка")
        except (TypeError, ValueError):
            pass
    async with AsyncCrawler(max_retries=0) as crawler:
        assert crawler.get_storage_stats() == {}
    print("Неверные параметры отклоняются; без storage краулер работает как раньше")


class BufferedStorage(DataStorage):
    """Хранилище с буфером, у которого flush() сначала падает flush_failures раз. Имитирует сбой диска при записи."""

    def __init__(self, flush_failures=0):
        super().__init__()
        self.buffer, self.written = [], []
        self.flush_failures = flush_failures
        self.flush_calls = 0

    async def save(self, data):
        validate_record(data)
        self.buffer.append(dict(data))             # только в буфер, на диск пока не пишем

    async def flush(self):
        self.flush_calls += 1
        if self.flush_failures > 0:
            self.flush_failures -= 1
            raise StorageError("имитация сбоя записи буфера")
        self.written += self.buffer
        self.saved_count += len(self.buffer)
        self.buffer.clear()

    async def close(self):
        await self.flush()

    def get_stats(self):
        return {"saved": self.saved_count, "buffered": len(self.buffer)}


async def test_flush_failures(server, tmp):
    section("Сбои при записи буфера (замечание ментора)")

    # 1. «сохранено» растёт только после реальной записи
    storage = JSONStorage(os.path.join(tmp, "pending.json"), buffer_size=10)
    async with AsyncCrawler(max_retries=0, storage=storage) as crawler:
        for i in (1, 2):
            await crawler._save_page(f"http://x.test/{i}", make_page(i))
        stats = crawler.get_storage_stats()
        assert (stats["saved"], stats["pending"], stats["storage"]["saved"]) == (0, 2, 0), stats
        await crawler._flush_storage()
        stats = crawler.get_storage_stats()
        assert (stats["saved"], stats["pending"], stats["storage"]["saved"]) == (2, 0, 2), stats
    print("Пока записи лежат в буфере, они не считаются сохранёнными; после flush() считаются")

    # 2. flush() падает дважды, потом получается: повторы работают
    storage = BufferedStorage(flush_failures=2)
    async with AsyncCrawler(max_retries=0, max_depth=3, storage=storage, storage_retries=3,
                            storage_retry_delay=0.01) as crawler:
        await crawler.crawl([server.base + "/"], progress=False)
        stats = crawler.get_storage_stats()
    assert (stats["saved"], stats["failed"], stats["retries"], stats["pending"]) == (4, 0, 2, 0), stats
    assert len(storage.written) == 4 and not stats["errors"]
    print("flush() упал дважды и удался с третьей попытки: сохранено 4, повторов 2")

    # 3. настоящий JSONStorage по недоступному пути (пример из замечания)
    blocker = os.path.join(tmp, "blocker")
    open(blocker, "w").close()                     # обычный файл, внутрь которого создать файл нельзя
    storage = JSONStorage(os.path.join(blocker, "results.json"))
    async with AsyncCrawler(max_retries=0, max_depth=3, storage=storage, storage_retries=2,
                            storage_retry_delay=0.01) as crawler:
        results = await crawler.crawl([server.base + "/"], progress=False)
        stats = crawler.get_storage_stats()
        report = crawler.format_error_report()
    assert len(results) == 4
    assert (stats["saved"], stats["failed"], stats["retries"], stats["pending"]) == (0, 4, 2, 0), stats
    assert len(stats["errors"]) == 4 and stats["storage"]["saved"] == 0
    assert "Ошибки сохранения (4)" in report
    print("Файл не создан: сохранено 0, ошибок 4, повторов flush() 2, все адреса в отчёте")

    # 4. flush() не удаётся при обходе, но получается при закрытии: статистика исправляется
    storage = BufferedStorage(flush_failures=1000)
    crawler = AsyncCrawler(max_retries=0, max_depth=3, storage=storage, storage_retries=1,
                           storage_retry_delay=0.01)
    await crawler.crawl([server.base + "/"], progress=False)
    stats = crawler.get_storage_stats()
    assert (stats["saved"], stats["failed"]) == (0, 4), stats
    storage.flush_failures = 0                     # диск «починили»
    await crawler.close()
    stats = crawler.get_storage_stats()
    assert (stats["saved"], stats["failed"], len(stats["errors"])) == (4, 0, 0), stats
    assert len(storage.written) == 4
    print("Буфер удалось записать при закрытии: статистика исправлена (сохранено 4, ошибок 0)")


async def main():
    test_record_format()
    server = LocalServer()
    await server.start()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            await test_json(tmp)
            await test_csv(tmp)
            await test_sqlite(tmp)
            await test_batches(tmp)
            await test_integrity(tmp)
            await test_errors(tmp)
            await test_crawler_integration(server, tmp)
            await test_flush_failures(server, tmp)
    finally:
        await server.stop()
    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    asyncio.run(main())