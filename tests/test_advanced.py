"""Итоговые тесты Дня 7: sitemap, статистика, конфиг, логи, монитор, AdvancedCrawler, командная строка."""

import asyncio
import glob
import gzip
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from aiohttp import web

from src.advanced_crawler import AdvancedCrawler
from src.config import ConfigError, from_dict, load_config
from src.crawler_stats import CrawlerStats
from src.log_setup import close_logging, setup_logging
from src.monitor import format_duration, format_progress, make_bar
from src.sitemap_parser import SitemapParser

TMP = tempfile.mkdtemp(prefix="crawler_test_")


def section(title):
    print(f"\n=== {title} ===")


# ---------- тестовый сайт ----------

PAGES = {
    "/":  '<title>Главная</title><a href="/a">a</a> <a href="/b">b</a>',
    "/a": '<title>A</title><a href="/c">c</a>',
    "/b": '<title>B</title><a href="/missing">битая</a>',
    "/c": '<title>C</title>конец',
    "/x": '<title>X</title>x',
    "/y": '<title>Y</title>y',
}


def urlset(base, paths):
    items = "".join(f"<url><loc>{base}{p}</loc></url>" for p in paths)
    return f'<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{items}</urlset>'


def sitemap_index(base, names):
    items = "".join(f"<sitemap><loc>{base}/sitemaps/{n}</loc></sitemap>" for n in names)
    return f'<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{items}</sitemapindex>'


async def sitemap_handler_root(request):
    base = f"http://127.0.0.1:{request.url.port}"
    return web.Response(text=urlset(base, ["/x", "/y"]), content_type="application/xml")


async def start_site():
    async def sitemap_handler(request):
        base = f"http://127.0.0.1:{request.url.port}"
        name = request.match_info["name"]
        if name == "index.xml":
            return web.Response(text=sitemap_index(base, ["s1.xml", "s2.xml.gz", "broken.xml", "missing.xml"]),
                                content_type="application/xml")
        if name == "s1.xml":
            return web.Response(text=urlset(base, ["/a", "/b"]), content_type="application/xml")
        if name == "s2.xml.gz":
            data = gzip.compress(urlset(base, ["/b", "/c"]).encode())
            return web.Response(body=data, content_type="application/gzip")
        if name == "broken.xml":
            return web.Response(text="<urlset><url>", content_type="application/xml")
        if name == "bomb.xml":
            bomb = '<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]><urlset><url><loc>&a;</loc></url></urlset>'
            return web.Response(text=bomb, content_type="application/xml")
        return web.Response(status=404, text="нет")

    async def page(request):
        body = PAGES.get(request.path)
        if body is None:
            return web.Response(status=404, text="нет такой страницы")
        return web.Response(text=f"<html><head></head><body>{body}</body></html>", content_type="text/html")

    app = web.Application()
    app.router.add_get("/sitemaps/{name}", sitemap_handler)
    app.router.add_get("/sitemap.xml", sitemap_handler_root)
    app.router.add_get("/{tail:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, site._server.sockets[0].getsockname()[1]


# ---------- тесты ----------

async def test_sitemap(base):
    section("1. SitemapParser")
    parser = SitemapParser()
    urls = await parser.fetch_sitemap(f"{base}/sitemap.xml")
    print("Обычный:", [u.replace(base, "") for u in urls])
    assert [u.replace(base, "") for u in urls] == ["/x", "/y"]

    urls = await parser.fetch_sitemap(f"{base}/sitemaps/index.xml")
    paths = [u.replace(base, "") for u in urls]
    print("Индекс:", paths, "| ошибок:", len(parser.errors))
    assert paths == ["/a", "/b", "/c"], "дубль /b должен исчезнуть, gzip должен читаться"
    assert len(parser.errors) == 2, "битый и отсутствующий файл попадают в ошибки"

    await parser.fetch_sitemap(f"{base}/sitemaps/bomb.xml")
    assert f"{base}/sitemaps/bomb.xml" in parser.errors, "DOCTYPE/ENTITY должны отклоняться"
    print("XML с ENTITY отклонён")


def test_stats():
    section("2. CrawlerStats и отчёты")
    now = [0.0]
    stats = CrawlerStats(clock=lambda: now[0])
    stats.start()
    for i in range(8):
        stats.record_page(f"https://a.test/{i}", status_code=200)
    stats.record_page("https://b.test/1", status_code=404, ok=False, error="PermanentError")
    stats.record_page("https://b.test/2", ok=False, error="<b>Опасно</b>")
    now[0] = 5.0
    stats.stop()
    assert stats.total_pages == 10 and stats.successful == 8 and stats.failed == 2
    assert abs(stats.pages_per_second - 2.0) < 1e-9
    assert stats.top_domains(1) == [("a.test", 8)]
    path_json, path_html = os.path.join(TMP, "s.json"), os.path.join(TMP, "s.html")
    stats.export_to_json(path_json)
    stats.export_to_html_report(path_html)
    data = json.load(open(path_json, encoding="utf-8"))
    assert data["status_codes"] == {"200": 8, "404": 1} and data["total_pages"] == 10
    page = open(path_html, encoding="utf-8").read()
    assert "&lt;b&gt;Опасно" in page and "<b>Опасно</b>" not in page, "HTML должен экранироваться"
    stats.stop()
    now[0] = 99.0
    assert stats.elapsed == 5.0, "после stop() время не растёт"
    print("Цифры, JSON и HTML в порядке")


def test_config():
    section("3. Конфигурация")
    for name in ("config.example.yaml", "config.example.json"):
        cfg = load_config(os.path.join(ROOT, "examples", name))
        assert cfg["crawler"]["max_pages"] == 100 and cfg["crawler"]["max_concurrent"] == 10
    assert from_dict({"logging": {"level": "debug"}})["logging"]["level"] == "DEBUG"
    for bad in ({"crawlr": {}}, {"crawler": {"max_pages": 0}}, {"storage": {"type": "excel"}},
                {"crawler": {"rate_limit": -1}}, {"start_urls": "строка"}):
        try:
            from_dict(bad)
        except ConfigError:
            continue
        raise AssertionError(f"плохой конфиг прошёл: {bad}")
    try:
        load_config(os.path.join(TMP, "нет.yaml"))
        raise AssertionError("отсутствующий файл не замечен")
    except ConfigError:
        pass
    print("YAML, JSON, значения по умолчанию и ошибки в порядке")


def test_logging():
    section("4. Логи с ротацией")
    log_path = os.path.join(TMP, "logs", "crawler.log")
    setup_logging(file=log_path, level="DEBUG", max_bytes=1500, backup_count=2, console=False)
    log = logging.getLogger("crawler")
    for i in range(80):
        log.info("строка номер %d", i)
    close_logging()
    files = glob.glob(log_path + "*")
    assert 2 <= len(files) <= 3, files
    setup_logging(file=log_path, level="INFO", console=False)
    setup_logging(file=log_path, level="INFO", console=False)
    log.info("уникальная-метка")
    close_logging()
    assert open(log_path, encoding="utf-8").read().count("уникальная-метка") == 1
    print("Ротация и повторная настройка работают, файлов:", len(files))


def test_monitor():
    section("5. Монитор")
    assert make_bar(50, 100, width=10) == "[#####-----]  50%"
    assert make_bar(0, 0) .endswith("0%")
    assert format_duration(65) == "1м 05с" and format_duration(None) == "—"
    assert "осталось ~15с" in format_progress(40, 100, 10, 5)
    print("Прогресс-бар, время и ETA в порядке")


async def test_advanced(base):
    section("6. AdvancedCrawler целиком")
    out = os.path.join(TMP, "adv")
    config = {
        "start_urls": [base + "/"],
        "sitemaps": [base + "/sitemap.xml"],
        "crawler": {"max_pages": 20, "max_depth": 3, "max_retries": 0},
        "storage": {"type": "json", "path": os.path.join(out, "results.json")},
        "logging": {"file": os.path.join(out, "crawler.log"), "level": "INFO"},
        "report": {"json": os.path.join(out, "stats.json"), "html": os.path.join(out, "report.html")},
    }
    crawler = AdvancedCrawler(config)
    await crawler.crawl(progress=False)
    stats = crawler.get_stats()
    await crawler.close()
    print("Страниц:", stats["total_pages"], "успешно:", stats["successful"], "ошибок:", stats["failed"])
    assert (stats["total_pages"], stats["successful"], stats["failed"]) == (7, 6, 1)
    assert stats["status_codes"] == {"200": 6, "404": 1}
    saved = json.load(open(os.path.join(out, "results.json"), encoding="utf-8"))
    assert {r["url"].replace(base, "") for r in saved} == {"/", "/a", "/b", "/c", "/x", "/y"}
    for name in ("stats.json", "report.html", "crawler.log"):
        assert os.path.getsize(os.path.join(out, name)) > 0, name

    # max_pages и фильтры
    config2 = {"start_urls": [base + "/"], "crawler": {"max_pages": 3, "max_retries": 0},
               "logging": {"file": None}}
    crawler = AdvancedCrawler(config2)
    await crawler.crawl(progress=False)
    assert crawler.get_stats()["total_pages"] == 3, "max_pages должен ограничивать обход"
    await crawler.close()

    config3 = {"start_urls": [base + "/"], "filters": {"exclude_patterns": ["/b$"]},
               "crawler": {"max_retries": 0}, "logging": {"file": None}}
    crawler = AdvancedCrawler(config3)
    results = await crawler.crawl(progress=False)
    assert not any(u.endswith("/b") for u in results), "исключённый адрес не должен загружаться"
    await crawler.close()
    await crawler.close()          # повторное закрытие безопасно
    print("max_pages, фильтры и повторное закрытие работают")


def run_cli(args):
    return subprocess.run([sys.executable, "crawler.py", *args], capture_output=True, text=True, cwd=ROOT)


async def test_cli(base):
    section("7. Командная строка")
    out = os.path.join(TMP, "cli.csv")
    result = await asyncio.to_thread(run_cli, ["--urls", base + "/", "--max-pages", "10", "--max-depth", "2",
                                               "--output", out, "--rate-limit", "100", "--no-progress"])
    print(result.stdout.strip().splitlines()[0])
    assert result.returncode == 0, result.stderr
    assert sum(1 for _ in open(out, encoding="utf-8")) == 5       # заголовок + 4 успешных
    result = await asyncio.to_thread(run_cli, [])
    assert result.returncode == 2 and "--urls" in result.stderr
    result = await asyncio.to_thread(run_cli, ["--config", os.path.join(TMP, "нет.yaml")])
    assert result.returncode == 2 and "не найден" in result.stderr
    result = await asyncio.to_thread(run_cli, ["--urls", base + "/", "--max-pages", "0"])
    assert result.returncode == 2
    print("Успешный запуск и три вида ошибок обработаны")


async def main():
    runner, port = await start_site()
    base = f"http://127.0.0.1:{port}"
    try:
        await test_sitemap(base)
        test_stats()
        test_config()
        test_logging()
        test_monitor()
        await test_advanced(base)
        await test_cli(base)
    finally:
        await runner.cleanup()
        shutil.rmtree(TMP, ignore_errors=True)
    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    asyncio.run(main())