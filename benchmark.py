"""Тесты производительности краулера.

Запуск:
    python benchmark.py            # полный набор (около минуты)
    python benchmark.py --quick    # короткая версия для проверки

Сайт-полигон запускается отдельным процессом на localhost, интернет не нужен.
У каждой страницы есть искусственная задержка (как у настоящего сервера),
иначе ответы приходят мгновенно и разница между подходами не видна.
"""

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
import tracemalloc
import urllib.request
from urllib.parse import urljoin

DELAY = 0.02          # задержка ответа сервера, секунд
TOTAL = 1000          # сколько страниц на сайте-полигоне


# ---------- сайт-полигон ----------

def serve(total, delay):
    """Страница /p/N ссылается на /p/3N+1, /p/3N+2, /p/3N+3 (получается дерево)."""
    from aiohttp import web

    async def page(request):
        n = int(request.match_info["n"])
        if n >= total:
            return web.Response(status=404, text="нет")
        await asyncio.sleep(delay)
        links = "".join(f'<a href="/p/{c}">страница {c}</a> ' for c in (3 * n + 1, 3 * n + 2, 3 * n + 3)
                        if c < total)
        body = f"<html><head><title>Страница {n}</title></head><body><p>Текст страницы {n}</p>{links}</body></html>"
        return web.Response(text=body, content_type="text/html")

    async def run():
        app = web.Application()
        app.router.add_get("/p/{n:\\d+}", page)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        print(site._server.sockets[0].getsockname()[1], flush=True)   # сообщаем порт родителю
        await asyncio.Event().wait()

    asyncio.run(run())


def start_site():
    proc = subprocess.Popen([sys.executable, __file__, "--serve", str(TOTAL), str(DELAY)],
                            stdout=subprocess.PIPE, text=True)
    port = int(proc.stdout.readline())
    return proc, f"http://127.0.0.1:{port}"


# ---------- синхронный краулер для сравнения ----------

def sync_crawl(start_url, max_pages):
    """Простейший обход «по одной странице»: скачали, разобрали, пошли дальше."""
    queue, seen, done = [start_url], {start_url}, 0
    while queue and done < max_pages:
        url = queue.pop(0)
        with urllib.request.urlopen(url, timeout=10) as response:
            html = response.read().decode("utf-8", errors="replace")
        done += 1
        for href in re.findall(r'href="([^"]+)"', html):
            link = urljoin(url, href)
            if link not in seen and len(seen) < max_pages:
                seen.add(link)
                queue.append(link)
    return done


# ---------- асинхронный краулер ----------

def make_config(base, pages, concurrent):
    return {
        "start_urls": [f"{base}/p/0"],
        "crawler": {"max_pages": pages, "max_depth": 20, "max_concurrent": concurrent,
                    "max_per_domain": concurrent, "max_retries": 0},
        "logging": {"file": None},
    }


async def async_crawl(base, pages, concurrent):
    from src.advanced_crawler import AdvancedCrawler
    crawler = AdvancedCrawler(make_config(base, pages, concurrent))
    try:
        await crawler.crawl(progress=False)
        return crawler.get_stats()["total_pages"]
    finally:
        await crawler.close()


def timed_async(base, pages, concurrent):
    started = time.perf_counter()
    done = asyncio.run(async_crawl(base, pages, concurrent))
    return done, time.perf_counter() - started


def peak_memory_mb(base, pages, concurrent):
    """Отдельный прогон под tracemalloc: он замедляет работу, поэтому скорость тут не меряем."""
    tracemalloc.start()
    asyncio.run(async_crawl(base, pages, concurrent))
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return peak / 1024 / 1024


# ---------- сами замеры ----------

def main(quick):
    sizes = [50, 100, 200] if quick else [100, 500, 1000]
    concurrency_pages = 100 if quick else 200
    results = {}
    proc, base = start_site()
    try:
        print(f"Сайт-полигон: {base}, задержка ответа {DELAY * 1000:.0f} мс\n")

        timed_async(base, 10, 5)            # прогрев: первый запуск дольше из-за импортов и сессии

        print("1. Синхронный и асинхронный краулер")
        n = sizes[0]
        started = time.perf_counter()
        done = sync_crawl(f"{base}/p/0", n)
        t_sync = time.perf_counter() - started
        done_a, t_async = timed_async(base, n, 20)
        print(f"   синхронный: {done} стр за {t_sync:.2f} с ({done / t_sync:.1f} стр/с)")
        print(f"   асинхронный (20 параллельно): {done_a} стр за {t_async:.2f} с ({done_a / t_async:.1f} стр/с)")
        print(f"   асинхронный быстрее в {t_sync / t_async:.1f} раз")
        results["sync_vs_async"] = {"pages": n, "sync_s": t_sync, "async_s": t_async}

        print("\n2. Масштабируемость и память (20 параллельно)")
        print(f"   {'страниц':>8} {'время, с':>9} {'стр/с':>8} {'память, МБ':>11} {'КБ/стр':>8}")
        results["scaling"] = []
        for pages in sizes:
            done, seconds = timed_async(base, pages, 20)
            mem = peak_memory_mb(base, pages, 20)
            print(f"   {done:>8} {seconds:>9.2f} {done / seconds:>8.1f} {mem:>11.1f} {mem * 1024 / done:>8.1f}")
            results["scaling"].append({"pages": done, "seconds": seconds, "mb": mem})

        print(f"\n3. Влияние числа параллельных запросов ({concurrency_pages} страниц)")
        print(f"   {'параллельно':>12} {'время, с':>9} {'стр/с':>8}")
        results["concurrency"] = []
        for concurrent in (1, 5, 10, 20, 50):
            done, seconds = timed_async(base, concurrency_pages, concurrent)
            print(f"   {concurrent:>12} {seconds:>9.2f} {done / seconds:>8.1f}")
            results["concurrency"].append({"concurrent": concurrent, "seconds": seconds})
    finally:
        proc.terminate()
        proc.wait()

    os.makedirs("demo_output", exist_ok=True)
    with open("demo_output/benchmark.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("\nРезультаты сохранены в demo_output/benchmark.json")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--serve":
        serve(int(sys.argv[2]), float(sys.argv[3]))
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--quick", action="store_true", help="короткая версия")
        main(parser.parse_args().quick)