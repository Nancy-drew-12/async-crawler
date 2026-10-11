"""Командная строка краулера.

Примеры:
    python crawler.py --urls https://example.com --max-pages 100 --output results.json
    python crawler.py --config config.yaml
    python crawler.py --config config.yaml --max-pages 20 --rate-limit 2
"""

import argparse
import asyncio
import os
import sys

from src.advanced_crawler import AdvancedCrawler
from src.config import ConfigError, from_dict, load_config

__all__ = ["AdvancedCrawler", "main"]


def build_parser():
    p = argparse.ArgumentParser(
        prog="crawler.py", description="Асинхронный веб-краулер",
        epilog="Значения из командной строки важнее значений из конфиг-файла.")
    p.add_argument("--urls", nargs="+", help="стартовые адреса (один или несколько)")
    p.add_argument("--max-pages", type=int, help="сколько страниц максимум")
    p.add_argument("--max-depth", type=int, help="на сколько шагов уходить от стартовых адресов")
    p.add_argument("--output", help="файл для результатов: .json, .csv или .db (.sqlite)")
    p.add_argument("--config", help="конфиг-файл (.yaml или .json)")
    p.add_argument("--respect-robots", action="store_true", help="соблюдать robots.txt")
    p.add_argument("--rate-limit", type=float, help="не больше стольких запросов в секунду")
    p.add_argument("--no-progress", action="store_true", help="не показывать строку прогресса")
    return p


def storage_type_for(path):
    """По расширению файла выбираем формат хранения."""
    ext = os.path.splitext(path)[1].lower()
    return {".csv": "csv", ".db": "sqlite", ".sqlite": "sqlite", ".sqlite3": "sqlite"}.get(ext, "json")


def build_config(args):
    """Конфиг-файл (если есть), поверх него аргументы командной строки."""
    config = load_config(args.config) if args.config else from_dict({})
    if args.urls:
        config["start_urls"] = args.urls
    if args.max_pages is not None:
        config["crawler"]["max_pages"] = args.max_pages
    if args.max_depth is not None:
        config["crawler"]["max_depth"] = args.max_depth
    if args.rate_limit is not None:
        config["crawler"]["rate_limit"] = args.rate_limit
    if args.respect_robots:
        config["crawler"]["respect_robots"] = True
    if args.output:
        config["storage"] = {"type": storage_type_for(args.output), "path": args.output}
    return from_dict(config)                    # повторная проверка уже с учётом аргументов


async def run(config, progress=True):
    crawler = AdvancedCrawler(config)
    try:
        await crawler.crawl(progress=progress)
        stats = crawler.get_stats()
        print(f"\nОбработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        print(f"Время: {stats['elapsed_seconds']:.1f} с, скорость {stats['pages_per_second']:.1f} стр/с")
        if config["storage"]["type"] != "none":
            print(f"Результаты: {config['storage']['path']}")
    finally:
        await crawler.close()


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        config = build_config(args)
        if not config["start_urls"] and not config["sitemaps"]:
            print("Ошибка: укажите --urls или --config со start_urls", file=sys.stderr)
            return 2
        asyncio.run(run(config, progress=not args.no_progress))
    except ConfigError as error:
        print(f"Ошибка настроек: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nОстановлено пользователем", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())