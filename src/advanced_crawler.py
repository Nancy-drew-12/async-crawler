"""AdvancedCrawler: главный класс, который собирает все части краулера вместе."""

import logging

from src.async_crawler import DEFAULT_USER_AGENT, AsyncCrawler
from src.config import ConfigError, from_dict, load_config
from src.crawler_stats import CrawlerStats
from src.csv_storage import CSVStorage
from src.json_storage import JSONStorage
from src.log_setup import close_logging, setup_logging
from src.monitor import ProgressMonitor
from src.sitemap_parser import SitemapParser
from src.sqlite_storage import SQLiteStorage

logger = logging.getLogger("crawler")


class AdvancedCrawler:
    """Краулер «всё в одном»: настройки из файла, логи, статистика, сохранение, отчёты.

    Пример:
        crawler = AdvancedCrawler.from_config("config.yaml")
        await crawler.crawl()
        print(crawler.get_stats()["total_pages"])
        crawler.export_to_html_report("report.html")
        await crawler.close()
    """

    def __init__(self, config=None):
        # from_dict дополняет недостающее значениями по умолчанию и проверяет всё
        self.config = from_dict(config or {})
        c = self.config["crawler"]
        lg = self.config["logging"]

        setup_logging(file=lg["file"], level=lg["level"],
                      max_bytes=lg["max_bytes"], backup_count=lg["backup_count"])

        self.stats = CrawlerStats()
        self.storage = self._make_storage()
        self.crawler = AsyncCrawler(
            max_concurrent=c["max_concurrent"],
            max_per_domain=min(c["max_per_domain"], c["max_concurrent"]),
            max_depth=c["max_depth"],
            requests_per_second=c["rate_limit"],
            respect_robots=c["respect_robots"],
            connect_timeout=c["connect_timeout"],
            read_timeout=c["read_timeout"],
            max_retries=c["max_retries"],
            user_agent=c["user_agent"] or DEFAULT_USER_AGENT,
            storage=self.storage,
            page_callback=self._on_page,
        )
        self.results = {}
        self._closed = False

    # ---------- создание ----------

    @classmethod
    def from_config(cls, path, overrides=None):
        """Создаёт краулер из YAML/JSON файла. overrides: словарь, который накладывается сверху."""
        config = load_config(path)
        if overrides:
            config = _deep_update(config, overrides)
        return cls(config)

    def _make_storage(self):
        kind = self.config["storage"]["type"]
        path = self.config["storage"]["path"]
        if kind == "json":
            return JSONStorage(path)
        if kind == "csv":
            return CSVStorage(path)
        if kind == "sqlite":
            return SQLiteStorage(path)
        return None

    def _on_page(self, url, ok, status, error, size):
        """AsyncCrawler сообщает сюда о каждой странице; мы пишем это в статистику."""
        self.stats.record_page(url, status_code=status, ok=ok, error=error, size=size)

    # ---------- работа ----------

    async def crawl(self, progress=True):
        """Обходит сайты: стартовые адреса + адреса из sitemap. Возвращает {url: данные страницы}."""
        if self._closed:
            raise RuntimeError("Краулер уже закрыт")
        urls = list(self.config["start_urls"])
        if self.config["sitemaps"]:
            urls += await self._urls_from_sitemaps()
        urls = list(dict.fromkeys(urls))                # убираем повторы, порядок сохраняем
        if not urls:
            raise ConfigError("нет адресов для обхода: укажите start_urls или sitemaps")

        c = self.config["crawler"]
        f = self.config["filters"]
        logger.info("Старт: %d адресов, max_pages=%d, max_depth=%d", len(urls), c["max_pages"], c["max_depth"])

        self.stats.start()
        monitor = ProgressMonitor(self._monitor_state) if progress else None
        if monitor:
            monitor.start()
        try:
            self.results = await self.crawler.crawl(
                urls, max_pages=c["max_pages"], same_domain_only=c["same_domain_only"],
                exclude_patterns=f["exclude_patterns"], include_patterns=f["include_patterns"],
                progress=False)                         # свою строку прогресса печатает монитор
        finally:
            self.stats.stop()
            if monitor:
                await monitor.stop()
        logger.info("Готово: %d страниц, ошибок %d", self.stats.total_pages, self.stats.failed)
        self._write_reports()
        return self.results

    async def _urls_from_sitemaps(self):
        parser = SitemapParser(user_agent=self.crawler.user_agent)
        found = []
        for sitemap_url in self.config["sitemaps"]:
            urls = await parser.fetch_sitemap(sitemap_url)
            logger.info("Sitemap %s: найдено адресов %d", sitemap_url, len(urls))
            found += urls
        return found

    def _monitor_state(self):
        """Цифры для строки прогресса."""
        s = self.crawler.get_crawl_stats()
        known = self.crawler.queue.get_stats()["total_added"] if self.crawler.queue else 0
        done = s["processed"] + s["failed"]
        total = max(done, min(self.config["crawler"]["max_pages"], known))
        return {"done": done, "total": total, "elapsed": s["elapsed"],
                "active": s["in_progress"], "failed": s["failed"]}

    def _write_reports(self):
        report = self.config["report"]
        if report["json"]:
            self.export_to_json(report["json"])
        if report["html"]:
            self.export_to_html_report(report["html"])

    # ---------- результаты ----------

    def get_stats(self):
        """Итоги: total_pages, successful, failed, status_codes, top_domains и так далее."""
        data = self.stats.to_dict()
        data["retries"] = self.crawler.retries_done
        data["storage"] = self.crawler.get_storage_stats()
        return data

    def export_to_json(self, filename):
        self.stats.export_to_json(filename)

    def export_to_html_report(self, filename):
        self.stats.export_to_html_report(filename)

    async def close(self):
        """Закрывает сессию, хранилище и файлы логов. Можно вызывать повторно."""
        if self._closed:
            return
        self._closed = True
        await self.crawler.close()
        close_logging()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


def _deep_update(base, extra):
    """Накладывает extra на base, заходя внутрь вложенных словарей."""
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base