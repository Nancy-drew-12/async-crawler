# Async Crawler

Асинхронный веб-краулер на `asyncio` и `aiohttp`: ходит по сайтам параллельно, соблюдает правила
вежливости, повторяет неудачные запросы, сохраняет данные в JSON, CSV или SQLite и собирает
статистику с HTML-отчётом. Все настройки можно держать в YAML/JSON-файле и запускать из командной строки.

## Возможности

- **Параллельный обход** с общей очередью, лимитами на число запросов и на домен.
- **Вежливость**: ограничение скорости (запросов в секунду), паузы между запросами, `robots.txt`
  (включая `Crawl-delay`), разные User-Agent.
- **Устойчивость к ошибкам**: типы ошибок (временные, постоянные, сетевые), повторы с растущей
  паузой, учёт `Retry-After`, предохранитель (circuit breaker) для «упавших» сайтов.
- **Хранение данных**: JSON (массив или JSON Lines), CSV, SQLite (с обновлением по `url`).
- **Sitemap**: обычные и индексные `sitemap.xml`, сжатие gzip, защита от «XML-бомб»; адреса из
  sitemap используются как источник для обхода.
- **Статистика и отчёты**: страницы, успехи и ошибки, коды ответов, топ доменов, скорость, время;
  экспорт в JSON и HTML (с таблицами и графиками).
- **Конфигурация** из YAML или JSON, проверка значений с понятными ошибками.
- **Командная строка** (`argparse`).
- **Логи** в файл с ротацией и временными метками.
- **Мониторинг** в реальном времени: прогресс-бар, скорость, оставшееся время, активные задачи.

## Установка

Нужен Python 3.10 или новее.

```bash
pip install -r requirements.txt
```

Основные зависимости: `aiohttp`, `aiofiles`, `aiosqlite`, `pyyaml`.

## Быстрый старт

### Из командной строки

```bash
python crawler.py --urls https://example.com --max-pages 100 --output results.json
```

| Параметр | Что делает |
|---|---|
| `--urls URL [URL ...]` | стартовые адреса |
| `--max-pages N` | сколько страниц обработать максимум |
| `--max-depth N` | на сколько шагов уходить от стартовых адресов |
| `--output ФАЙЛ` | куда сохранить результаты; формат по расширению: `.json`, `.csv`, `.db` / `.sqlite` |
| `--config ФАЙЛ` | конфиг-файл `.yaml` или `.json` |
| `--respect-robots` | соблюдать `robots.txt` |
| `--rate-limit N` | не больше N запросов в секунду |
| `--no-progress` | не показывать строку прогресса |

Параметры командной строки главнее значений из конфиг-файла. Код возврата: `0` — успех,
`2` — ошибка настроек, `130` — остановлено пользователем (Ctrl+C).

### Из Python

```python
import asyncio
from crawler import AdvancedCrawler


async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")

    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")

    crawler.export_to_html_report("report.html")
    await crawler.close()


asyncio.run(main())
```

Полный пример с локальным сайтом, который не требует интернета, — `demo_advanced.py`.

## Руководство по конфигурации

Файл YAML или JSON. Всё, чего в нём нет, берётся по умолчанию. Незнакомые ключи считаются
ошибкой (так опечатки не остаются незамеченными). Готовые примеры: `examples/config.example.yaml`
и `examples/config.example.json`.

```yaml
start_urls:
  - https://example.com
sitemaps: []

crawler:
  max_pages: 100
  max_depth: 2
  max_concurrent: 10
  rate_limit: 5
  respect_robots: true

filters:
  exclude_patterns:
    - "\\.pdf$"

storage:
  type: json
  path: results.json

logging:
  file: crawler.log
  level: INFO

report:
  json: stats.json
  html: report.html
```

| Раздел и ключ | По умолчанию | Описание |
|---|---|---|
| `start_urls` | `[]` | стартовые адреса |
| `sitemaps` | `[]` | адреса `sitemap.xml`; все найденные в них URL добавляются к стартовым |
| `crawler.max_pages` | `100` | максимум страниц за запуск |
| `crawler.max_depth` | `2` | глубина обхода от стартовых адресов |
| `crawler.max_concurrent` | `10` | сколько запросов одновременно |
| `crawler.max_per_domain` | `3` | сколько запросов одновременно к одному сайту |
| `crawler.rate_limit` | `null` | запросов в секунду (`null` — без ограничения) |
| `crawler.respect_robots` | `false` | соблюдать `robots.txt` |
| `crawler.same_domain_only` | `false` | не выходить за домены стартовых адресов |
| `crawler.connect_timeout` | `5.0` | таймаут подключения, секунд |
| `crawler.read_timeout` | `10.0` | таймаут чтения ответа, секунд |
| `crawler.max_retries` | `2` | сколько раз повторять неудачный запрос |
| `crawler.user_agent` | `null` | свой User-Agent (`null` — стандартный) |
| `filters.exclude_patterns` | `[]` | регулярные выражения: такие адреса пропускаются |
| `filters.include_patterns` | `[]` | если не пусто, берутся только подходящие адреса |
| `storage.type` | `none` | `none`, `json`, `csv` или `sqlite` |
| `storage.path` | `results.json` | файл результатов |
| `logging.file` | `crawler.log` | файл логов (`null` — только консоль) |
| `logging.level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `logging.max_bytes` | `1000000` | размер файла, после которого начинается новый |
| `logging.backup_count` | `3` | сколько старых файлов логов хранить |
| `report.json` | `null` | куда сохранить статистику в JSON после обхода |
| `report.html` | `null` | куда сохранить HTML-отчёт после обхода |

Совет: для маленьких сайтов с одним доменом увеличьте `max_per_domain` (до `max_concurrent`),
иначе одновременно будет работать только 3 запроса.

## Описание API

### `AdvancedCrawler` (`src/advanced_crawler.py`)

Главный класс, который собирает все компоненты вместе.

| Метод | Описание |
|---|---|
| `AdvancedCrawler(config: dict)` | создать из словаря настроек (недостающее заполняется по умолчанию) |
| `AdvancedCrawler.from_config(path, overrides=None)` | создать из YAML/JSON-файла; `overrides` накладывается сверху |
| `await crawl(progress=True)` | обойти сайты; возвращает `{url: данные страницы}` |
| `get_stats()` | словарь: `total_pages`, `successful`, `failed`, `success_rate`, `elapsed_seconds`, `pages_per_second`, `status_codes`, `top_domains`, `errors`, `retries`, `storage` |
| `export_to_json(filename)` | сохранить статистику в JSON |
| `export_to_html_report(filename)` | сохранить HTML-отчёт |
| `await close()` | закрыть сессию, хранилище и файлы логов (можно вызывать повторно) |

Можно использовать как `async with AdvancedCrawler(config) as crawler:`.

### Остальные компоненты

| Модуль | Назначение |
|---|---|
| `src/async_crawler.py` — `AsyncCrawler` | ядро: загрузка, обход, вежливость, повторы, сохранение |
| `src/crawler_stats.py` — `CrawlerStats` | подсчёт статистики и экспорт в JSON/HTML |
| `src/sitemap_parser.py` — `SitemapParser` | `await fetch_sitemap(url) -> list[str]`, `await fetch_for_site(site_url)` |
| `src/config.py` | `load_config(path)`, `from_dict(data)`, `ConfigError` |
| `src/log_setup.py` | `setup_logging(...)`, `close_logging()` |
| `src/monitor.py` | `ProgressMonitor`, `format_progress(...)` |
| `src/json_storage.py`, `csv_storage.py`, `sqlite_storage.py` | хранилища `JSONStorage`, `CSVStorage`, `SQLiteStorage` |
| `src/retry.py`, `circuit_breaker.py`, `errors.py` | повторы, предохранитель, типы ошибок |
| `src/robots_parser.py`, `rate_limiter.py`, `politeness.py` | `robots.txt`, ограничение скорости, вежливость |
| `src/url_filter.py`, `crawler_queue.py`, `semaphore_manager.py` | фильтры адресов, очередь, лимиты параллельности |

## Структура проекта

```
crawler.py            командная строка и точка входа (AdvancedCrawler)
benchmark.py          тесты производительности
demo_advanced.py      пример из задания
src/                  код краулера
tests/                тесты
examples/             примеры конфигов
```

## Тесты

```bash
python tests/test_crawl.py       # очередь, фильтры, обход
python tests/test_polite.py      # скорость запросов, robots.txt
python tests/test_retry.py       # ошибки, повторы, предохранитель
python tests/test_storage.py     # JSON, CSV, SQLite
python tests/test_advanced.py    # sitemap, статистика, конфиг, логи, AdvancedCrawler, CLI
```

Тесты работают на локальном сайте и не требуют интернета.

## Производительность

Замеры `python benchmark.py` на MacBook Pro: локальный сайт-полигон, задержка ответа 20 мс на страницу,
20 параллельных запросов.

| | Результат |
|---|---|
| Синхронный краулер, 100 страниц | 2.50 с (40 стр/с) |
| Асинхронный краулер, 100 страниц | 0.28 с (358 стр/с), быстрее в 8.9 раза |

| Страниц | Время, с | Скорость, стр/с | Пиковая память, МБ |
|---|---|---|---|
| 100 | 0.27 | 376 | 1.0 |
| 500 | 0.70 | 709 | 1.8 |
| 1000 | 1.24 | 804 | 2.5 |

Влияние числа параллельных запросов (200 страниц): 1 запрос — 37 стр/с, 5 — 186, 10 — 350,
20 — 529, 50 — 696. Время растёт примерно линейно с числом страниц, лишних задержек внутри краулера
не обнаружено: теоретический максимум при 20 запросах и задержке 20 мс равен 1000 стр/с, а получается около 800.
Абсолютные цифры зависят от компьютера и сайта; на реальных сайтах скорость ограничивают
задержки сервера и правила вежливости (`rate_limit`).

## Что не реализовано

Из необязательных пунктов задания: прокси, cookies и сессии, JavaScript-рендеринг и распределённый
краулинг не реализованы.

## Известные ограничения

- Страницы целиком хранятся в памяти до конца обхода (`processed_urls`); для очень больших обходов
  (сотни тысяч страниц) нужно сохранять данные в хранилище и очищать результаты.
- Робот не исполняет JavaScript: содержимое, которое подгружается скриптами, он не увидит.