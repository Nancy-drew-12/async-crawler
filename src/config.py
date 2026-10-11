"""Настройки краулера из файла (YAML или JSON)."""

import copy
import json
import os

from src.errors import CrawlerError


class ConfigError(CrawlerError):
    """Конфиг-файл не найден, не читается или в нём ошибка."""


# Значения по умолчанию. Всё, чего нет в файле, берётся отсюда.
DEFAULTS = {
    "start_urls": [],             # с чего начинаем
    "sitemaps": [],               # адреса sitemap.xml как источник URL
    "crawler": {
        "max_pages": 100,         # сколько страниц максимум
        "max_depth": 2,           # на сколько шагов уходим от стартовых адресов
        "max_concurrent": 10,     # сколько запросов одновременно
        "max_per_domain": 3,      # сколько одновременно на один сайт
        "rate_limit": None,       # запросов в секунду (None = без лимита)
        "respect_robots": False,  # соблюдать robots.txt
        "same_domain_only": False,
        "connect_timeout": 5.0,
        "read_timeout": 10.0,
        "max_retries": 2,
        "user_agent": None,       # None = стандартный
    },
    "filters": {
        "exclude_patterns": [],   # регулярные выражения: такие адреса пропускаем
        "include_patterns": [],   # если не пусто: берём только подходящие
    },
    "storage": {
        "type": "none",           # none / json / csv / sqlite
        "path": "results.json",
    },
    "logging": {
        "file": "crawler.log",    # None = только в консоль
        "level": "INFO",          # DEBUG / INFO / WARNING / ERROR
        "max_bytes": 1_000_000,   # когда файл больше, начинается новый
        "backup_count": 3,        # сколько старых файлов хранить
    },
    "report": {
        "json": None,             # например "stats.json"
        "html": None,             # например "report.html"
    },
}

STORAGE_TYPES = ("none", "json", "csv", "sqlite")
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def _merge(defaults, user, path=""):
    """Накладывает пользовательские значения на значения по умолчанию.

    Незнакомый ключ (скорее всего опечатка) считается ошибкой.
    """
    if not isinstance(user, dict):
        raise ConfigError(f"раздел '{path or 'корень'}' должен быть словарём")
    result = copy.deepcopy(defaults)
    for key, value in user.items():
        if key not in defaults:
            known = ", ".join(defaults)
            raise ConfigError(f"неизвестная настройка '{path}{key}'. Допустимые: {known}")
        if isinstance(defaults[key], dict):
            result[key] = _merge(defaults[key], value or {}, path=f"{path}{key}.")
        else:
            result[key] = value
    return result


def _check_number(value, name, minimum, allow_none=False, integer=False):
    if value is None and allow_none:
        return
    kind = int if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, kind) or value < minimum:
        what = "целым числом" if integer else "числом"
        raise ConfigError(f"{name} должен быть {what} не меньше {minimum}")


def _check_str_list(value, name):
    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise ConfigError(f"{name} должен быть списком строк")


def validate(cfg):
    """Проверяет значения. При проблеме бросает ConfigError с понятным текстом."""
    _check_str_list(cfg["start_urls"], "start_urls")
    _check_str_list(cfg["sitemaps"], "sitemaps")
    c = cfg["crawler"]
    _check_number(c["max_pages"], "crawler.max_pages", 1, integer=True)
    _check_number(c["max_depth"], "crawler.max_depth", 0, integer=True)
    _check_number(c["max_concurrent"], "crawler.max_concurrent", 1, integer=True)
    _check_number(c["max_per_domain"], "crawler.max_per_domain", 1, integer=True)
    _check_number(c["max_retries"], "crawler.max_retries", 0, integer=True)
    _check_number(c["connect_timeout"], "crawler.connect_timeout", 0.001)
    _check_number(c["read_timeout"], "crawler.read_timeout", 0.001)
    if c["rate_limit"] is not None:
        _check_number(c["rate_limit"], "crawler.rate_limit", 0.001)
    for flag in ("respect_robots", "same_domain_only"):
        if not isinstance(c[flag], bool):
            raise ConfigError(f"crawler.{flag} должен быть true или false")
    _check_str_list(cfg["filters"]["exclude_patterns"], "filters.exclude_patterns")
    _check_str_list(cfg["filters"]["include_patterns"], "filters.include_patterns")
    if cfg["storage"]["type"] not in STORAGE_TYPES:
        raise ConfigError(f"storage.type должен быть одним из: {', '.join(STORAGE_TYPES)}")
    if cfg["storage"]["type"] != "none" and not cfg["storage"]["path"]:
        raise ConfigError("storage.path не может быть пустым")
    lg = cfg["logging"]
    if str(lg["level"]).upper() not in LOG_LEVELS:
        raise ConfigError(f"logging.level должен быть одним из: {', '.join(LOG_LEVELS)}")
    lg["level"] = str(lg["level"]).upper()
    _check_number(lg["max_bytes"], "logging.max_bytes", 1, integer=True)
    _check_number(lg["backup_count"], "logging.backup_count", 0, integer=True)
    return cfg


def load_config(path):
    """Читает YAML или JSON файл и возвращает проверенный словарь настроек."""
    if not os.path.isfile(path):
        raise ConfigError(f"файл настроек не найден: {path}")
    ext = os.path.splitext(path)[1].lower()
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        if ext in (".yaml", ".yml"):
            try:
                import yaml
            except ImportError:
                raise ConfigError("для YAML нужна библиотека: pip install pyyaml") from None
            data = yaml.safe_load(text) or {}
        elif ext == ".json":
            data = json.loads(text) if text.strip() else {}
        else:
            raise ConfigError(f"неизвестный формат '{ext}': нужен .yaml, .yml или .json")
    except ConfigError:
        raise
    except (OSError, ValueError) as e:      # ValueError включает ошибки JSON
        raise ConfigError(f"не удалось прочитать {path}: {e}") from e
    except Exception as e:                  # ошибки разбора YAML
        raise ConfigError(f"ошибка в файле {path}: {e}") from e
    return from_dict(data)


def from_dict(data):
    """Собирает настройки из словаря (без файла). Удобно в тестах и для CLI."""
    return validate(_merge(DEFAULTS, data))