"""Настройка логирования: файл с ротацией + консоль."""

import logging
import os
from logging.handlers import RotatingFileHandler

LOGGER_NAME = "crawler"      # все модули краулера пишут в этот логгер
FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_MARK = "_crawler_handler"   # метка, чтобы находить и убирать только свои обработчики


def setup_logging(file="crawler.log", level="INFO", max_bytes=1_000_000,
                  backup_count=3, console=True, console_level="WARNING"):
    """Включает запись логов.

    file          куда писать (None = только консоль)
    level         с какого уровня писать в файл: DEBUG / INFO / WARNING / ERROR
    max_bytes     когда файл вырастет до этого размера, начнётся новый
    backup_count  сколько старых файлов хранить (crawler.log.1, .2, ...)
    console       дублировать ли важное в консоль
    console_level с какого уровня показывать в консоли (по умолчанию только WARNING и выше,
                  чтобы не мешать строке прогресса)

    Можно вызывать повторно: старые обработчики заменяются, строки не дублируются.
    """
    logger = logging.getLogger(LOGGER_NAME)
    close_logging()

    file_level = getattr(logging, str(level).upper())
    cons_level = getattr(logging, str(console_level).upper())
    # логгер пропускает всё, что нужно хотя бы одному из обработчиков
    logger.setLevel(min(file_level if file else cons_level, cons_level if console else file_level))
    logger.propagate = False

    formatter = logging.Formatter(FORMAT, DATE_FORMAT)

    if file:
        folder = os.path.dirname(os.path.abspath(file))
        os.makedirs(folder, exist_ok=True)
        handler = RotatingFileHandler(file, maxBytes=max_bytes, backupCount=backup_count,
                                      encoding="utf-8")
        handler.setLevel(file_level)
        handler.setFormatter(formatter)
        setattr(handler, _MARK, True)
        logger.addHandler(handler)

    if console:
        handler = logging.StreamHandler()
        handler.setLevel(cons_level)
        handler.setFormatter(formatter)
        setattr(handler, _MARK, True)
        logger.addHandler(handler)

    return logger


def close_logging():
    """Убирает и закрывает обработчики, которые поставил setup_logging."""
    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _MARK, False):
            logger.removeHandler(handler)
            handler.close()