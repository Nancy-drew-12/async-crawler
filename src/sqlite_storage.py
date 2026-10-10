"""Сохранение страниц в базу SQLite. Работа с базой асинхронная (aiosqlite), вставки пачками."""
import asyncio
import json
import os
import sqlite3
from datetime import datetime
from urllib.parse import urlsplit

import aiosqlite

from src.storage import DataStorage, StorageError, validate_record

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    url          TEXT NOT NULL UNIQUE,
    domain       TEXT NOT NULL,
    title        TEXT NOT NULL DEFAULT '',
    text         TEXT NOT NULL DEFAULT '',
    links        TEXT NOT NULL DEFAULT '[]',
    metadata     TEXT NOT NULL DEFAULT '{}',
    crawled_at   TEXT,
    status_code  INTEGER,
    content_type TEXT NOT NULL DEFAULT ''
)
"""

# Индексы ускоряют поиск по этим колонкам (url уже проиндексирован из-за UNIQUE)
CREATE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain)",
    "CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status_code)",
    "CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at)",
)

# Если такой url уже есть, запись обновляется, а не дублируется
INSERT = """
INSERT INTO pages (url, domain, title, text, links, metadata, crawled_at, status_code, content_type)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(url) DO UPDATE SET
    domain = excluded.domain, title = excluded.title, text = excluded.text,
    links = excluded.links, metadata = excluded.metadata, crawled_at = excluded.crawled_at,
    status_code = excluded.status_code, content_type = excluded.content_type
"""

DB_ERRORS = (sqlite3.Error, OSError)


def _to_record(row) -> dict:
    """Строка таблицы -> запись стандартного формата."""
    return {
        "url": row["url"], "title": row["title"], "text": row["text"],
        "links": json.loads(row["links"]), "metadata": json.loads(row["metadata"]),
        "crawled_at": datetime.fromisoformat(row["crawled_at"]) if row["crawled_at"] else None,
        "status_code": row["status_code"], "content_type": row["content_type"],
    }


class SQLiteStorage(DataStorage):
    """Хранит страницы в таблице pages. Записи копятся и вставляются пачками по batch_size штук
    одной транзакцией: так намного быстрее, чем по одной. Повторное сохранение того же url
    обновляет запись. Таблица и индексы создаются сами (init_db)."""

    def __init__(self, path: str, batch_size: int = 50):
        super().__init__()
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 1:
            raise ValueError("batch_size должен быть целым числом не меньше 1")
        self.path = os.fspath(path)
        self.batch_size = batch_size
        self._db: aiosqlite.Connection | None = None
        self._buffer: list[tuple] = []
        self._closed = False
        self._lock = asyncio.Lock()
        self.flushes = 0

    # ---------- создание базы ----------

    async def init_db(self) -> None:
        """Открывает базу и создаёт таблицу и индексы, если их ещё нет. Можно вызывать повторно."""
        async with self._lock:
            await self._init_locked()

    async def _init_locked(self) -> None:
        if self._closed:
            raise StorageError("хранилище уже закрыто")
        if self._db is not None:
            return
        try:
            db = await aiosqlite.connect(self.path)
            try:
                db.row_factory = sqlite3.Row
                await db.execute("PRAGMA journal_mode=WAL")      # чтение не мешает записи
                await db.execute("PRAGMA synchronous=NORMAL")    # быстрее, и для краулера этого достаточно
                await db.execute(CREATE_TABLE)
                for statement in CREATE_INDEXES:
                    await db.execute(statement)
                await db.commit()
            except BaseException:
                await db.close()
                raise
        except DB_ERRORS as error:
            raise StorageError(f"не удалось открыть базу {self.path}: {error}") from error
        self._db = db

    # ---------- запись ----------

    @staticmethod
    def _row(data: dict) -> tuple:
        crawled_at = data["crawled_at"]
        return (
            data["url"], urlsplit(data["url"]).netloc.lower(), str(data["title"] or ""), str(data["text"] or ""),
            json.dumps(data["links"], ensure_ascii=False, default=str),
            json.dumps(data["metadata"], ensure_ascii=False, default=str),
            crawled_at.isoformat() if isinstance(crawled_at, datetime) else crawled_at,
            data["status_code"], str(data["content_type"] or ""),
        )

    async def save(self, data: dict) -> None:
        validate_record(data)
        row = self._row(data)
        async with self._lock:
            if self._closed:
                raise StorageError("хранилище уже закрыто")
            self._buffer.append(row)
            if len(self._buffer) >= self.batch_size:
                try:
                    await self._flush_locked()
                except StorageError:
                    self._buffer.pop()          # эта запись не сохранена: вызывающий может повторить
                    raise
            self.saved_count += 1

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        await self._init_locked()
        if not self._buffer:
            return
        try:
            await self._db.executemany(INSERT, self._buffer)
            await self._db.commit()
        except DB_ERRORS as error:
            try:
                await self._db.rollback()
            except DB_ERRORS:
                pass
            raise StorageError(f"не удалось записать в базу {self.path}: {error}") from error
        self._buffer.clear()
        self.flushes += 1

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            try:
                await self._flush_locked()
            finally:
                self._closed = True
                if self._db is not None:
                    await self._db.close()
                    self._db = None

    # ---------- чтение ----------

    async def _query(self, sql: str, params: tuple = ()) -> list:
        async with self._lock:
            await self._init_locked()
            await self._flush_locked()          # сначала дописываем буфер, чтобы читать свежие данные
            try:
                async with self._db.execute(sql, params) as cursor:
                    return await cursor.fetchall()
            except DB_ERRORS as error:
                raise StorageError(f"ошибка чтения из базы: {error}") from error

    async def count(self) -> int:
        rows = await self._query("SELECT COUNT(*) AS n FROM pages")
        return rows[0]["n"]

    async def get(self, url: str) -> dict | None:
        """Находит страницу по точному url (поиск по индексу)."""
        rows = await self._query("SELECT * FROM pages WHERE url = ?", (url,))
        return _to_record(rows[0]) if rows else None

    async def find(self, domain: str | None = None, status_code: int | None = None,
                   title_contains: str | None = None, limit: int = 100) -> list[dict]:
        """Ищет страницы по домену, коду ответа и/или слову в заголовке."""
        conditions, params = [], []
        if domain is not None:
            conditions.append("domain = ?")
            params.append(domain.lower())
        if status_code is not None:
            conditions.append("status_code = ?")
            params.append(status_code)
        if title_contains:
            conditions.append("title LIKE ?")
            params.append(f"%{title_contains}%")
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = await self._query(f"SELECT * FROM pages{where} ORDER BY id LIMIT ?", (*params, limit))
        return [_to_record(row) for row in rows]

    async def get_db_stats(self) -> dict:
        """Сводка по базе: сколько страниц, сколько по доменам и по кодам ответа."""
        total = await self.count()
        by_domain = await self._query("SELECT domain, COUNT(*) AS n FROM pages GROUP BY domain ORDER BY n DESC")
        by_status = await self._query("SELECT status_code, COUNT(*) AS n FROM pages GROUP BY status_code ORDER BY status_code")
        return {"pages": total,
                "by_domain": {row["domain"]: row["n"] for row in by_domain},
                "by_status": {row["status_code"]: row["n"] for row in by_status}}

    def get_stats(self) -> dict:
        return {"saved": self.saved_count, "buffered": len(self._buffer), "flushes": self.flushes,
                "path": self.path}