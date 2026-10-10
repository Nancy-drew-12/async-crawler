"""Сохранение страниц в JSON-файл. Запись асинхронная (aiofiles), с буфером."""
import asyncio
import json
import os
from datetime import datetime

import aiofiles

from src.storage import DataStorage, StorageError, record_to_jsonable, validate_record


class JSONStorage(DataStorage):
    """Пишет записи в JSON-файл по мере поступления, не держа всё в памяти.

    Два формата файла:
      lines=False (по умолчанию): обычный JSON-массив [ {...}, {...} ]. Его читает json.load.
      lines=True: «JSON Lines», одна запись на строку без скобок. Безопаснее при больших объёмах
                  и обрыве программы, но целиком json.load такой файл не прочитает.
    indent=2 даёт красивый многострочный вывод (для JSON Lines не применяется: там строка = запись).
    append=True дописывает в существующий файл, иначе файл создаётся заново.
    Записи копятся в буфере и пишутся на диск пачками по buffer_size штук.
    """

    def __init__(self, path: str, indent: int | None = None, lines: bool = False,
                 append: bool = False, buffer_size: int = 50, encoding: str = "utf-8"):
        super().__init__()
        if not isinstance(buffer_size, int) or isinstance(buffer_size, bool) or buffer_size < 1:
            raise ValueError("buffer_size должен быть целым числом не меньше 1")
        self.path = os.fspath(path)
        self.indent = None if lines else indent
        self.lines = lines
        self.append = append
        self.buffer_size = buffer_size
        self.encoding = encoding
        self._buffer: list[str] = []        # записи, ещё не записанные на диск
        self._file = None
        self._first = True                  # в массиве пока нет ни одной записи
        self._closed = False
        self._lock = asyncio.Lock()         # воркеры вызывают save одновременно, запись идёт по очереди
        self.flushes = 0

    # ---------- открытие файла ----------

    async def _open(self) -> None:
        try:
            exists = os.path.exists(self.path) and os.path.getsize(self.path) > 0
            if self.lines:
                self._file = await aiofiles.open(self.path, "a" if self.append else "w", encoding=self.encoding)
                self._first = False
            elif self.append and exists:
                self._file = await aiofiles.open(self.path, "r+b")
                self._first = await self._cut_array_end()
                # после обрезки позиция стоит в конце, дальше пишем байты
            else:
                self._file = await aiofiles.open(self.path, "wb")
                await self._file.write(b"[\n")
        except (OSError, ValueError) as error:
            if self._file is not None:              # файл успели открыть, но он оказался не тот
                await self._file.close()
                self._file = None
            raise StorageError(f"не удалось открыть {self.path}: {error}") from error

    async def _cut_array_end(self) -> bool:
        """Убирает из существующего файла закрывающую ']'. Возвращает True, если массив был пустой."""
        size = await self._file.seek(0, 2)
        tail_size = min(size, 64)
        await self._file.seek(size - tail_size)
        tail = (await self._file.read(tail_size)).rstrip()
        if not tail.endswith(b"]"):
            raise ValueError("файл не похож на JSON-массив: нет закрывающей ]")
        body = tail[:-1].rstrip()
        if not body:
            raise ValueError("файл не похож на JSON-массив: нет открывающей [")
        new_size = size - tail_size + len(body)
        await self._file.truncate(new_size)
        await self._file.seek(new_size)
        return body.endswith(b"[")

    # ---------- запись ----------

    def _dump(self, data: dict) -> str:
        return json.dumps(record_to_jsonable(data), ensure_ascii=False, indent=self.indent, default=str)

    async def save(self, data: dict) -> None:
        validate_record(data)
        text = self._dump(data)
        async with self._lock:
            if self._closed:
                raise StorageError("хранилище уже закрыто")
            self._buffer.append(text)
            if len(self._buffer) >= self.buffer_size:
                try:
                    await self._flush_locked()
                except StorageError:
                    self._buffer.pop()      # эта запись не сохранена: вызывающий может повторить
                    raise
            self.saved_count += 1

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if self._file is None:
            await self._open()
        if not self._buffer:
            return
        try:
            if self.lines:
                await self._file.write("\n".join(self._buffer) + "\n")
            else:
                chunk = ",\n".join(self._buffer)
                if not self._first:
                    chunk = ",\n" + chunk
                await self._file.write(chunk.encode(self.encoding))
                self._first = False
            await self._file.flush()
        except (OSError, ValueError) as error:
            raise StorageError(f"не удалось записать в {self.path}: {error}") from error
        self._buffer.clear()
        self.flushes += 1

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            try:
                await self._flush_locked()                      # при необходимости заодно создаёт файл
                if not self.lines:
                    await self._file.write(b"\n]\n")
            except (OSError, ValueError) as error:
                raise StorageError(f"не удалось закрыть {self.path}: {error}") from error
            finally:
                self._closed = True
                if self._file is not None:
                    await self._file.close()
                    self._file = None

    def get_stats(self) -> dict:
        return {"saved": self.saved_count, "buffered": len(self._buffer), "flushes": self.flushes,
                "path": self.path}


async def read_json(path: str, parse_dates: bool = True, encoding: str = "utf-8") -> list[dict]:
    """Читает файл, который написал JSONStorage (массив или JSON Lines), и возвращает список записей."""
    async with aiofiles.open(path, "r", encoding=encoding) as file:
        text = await file.read()
    text = text.strip()
    if not text:
        return []
    records = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line.strip()]
    if parse_dates:
        for record in records:
            if isinstance(record.get("crawled_at"), str):
                record["crawled_at"] = datetime.fromisoformat(record["crawled_at"])
    return records