"""Сохранение страниц в CSV-файл. Запись асинхронная (aiofiles), с буфером."""
import asyncio
import csv
import io
import json
import os
import sys
from datetime import datetime

import aiofiles

from src.storage import RECORD_FIELDS, DataStorage, StorageError, validate_record

# Эти поля в CSV не помещаются как есть, поэтому хранятся как JSON-текст в одной ячейке
JSON_FIELDS = ("links", "metadata")


def _cell(name: str, value) -> str:
    """Превращает значение поля в текст для одной ячейки CSV."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if name in JSON_FIELDS or isinstance(value, (list, dict)):
        text = json.dumps(value, ensure_ascii=False, default=str)
    else:
        text = str(value)
    return text.replace("\x00", "")             # нулевой символ ломает многие программы для таблиц


class CSVStorage(DataStorage):
    """Пишет записи в CSV-файл по мере поступления.

    Заголовок берётся из первой записи (стандартные поля идут первыми) или из fieldnames.
    Списки и словари (links, metadata) пишутся как JSON-текст, поэтому их можно прочитать обратно.
    Запятые, кавычки и переносы строк внутри текста экранируются автоматически.
    encoding: "utf-8" по умолчанию; "utf-8-sig" помогает Excel правильно показать кириллицу;
              символы, которых нет в кодировке (например, эмодзи в cp1251), заменяются на «?».
    delimiter: "," по умолчанию; русский Excel охотнее открывает файлы с ";".
    append=True дописывает в существующий файл, иначе файл создаётся заново.
    """

    def __init__(self, path: str, encoding: str = "utf-8", delimiter: str = ",",
                 fieldnames: list[str] | None = None, append: bool = False, buffer_size: int = 50):
        super().__init__()
        if not isinstance(buffer_size, int) or isinstance(buffer_size, bool) or buffer_size < 1:
            raise ValueError("buffer_size должен быть целым числом не меньше 1")
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise ValueError("delimiter должен быть одним символом")
        self.path = os.fspath(path)
        self.encoding = encoding
        self.delimiter = delimiter
        self.append = append
        self.buffer_size = buffer_size
        self.fieldnames = list(fieldnames) if fieldnames else None
        self._buffer: list[str] = []        # готовые строки CSV, ещё не записанные на диск
        self._file = None
        self._closed = False
        self._lock = asyncio.Lock()
        self.flushes = 0

    # ---------- текст строки ----------

    def _format_row(self, values: list[str]) -> str:
        out = io.StringIO()
        csv.writer(out, delimiter=self.delimiter, lineterminator="\n").writerow(values)
        return out.getvalue()

    async def _existing_header(self) -> list[str] | None:
        """Если файл уже есть и не пуст, возвращает его заголовок."""
        if not (os.path.exists(self.path) and os.path.getsize(self.path) > 0):
            return None
        try:
            async with aiofiles.open(self.path, "r", encoding=self.encoding, newline="") as file:
                line = await file.readline()
        except (OSError, UnicodeError) as error:
            raise StorageError(f"не удалось прочитать заголовок {self.path}: {error}") from error
        header = next(csv.reader([line], delimiter=self.delimiter), None)
        if not header:
            raise StorageError(f"в файле {self.path} нет заголовка CSV")
        return header

    async def _prepare(self, data: dict) -> None:
        """При первом сохранении выбирает заголовок: из файла (при дозаписи), из fieldnames или из записи."""
        if self.fieldnames is not None:
            return
        if self.append:
            self.fieldnames = await self._existing_header()
        if self.fieldnames is None:
            extra = [name for name in data if name not in RECORD_FIELDS]
            self.fieldnames = [name for name in RECORD_FIELDS if name in data] + extra

    # ---------- открытие файла ----------

    async def _open(self) -> None:
        try:
            existing = self.append and os.path.exists(self.path) and os.path.getsize(self.path) > 0
            # при дозаписи BOM второй раз писать нельзя
            encoding = "utf-8" if existing and self.encoding.lower().replace("_", "-") == "utf-8-sig" else self.encoding
            mode = "a" if existing else "w"
            lost_newline = existing and not await self._ends_with_newline()
            self._file = await aiofiles.open(self.path, mode, encoding=encoding, errors="replace", newline="")
            if lost_newline:
                await self._file.write("\n")        # прошлый запуск оборвался на середине строки
            if not existing:
                await self._file.write(self._format_row(self.fieldnames))
        except OSError as error:
            if self._file is not None:
                await self._file.close()
                self._file = None
            raise StorageError(f"не удалось открыть {self.path}: {error}") from error

    async def _ends_with_newline(self) -> bool:
        async with aiofiles.open(self.path, "rb") as file:
            await file.seek(-1, 2)
            return (await file.read(1)) == b"\n"

    # ---------- запись ----------

    async def save(self, data: dict) -> None:
        validate_record(data)
        async with self._lock:
            if self._closed:
                raise StorageError("хранилище уже закрыто")
            await self._prepare(data)
            row = self._format_row([_cell(name, data.get(name)) for name in self.fieldnames])
            self._buffer.append(row)
            if len(self._buffer) >= self.buffer_size:
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
        if self._file is None:
            if self.fieldnames is None:
                self.fieldnames = await self._existing_header() if self.append else None
                self.fieldnames = self.fieldnames or list(RECORD_FIELDS)
            await self._open()
        if not self._buffer:
            return
        try:
            await self._file.write("".join(self._buffer))
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
                await self._flush_locked()      # при необходимости создаёт файл хотя бы с заголовком
            finally:
                self._closed = True
                if self._file is not None:
                    await self._file.close()
                    self._file = None

    def get_stats(self) -> dict:
        return {"saved": self.saved_count, "buffered": len(self._buffer), "flushes": self.flushes,
                "path": self.path}


async def read_csv(path: str, encoding: str = "utf-8", delimiter: str = ",", parse: bool = True) -> list[dict]:
    """Читает файл, который написал CSVStorage. С parse=True возвращает links, metadata,
    status_code и crawled_at в исходном виде (список, словарь, число, дата)."""
    async with aiofiles.open(path, "r", encoding=encoding, newline="") as file:
        text = await file.read()
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))      # страницы бывают длиннее лимита по умолчанию
    rows = list(csv.DictReader(io.StringIO(text, newline=""), delimiter=delimiter))
    if parse:
        for row in rows:
            for name in JSON_FIELDS:
                if name in row:
                    row[name] = json.loads(row[name]) if row[name] else ([] if name == "links" else {})
            if "status_code" in row:
                row["status_code"] = int(row["status_code"]) if row["status_code"] else None
            if row.get("crawled_at"):
                row["crawled_at"] = datetime.fromisoformat(row["crawled_at"])
    return rows