"""Мониторинг в реальном времени: прогресс-бар, скорость, ETA, активные задачи."""

import asyncio


def format_duration(seconds):
    """65 -> '1м 05с', 3700 -> '1ч 01м', None -> '—'."""
    if seconds is None:
        return "—"
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}с"
    if seconds < 3600:
        return f"{seconds // 60}м {seconds % 60:02d}с"
    return f"{seconds // 3600}ч {seconds % 3600 // 60:02d}м"


def make_bar(done, total, width=20):
    """Строка вида [########------------]  40%."""
    if total <= 0:
        fraction = 0.0
    else:
        fraction = min(1.0, max(0.0, done / total))
    filled = int(round(fraction * width))
    return f"[{'#' * filled}{'-' * (width - filled)}] {fraction * 100:3.0f}%"


def estimate_eta(done, total, elapsed):
    """Сколько секунд осталось. None, если пока нечего считать."""
    if done <= 0 or elapsed <= 0 or total <= done:
        return 0.0 if total and done >= total else None
    speed = done / elapsed
    return (total - done) / speed


def format_progress(done, total, elapsed, active, failed=0, width=20):
    """Одна строка мониторинга."""
    speed = done / elapsed if elapsed > 0 else 0.0
    eta = estimate_eta(done, total, elapsed)
    return (f"{make_bar(done, total, width)} {done}/{total} | {speed:.1f} стр/с | "
            f"осталось ~{format_duration(eta)} | активных: {active} | ошибок: {failed}")


class ProgressMonitor:
    """Раз в interval секунд печатает строку прогресса.

    Монитор сам ничего не считает, а спрашивает цифры у функции get_state,
    которая возвращает словарь: done, total, elapsed, active, failed.
    """

    def __init__(self, get_state, interval=1.0, printer=print):
        self._get_state = get_state
        self.interval = interval
        self._print = printer
        self._task = None

    def line(self):
        s = self._get_state()
        return format_progress(s["done"], s["total"], s["elapsed"], s["active"],
                               s.get("failed", 0))

    async def _loop(self):
        while True:
            await asyncio.sleep(self.interval)
            self._print(self.line())

    def start(self):
        """Запустить фоновый вывод (нужен работающий цикл asyncio)."""
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def stop(self, final=True):
        """Остановить вывод и напечатать итоговую строку."""
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if final:
            self._print(self.line() + "  | готово")