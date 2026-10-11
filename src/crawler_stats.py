"""Статистика работы краулера и экспорт отчётов (JSON и HTML)."""

import html
import json
import time
from collections import Counter
from datetime import datetime
from urllib.parse import urlparse


class CrawlerStats:
    """Считает, что сделал краулер: страницы, коды ответов, домены, скорость.

    Как пользоваться:
        stats = CrawlerStats()
        stats.start()
        stats.record_page(url, status_code=200)            # успех
        stats.record_page(url, status_code=404, ok=False)  # неудача
        stats.stop()
        print(stats.to_dict())
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock            # подменяем часы в тестах
        self._started = None           # когда начали (по часам clock)
        self._stopped = None           # когда закончили
        self._started_at = None        # время начала для человека (datetime)
        self.successful = 0
        self.failed = 0
        self.status_codes = Counter()  # {200: 90, 404: 3, ...}
        self.domains = Counter()       # {"example.com": 50, ...}
        self.errors = Counter()        # {"TimeoutError": 2, ...}
        self.bytes_total = 0

    # ---------- запись событий ----------

    def start(self):
        """Включить секундомер. Повторный вызов ничего не меняет."""
        if self._started is None:
            self._started = self._clock()
            self._started_at = datetime.now()
            self._stopped = None

    def stop(self):
        """Остановить секундомер (время работы больше не растёт)."""
        if self._started is not None and self._stopped is None:
            self._stopped = self._clock()

    def record_page(self, url, status_code=None, ok=True, error=None, size=0):
        """Записать одну обработанную страницу."""
        if self._started is None:
            self.start()
        if ok:
            self.successful += 1
        else:
            self.failed += 1
            if error:
                self.errors[str(error)] += 1
        if status_code is not None:
            self.status_codes[int(status_code)] += 1
        domain = urlparse(url).netloc
        if domain:
            self.domains[domain] += 1
        self.bytes_total += max(0, int(size or 0))

    # ---------- вычисляемые числа ----------

    @property
    def total_pages(self):
        return self.successful + self.failed

    @property
    def elapsed(self):
        """Сколько секунд работает (или проработал) краулер."""
        if self._started is None:
            return 0.0
        end = self._stopped if self._stopped is not None else self._clock()
        return max(0.0, end - self._started)

    @property
    def pages_per_second(self):
        """Средняя скорость: страниц в секунду."""
        elapsed = self.elapsed
        return self.total_pages / elapsed if elapsed > 0 else 0.0

    @property
    def success_rate(self):
        """Доля успешных, от 0 до 1."""
        return self.successful / self.total_pages if self.total_pages else 0.0

    def top_domains(self, n=10):
        """Домены с наибольшим числом страниц: [(домен, число), ...]."""
        return self.domains.most_common(n)

    def to_dict(self, top_n=10):
        """Вся статистика одним словарём (его же пишем в JSON)."""
        return {
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "success_rate": round(self.success_rate, 4),
            "elapsed_seconds": round(self.elapsed, 3),
            "pages_per_second": round(self.pages_per_second, 3),
            "started_at": self._started_at.isoformat(timespec="seconds")
            if self._started_at else None,
            "status_codes": {str(k): v for k, v in sorted(self.status_codes.items())},
            "top_domains": [
                {"domain": d, "pages": c} for d, c in self.top_domains(top_n)
            ],
            "errors": dict(self.errors.most_common()),
            "bytes_total": self.bytes_total,
        }

    # ---------- экспорт ----------

    def export_to_json(self, filename):
        """Сохранить статистику в JSON-файл."""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)

    def export_to_html_report(self, filename, title="Отчёт краулера"):
        """Сохранить красивый HTML-отчёт: цифры, таблицы и столбчатые графики.

        Графики нарисованы обычным HTML/CSS, поэтому интернет не нужен.
        """
        data = self.to_dict()
        esc = html.escape

        cards = [
            ("Всего страниц", data["total_pages"]),
            ("Успешно", data["successful"]),
            ("Ошибок", data["failed"]),
            ("Успешность", f"{data['success_rate'] * 100:.1f}%"),
            ("Время работы", f"{data['elapsed_seconds']:.1f} с"),
            ("Скорость", f"{data['pages_per_second']:.2f} стр/с"),
        ]
        cards_html = "".join(
            f'<div class="card"><div class="num">{esc(str(v))}</div>'
            f'<div class="lbl">{esc(k)}</div></div>'
            for k, v in cards
        )

        status_rows = [(k, v) for k, v in data["status_codes"].items()]
        domain_rows = [(d["domain"], d["pages"]) for d in data["top_domains"]]
        error_rows = list(data["errors"].items())

        def chart(rows, color_fn=None):
            if not rows:
                return '<p class="empty">Нет данных</p>'
            biggest = max(v for _, v in rows) or 1
            out = []
            for name, value in rows:
                width = max(2, round(value / biggest * 100))
                color = color_fn(name) if color_fn else "#4f7cff"
                out.append(
                    '<div class="row">'
                    f'<span class="name">{esc(str(name))}</span>'
                    f'<span class="bar" style="width:{width}%;background:{color}"></span>'
                    f'<span class="val">{value}</span></div>'
                )
            return "".join(out)

        def status_color(code):
            first = str(code)[:1]
            return {"2": "#2faa5a", "3": "#e0a526", "4": "#e8743b",
                    "5": "#d64545"}.get(first, "#888")

        started = data["started_at"] or "—"
        page = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, sans-serif; margin: 0;
        background: #f4f6fb; color: #1d2433; }}
 main {{ max-width: 900px; margin: 0 auto; padding: 24px 16px; }}
 h1 {{ margin: 0 0 4px; }} .sub {{ color: #6b7488; margin-bottom: 20px; }}
 .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
          gap: 12px; margin-bottom: 24px; }}
 .card {{ background: #fff; border-radius: 10px; padding: 14px;
         box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
 .num {{ font-size: 24px; font-weight: 700; }} .lbl {{ color: #6b7488; font-size: 13px; }}
 section {{ background: #fff; border-radius: 10px; padding: 16px 18px;
           margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
 h2 {{ margin: 0 0 12px; font-size: 18px; }}
 .row {{ display: flex; align-items: center; gap: 10px; margin: 6px 0; }}
 .name {{ flex: 0 0 180px; font-size: 14px; overflow: hidden;
         text-overflow: ellipsis; white-space: nowrap; }}
 .bar {{ height: 16px; border-radius: 4px; display: inline-block; }}
 .val {{ font-size: 13px; color: #6b7488; }}
 table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
 th, td {{ text-align: left; padding: 6px 4px; border-bottom: 1px solid #e6e9f0; }}
 .empty {{ color: #6b7488; }}
</style></head><body><main>
<h1>{esc(title)}</h1>
<div class="sub">Запуск: {esc(started)}</div>
<div class="cards">{cards_html}</div>
<section><h2>Коды ответов</h2>{chart(status_rows, status_color)}</section>
<section><h2>Топ доменов</h2>{chart(domain_rows)}</section>
<section><h2>Ошибки</h2>
{"<table><tr><th>Тип</th><th>Сколько</th></tr>" +
 "".join(f"<tr><td>{esc(k)}</td><td>{v}</td></tr>" for k, v in error_rows) +
 "</table>" if error_rows else '<p class="empty">Ошибок не было</p>'}
</section>
</main></body></html>"""
        with open(filename, "w", encoding="utf-8") as f:
            f.write(page)