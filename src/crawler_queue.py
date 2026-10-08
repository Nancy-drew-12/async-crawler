import asyncio
import heapq
import itertools


class CrawlerQueue:
    """Очередь URL с приоритетами.

    - чем больше priority, тем раньше URL выйдет из очереди;
    - при равном приоритете порядок такой же, как порядок добавления;
    - один и тот же URL никогда не попадает в очередь дважды.
    """

    def __init__(self):
        self._heap = []                    # элементы: (-priority, номер, url)
        self._counter = itertools.count()  # номер добавления, чтобы сохранять порядок
        self._seen = set()                 # все URL, которые когда-либо добавляли
        self._in_progress = set()          # URL, которые выданы, но ещё не завершены
        self._processed = set()            # успешно обработанные
        self._failed = {}                  # url -> текст ошибки
        self._duplicates = 0               # сколько повторных добавлений отклонили

    def add_url(self, url: str, priority: int = 0) -> bool:
        """Добавляет URL в очередь. Возвращает True, если добавили, и False, если такой уже был."""
        if not isinstance(url, str) or not url:
            raise TypeError("url должен быть непустой строкой")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("priority должен быть целым числом")
        if url in self._seen:
            self._duplicates += 1
            return False
        self._seen.add(url)
        # heapq достаёт наименьший элемент, поэтому приоритет берём со знаком минус
        heapq.heappush(self._heap, (-priority, next(self._counter), url))
        return True

    async def get_next(self) -> str | None:
        """Возвращает следующий URL.

        Если очередь пуста, но другие URL ещё обрабатываются (они могут добавить новые ссылки),
        ждёт. Если пусто и ничего не обрабатывается, возвращает None: работа закончена.
        """
        while True:
            if self._heap:
                _, _, url = heapq.heappop(self._heap)
                self._in_progress.add(url)
                return url
            if not self._in_progress:
                return None
            await asyncio.sleep(0.05)

    def mark_processed(self, url: str) -> None:
        """Отмечает URL как успешно обработанный."""
        self._in_progress.discard(url)
        self._failed.pop(url, None)
        self._processed.add(url)

    def mark_failed(self, url: str, error: str) -> None:
        """Отмечает URL как неудачный и запоминает причину."""
        self._in_progress.discard(url)
        self._failed[url] = error

    def get_stats(self) -> dict:
        """Статистика очереди."""
        return {
            "queued": len(self._heap),
            "in_progress": len(self._in_progress),
            "processed": len(self._processed),
            "failed": len(self._failed),
            "total_added": len(self._seen),
            "duplicates_skipped": self._duplicates,
        }