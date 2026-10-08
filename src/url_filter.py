
import re
from urllib.parse import urlparse, urlsplit, urlunsplit


def normalize_domain(url: str) -> str:
    """'https://www.Example.com/a' -> 'example.com' (без www и в нижнем регистре).

    Можно передать и просто домен: 'Example.com' -> 'example.com'.
    """
    if "//" not in url:
        url = "//" + url
    host = urlparse(url).netloc.lower()
    return host[4:] if host.startswith("www.") else host

def normalize_url(url: str) -> str:
    """Приводит адрес к единому виду, чтобы один и тот же адрес не считался двумя разными.

    'HTTPS://Example.com#top' -> 'https://example.com/'
    """
    parts = urlsplit(url)
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", parts.query, ""))


class URLFilter:
    """Решает, можно ли ходить по ссылке.

    - allowed_domains: набор доменов, которыми нужно ограничиться (None = любые);
    - exclude_patterns: если URL подходит под любой шаблон, он отбрасывается;
    - include_patterns: если шаблоны заданы, URL должен подойти хотя бы под один.
    Шаблоны это регулярные выражения (поиск внутри адреса).
    Исключение сильнее включения.
    """

    def __init__(self, allowed_domains=None, exclude_patterns=None, include_patterns=None):
        self.allowed_domains = {normalize_domain(d) for d in allowed_domains} if allowed_domains else None
        self.exclude = [re.compile(p) for p in (exclude_patterns or [])]
        self.include = [re.compile(p) for p in (include_patterns or [])]

    def is_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return False
        if self.allowed_domains is not None and normalize_domain(url) not in self.allowed_domains:
            return False
        if any(p.search(url) for p in self.exclude):
            return False
        if self.include and not any(p.search(url) for p in self.include):
            return False
        return True