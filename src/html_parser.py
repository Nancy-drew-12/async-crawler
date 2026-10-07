import logging
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup
from urllib.parse import urldefrag, urljoin, urlparse

logger = logging.getLogger(__name__)


class HTMLParser:
    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        title = soup.title.get_text(strip=True) if soup.title else ""
        metadata = {"title": title, "description": "", "keywords": ""}
        for name in ("description", "keywords"):
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                metadata[name] = tag["content"].strip()
        return metadata
    
    def extract_links(self, soup: BeautifulSoup, base_url: str,
                      same_domain_only: bool = False) -> list[str]:
        base_host = urlparse(base_url).netloc
        links = []
        seen = set()
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if href.startswith(("#", "mailto:", "javascript:", "tel:")):
                continue
            absolute, _ = urldefrag(urljoin(base_url, href))
            if not self._is_valid_url(absolute):
                continue
            if same_domain_only and urlparse(absolute).netloc != base_host:
                continue
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)
        return links

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    
    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        if selector:
            elements = soup.select(selector)
        else:
            elements = [soup.body or soup]

        parts = [el.get_text(separator=" ", strip=True) for el in elements]
        return " ".join(part for part in parts if part)
    
    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images = []
        for tag in soup.find_all("img", src=True):
            src = urljoin(base_url, tag["src"].strip())
            if self._is_valid_url(src):
                images.append({"src": src, "alt": tag.get("alt", "").strip()})
        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict:
        return {
            level: [h.get_text(strip=True) for h in soup.find_all(level)]
            for level in ("h1", "h2", "h3")
        }
    
    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        result = []
        for lst in soup.find_all(["ul", "ol"]):
            items = [li.get_text(" ", strip=True)
                     for li in lst.find_all("li", recursive=False)]
            if items:
                result.append({"type": lst.name, "items": items})
        return result
    
    def _safe(self, name: str, func, default, url: str, errors: list):
        try:
            return func()
        except Exception as e:
            logger.warning("Ошибка при извлечении '%s' на %s: %s: %s",
                           name, url, type(e).__name__, e)
            errors.append(f"{name}: {type(e).__name__}: {e}")
            return default

    async def parse_html(self, html: str, url: str) -> dict:
        errors = []
        result = {
            "url": url, "title": "", "text": "", "links": [], "metadata": {},
            "images": [], "headings": {"h1": [], "h2": [], "h3": []},
            "tables": [], "lists": [], "errors": errors,
        }
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            logger.warning("Не удалось разобрать HTML %s: %s: %s", url, type(e).__name__, e)
            errors.append(f"parse: {type(e).__name__}: {e}")
            return result

        result["metadata"] = self._safe("metadata", lambda: self.extract_metadata(soup),
                                        {}, url, errors)
        result["title"] = result["metadata"].get("title", "")
        result["links"] = self._safe("links", lambda: self.extract_links(soup, url),
                                     [], url, errors)
        result["images"] = self._safe("images", lambda: self.extract_images(soup, url),
                                      [], url, errors)
        result["headings"] = self._safe("headings", lambda: self.extract_headings(soup),
                                        result["headings"], url, errors)
        result["tables"] = self._safe("tables", lambda: self.extract_tables(soup),
                                      [], url, errors)
        result["lists"] = self._safe("lists", lambda: self.extract_lists(soup),
                                     [], url, errors)
        result["text"] = self._safe("text", lambda: self.extract_text(soup),
                                    "", url, errors)
        return result