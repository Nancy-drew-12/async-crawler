import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from bs4 import BeautifulSoup

from src.html_parser import HTMLParser

parser = HTMLParser()
URL = "https://example.com/docs/"


def soup_of(html):
    return BeautifulSoup(html, "lxml")


def section(title):
    print(f"\n=== {title} ===")


def main():
    section("1. Валидный HTML: метаданные, заголовки, текст")
    html = """<html><head><title>Тест</title>
    <meta name="description" content="Описание">
    <meta name="keywords" content="a, b"></head>
    <body><script>var x = 1;</script><h1>Главный</h1><h2>Раздел</h2>
    <p>Текст страницы</p></body></html>"""
    result = asyncio.run(parser.parse_html(html, URL))
    print(result["metadata"])
    assert result["title"] == "Тест"
    assert result["metadata"]["description"] == "Описание"
    assert result["metadata"]["keywords"] == "a, b"
    assert result["headings"]["h1"] == ["Главный"]
    assert result["headings"]["h2"] == ["Раздел"]
    assert "Текст страницы" in result["text"]
    assert "var x" not in result["text"], "код из script не должен попадать в текст"
    assert result["errors"] == []

    section("2. Ссылки: относительные становятся абсолютными")
    links_html = ('<a href="/about">1</a><a href="page.html">2</a>'
                  '<a href="https://other.com/x#top">3</a><a href="mailto:a@b.ru">4</a>'
                  '<a href="#top">5</a><a href="/about">6</a>')
    links = parser.extract_links(soup_of(links_html), URL)
    print(links)
    assert links == ["https://example.com/about",
                     "https://example.com/docs/page.html",
                     "https://other.com/x"]

    section("3. Фильтр внешних ссылок")
    links = parser.extract_links(soup_of(links_html), URL, same_domain_only=True)
    print(links)
    assert links == ["https://example.com/about",
                     "https://example.com/docs/page.html"]

    section("4. Картинки, таблицы, списки")
    html = ('<img src="/a.png" alt="Кот"><img src="b.jpg">'
            '<table><tr><th>Имя</th><th>Баланс</th></tr><tr><td>Анна</td><td>150</td></tr></table>'
            '<ul><li>один</li><li>два</li></ul><ol><li>раз</li></ol>')
    soup = soup_of(html)
    images = parser.extract_images(soup, URL)
    print(images)
    assert images == [{"src": "https://example.com/a.png", "alt": "Кот"},
                      {"src": "https://example.com/docs/b.jpg", "alt": ""}]
    assert parser.extract_tables(soup) == [[["Имя", "Баланс"], ["Анна", "150"]]]
    assert parser.extract_lists(soup) == [{"type": "ul", "items": ["один", "два"]},
                                          {"type": "ol", "items": ["раз"]}]

    section("5. Битый HTML не ломает парсер")
    broken = ('<html><head><title>Тест</title></head><body><h1>Привет'
              '<p>Незакрытый <b>тег<a href="/x">ссылка</body>')
    result = asyncio.run(parser.parse_html(broken, "https://example.com/"))
    print(result["title"], result["links"])
    assert result["title"] == "Тест"
    assert result["links"] == ["https://example.com/x"]
    assert result["errors"] == []

    section("6. Пустой вход и None не ломают парсер")
    result = asyncio.run(parser.parse_html("", URL))
    assert result["title"] == "" and result["links"] == []
    result = asyncio.run(parser.parse_html(None, URL))
    print("None ->", result["errors"])
    assert isinstance(result, dict) and result["links"] == []

    section("7. Сбой одного блока: остальные результаты сохраняются")

    class BrokenParser(HTMLParser):
        def extract_tables(self, soup):
            raise ValueError("сломано нарочно")

    result = asyncio.run(BrokenParser().parse_html(
        "<html><head><title>Тест</title></head><body><a href='/x'>x</a></body></html>",
        "https://example.com/"))
    print(result["errors"])
    assert result["tables"] == []
    assert result["title"] == "Тест"
    assert result["links"] == ["https://example.com/x"]
    assert len(result["errors"]) == 1 and "tables" in result["errors"][0]

    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    main()
    