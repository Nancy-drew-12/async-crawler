from src.url_filter import URLFilter

f = URLFilter(allowed_domains={"example.com"})
print(f.is_allowed("https://example.com/a"),      # True
      f.is_allowed("https://www.example.com/b"),  # True (www не мешает)
      f.is_allowed("https://other.org/"),         # False (чужой домен)
      f.is_allowed("mailto:a@b.c"))               # False (не веб-ссылка)

f = URLFilter(exclude_patterns=[r"\.pdf$", r"/login"])
print(f.is_allowed("https://x.com/doc.pdf"),      # False
      f.is_allowed("https://x.com/login"),        # False
      f.is_allowed("https://x.com/page"))         # True

f = URLFilter(include_patterns=[r"/blog/"])
print(f.is_allowed("https://x.com/blog/1"),       # True
      f.is_allowed("https://x.com/shop"))         # False

f = URLFilter(include_patterns=[r"/blog/"], exclude_patterns=[r"draft"])
print(f.is_allowed("https://x.com/blog/draft-1"), # False (исключение сильнее)
      f.is_allowed("https://x.com/blog/2"))       # True