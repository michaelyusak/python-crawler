import time
from bs4 import BeautifulSoup, Tag

class HtmlParser:
    def __init__(self, url: str, html: str, page_type: str = "article", tags: list[str] = None):
        self.url = url
        self.page_type = page_type
        self.tags = tags
        self.soup = BeautifulSoup(html, "html.parser")

    def __find_title(self) -> str:
        h1_tag = self.soup.find("h1")
        title_tag = h1_tag if h1_tag and h1_tag.text.strip() else self.soup.find("title")
        title = title_tag.text.strip() if title_tag else "Untitled"
        return title
    
    def __filter(self):
        # Remove obvious noise tags
        for tag in self.soup(["script", "style", "nav", "footer", "header", "aside", "form", "noscript", "svg"]):
            tag.decompose()

        bad_keywords = ["sidebar", "footer", "header", "nav", "menu", "popup", "banner", "ads", "cookie"]

        # Collect elements to be removed first
        to_remove = []

        for el in self.soup.find_all(True):  # True matches all tags
            if not isinstance(el, Tag):
                continue

            classes = el.get("class") or []
            if any(any(bad in cls.lower() for bad in bad_keywords) for cls in classes):
                to_remove.append(el)
                continue

            el_id = el.get("id") or ""
            if any(bad in el_id.lower() for bad in bad_keywords):
                to_remove.append(el)

        # Now remove them safely
        for el in to_remove:
            el.decompose()

        print(self.soup.get_text())

    def __find_content(self) -> str:
        # Priority 1: <article>
        article = self.soup.find("article")
        if article:
            return article.get_text(separator=" ", strip=True)

        # Priority 2: <main>
        main = self.soup.find("main")
        if main:
            return main.get_text(separator=" ", strip=True)

        # Priority 3: Largest <div> by text length (excluding headers/footers/navs)
        divs = self.soup.find_all("div")
        content_divs = [
            div for div in divs
            if not div.get("class") or not any(c in div.get("class", []) for c in ["header", "footer", "nav", "sidebar"])
        ]
        if content_divs:
            largest_div = max(content_divs, key=lambda d: len(d.get_text(strip=True)))
            return largest_div.get_text(separator=" ", strip=True)

        # Fallback: Entire body
        if self.soup.body:
            return self.soup.body.get_text(separator=" ", strip=True)
    
    def html_to_doc(self):
        """
        Transform raw HTML into an Elasticsearch document.

        Args:
            url (str): URL of the page.
            html (str): Raw HTML content.
            page_type (str): Type of page (default: "article").
            tags (list): List of tags (optional).

        Returns:
            dict: Elasticsearch-ready document.
        """
            
        title = self.__find_title()

        # Filter before find content
        self.__filter()

        content = self.__find_content()

        doc = {
            "url": self.url,
            "title": title,
            "content": content,
            "type": self.page_type,
            "tags": self.tags or [],
            "indexed_at": int(time.time())  # store epoch seconds
        }

        return doc