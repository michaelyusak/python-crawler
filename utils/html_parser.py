import time
from bs4 import BeautifulSoup

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

        body_text = ""
        if self.soup.body:
            body_text = self.soup.body.get_text(separator=" ", strip=True)

        doc = {
            "url": self.url,
            "title": title,
            "content": body_text,
            "type": self.page_type,
            "tags": self.tags or [],
            "indexed_at": int(time.time())  # store epoch seconds
        }

        return doc