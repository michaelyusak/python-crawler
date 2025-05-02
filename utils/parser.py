from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for tag in soup.find_all('a', href=True):
        full_url = urljoin(base_url, tag['href'])
        links.append(full_url)
    return links

def html_to_es_doc(url, html, page_type="article", tags=None):
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

    soup = BeautifulSoup(html, "html.parser")

    # Prefer <h1> as title if available, else fallback to <title>
    h1_tag = soup.find("h1")
    title_tag = h1_tag if h1_tag and h1_tag.text.strip() else soup.find("title")
    title = title_tag.text.strip() if title_tag else "Untitled"

    body_text = ""
    if soup.body:
        body_text = soup.body.get_text(separator=" ", strip=True)

    doc = {
        "url": url,
        "title": title,
        "content": body_text,
        "type": page_type,
        "tags": tags or [],
        "indexed_at": int(time.time())  # store epoch seconds
    }

    return doc
