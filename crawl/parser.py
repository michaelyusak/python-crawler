from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for tag in soup.find_all('a', href=True):
        full_url = urljoin(base_url, tag['href'])
        links.append(full_url)
    return links
