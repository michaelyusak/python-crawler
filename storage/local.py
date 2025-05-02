import os
import hashlib

def save_page(url, html):
    os.makedirs('data', exist_ok=True)
    filename = hashlib.md5(url.encode()).hexdigest() + '.html'
    filepath = os.path.join('data', filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)