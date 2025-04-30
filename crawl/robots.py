import urllib.parse
import urllib.request
import urllib.robotparser
import logging
import socket

def safe_read_robots_txt(url, timeout=5):
    parsed = urllib.parse.urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = base_url + "/robots.txt"

    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/123.0.0.0 Safari/537.36"
        }

        req = urllib.request.Request(robots_url, headers=headers)

        # Use urllib.request with a timeout
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content = response.read().decode('utf-8')
            rp.parse(content.splitlines())
    except (urllib.error.URLError, socket.timeout) as e:
        logging.warning(f"Failed to fetch robots.txt from {robots_url}: {e}")
        # Optionally, allow crawling if robots.txt fails
        rp.disallow_all = False

    return rp