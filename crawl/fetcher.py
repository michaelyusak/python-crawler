import requests

def fetch_page(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.text
        else:
            print(f'Failed to fetch {url}: {response.status_code}')
    except Exception as e:
        print(f'Error fetching {url}: {e}')
    return None
