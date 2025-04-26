def is_valid_url(url):
    if url.startswith('http') and not any(bad in url for bad in ['#', 'javascript:', '.pdf']):
        return True
    return False
