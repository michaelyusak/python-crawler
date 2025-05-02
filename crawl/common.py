import hashlib

def is_valid_url(url):
    if url.startswith('http') and not any(bad in url for bad in ['#', 'javascript:', '.pdf']):
        return True
    return False

def hash(raw: str) -> str:
    """
    Returns a SHA-256 hash of the given HTML content.
    
    Args:
        content (str): The raw HTML to hash.
    
    Returns:
        str: Hex digest of the hash.
    """
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()