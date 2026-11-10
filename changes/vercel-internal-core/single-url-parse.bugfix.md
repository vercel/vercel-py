Build each request URL in one pass. Query parameters are encoded into the URL before HTTPX2 parses it, instead of having it parse the URL a second time to attach them.
