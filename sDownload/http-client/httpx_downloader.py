import httpx


class HttpxDownloader:
    def __init__(self, session):
        self.session = session
