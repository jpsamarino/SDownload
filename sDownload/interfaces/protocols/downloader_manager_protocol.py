from typing import Protocol
from sDownload.interfaces.models import URLConfig, DLManagerConfig


class DownloaderManagerProtocol(Protocol):

    def add_url(self, urlConfig: str):
        pass

    def add_urls(self, urls: list):
        pass

    def remove_url(self, url: str):
        pass

    async def wait_until_done(self):
        pass

    async def check_url(self, url: str):
        pass

    async def start(self, indexes: list):
        pass

    async def stop(self, indexes: list):
        pass

    async def pause(self, indexes: list):
        pass

    async def get_download_info(self, indexes: int):
        pass
