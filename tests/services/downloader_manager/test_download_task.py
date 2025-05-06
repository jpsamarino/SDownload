import asyncio
import logging
import pytest
from datetime import datetime

from sDownload.services.downloader_manager.download_task import DownloadConfig, DownloadTask


class DummyDownloader:
    async def download_chunk(self, url, start, end):
        _end = end if end is not None else 11
        data = b"123456780123"
        _data = data[start:_end+1]  # verify
        for i in range(0, len(_data), 2):
            chunk = _data[i:i+2]
            yield chunk
            await asyncio.sleep(0.01)


class DummyStorage:
    async def save_binary_data(self, it, name):
        async for _ in it:
            pass


@pytest.mark.asyncio
async def test_download_task_logs_debug(caplog):
    config = DownloadConfig(
        file_name="test.bin",
        file_dir=None,
        file_size=12,
        file_id="123",
        download_url="http://example.com/file",
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
    )

    logger = logging.getLogger("sDownload.test")
    logger.setLevel(logging.DEBUG)

    with caplog.at_level(logging.DEBUG, logger="sDownload.test"):
        task = DownloadTask(
            cfg=config,
            downloader=DummyDownloader(),
            storage=DummyStorage(),
            logger=logger
        )
        await task.start()

    # Mostra todos os logs capturados no terminal
    for record in caplog.records:
        print(f"{record.levelname}: {record.message}")

    # Teste opcional: verificar se mensagens de debug foram emitidas
    assert any("starting" in r.message.lower()
               or "chunk" in r.message.lower() for r in caplog.records)
