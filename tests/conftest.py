import asyncio
import random
import socket
import pytest_asyncio
from testcontainers.core.container import DockerContainer


def find_available_port(start=8000, end=9000):
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('localhost', port)) != 0:
                return port
    raise RuntimeError("No available ports found")


@pytest_asyncio.fixture(scope="session")
async def nginx_custom():
    http_port = random.randint(8080, 8400)
    https_port = random.randint(8401, 8600)

    container = (
        DockerContainer("sdownload_test_nginx")
        .with_bind_ports(80, http_port)
        .with_bind_ports(443, https_port)
    )

    container.start()
    await asyncio.sleep(2)

    yield {
        "http": f"http://{container.get_container_host_ip()}:{http_port}",
        "https": f"https://{container.get_container_host_ip()}:{https_port}"
    }

    container.stop()
