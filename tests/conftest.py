import asyncio
import pytest_asyncio
from testcontainers.core.container import DockerContainer
from sDownload.utils import find_available_port


@pytest_asyncio.fixture(scope="session")
async def nginx_custom():
    http_port = find_available_port(8080, 8400)
    https_port = find_available_port(8401, 8600)

    container = (
        DockerContainer("sdownload_test_nginx")
        .with_bind_ports(80, http_port)
        .with_bind_ports(443, https_port)
    )

    container.start()
    await asyncio.sleep(2)

    yield {
        "http": f"http://{container.get_container_host_ip()}:{http_port}",
        "https": f"https://{container.get_container_host_ip()}:{https_port}",
    }

    container.stop()


@pytest_asyncio.fixture(scope="session")
async def webdav_server():
    """
    Fixture that starts a WebDAV container (bytemark/webdav) for testing.
    """
    port = find_available_port(9000, 9500)
    user = "admin"
    password = "admin"
    # Set credentials via environment variables
    container = (
        DockerContainer("sdownload_test_webdav")
        .with_bind_ports(80, port)
        .with_env("USER", user)
        .with_env("PASSWORD", password)
        .with_env("AUTH_TYPE", "Basic")
    )

    container.start()
    # Give it a moment to initialize
    await asyncio.sleep(2)

    host = container.get_container_host_ip()
    url = f"http://{host}:{port}"

    yield {
        "url": url,
        "auth": (user, password),
    }

    container.stop()


@pytest_asyncio.fixture(scope="session")
async def webdav_public_server():
    """
    Fixture that starts a Public WebDAV container (no auth).
    """
    port = find_available_port(9501, 9999)

    # Reuse the same image but allow anonymous methods
    container = (
        DockerContainer("sdownload_test_webdav")
        .with_bind_ports(80, port)
        .with_env("ANONYMOUS_METHODS", "GET,PROPFIND,OPTIONS,REPORT")
    )

    container.start()
    await asyncio.sleep(2)

    host = container.get_container_host_ip()
    url = f"http://{host}:{port}"

    yield url

    container.stop()
