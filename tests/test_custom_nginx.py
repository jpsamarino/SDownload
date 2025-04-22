from testcontainers.core.container import DockerContainer
import pytest
import requests
import time


@pytest.fixture(scope="session")
def nginx_custom():
    container = (
        DockerContainer("my-custom-nginx")
        .with_exposed_ports(80)
    )
    container.start()
    time.sleep(1)  # dá tempo pro nginx subir

    yield f"http://localhost:{container.get_exposed_port(80)}"

    container.stop()


def test_custom_nginx(nginx_custom):
    response = requests.get(f"{nginx_custom}/index.html")
    assert response.status_code == 200
