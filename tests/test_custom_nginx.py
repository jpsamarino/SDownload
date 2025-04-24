import pytest
import httpx


@pytest.mark.asyncio
async def test_container_nginx(nginx_custom):
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(f"{nginx_custom['https']}/json-data")
        assert response.status_code == 200
