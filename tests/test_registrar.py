import pytest

from fastmicro.registrar import registrar, json_serializer


@pytest.mark.asyncio()
async def test_reregister() -> None:
    registrar.unregister("json")
    registrar.register("json", json_serializer)
