import pytest

from fastmicro.registrar import registrar, msgpack_serializer


@pytest.mark.asyncio()
async def test_reregister() -> None:
    registrar.unregister("msgpack")
    registrar.register("msgpack", msgpack_serializer)
