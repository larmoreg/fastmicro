import pytest

from fastmicro.registrar import registrar, json_serializer


@pytest.mark.asyncio()
async def test_duplicate() -> None:
    with pytest.raises(ValueError, match="Serializer json already registered"):
        registrar.register("json", json_serializer)


@pytest.mark.asyncio()
async def test_get_unknown() -> None:
    with pytest.raises(ValueError, match="Unknown serializer test"):
        registrar.get("test")


@pytest.mark.asyncio()
async def test_unregister_unknown() -> None:
    with pytest.raises(ValueError, match="Unknown serializer test"):
        registrar.unregister("test")


@pytest.mark.asyncio()
async def test_reregister() -> None:
    registrar.unregister("json")
    registrar.register("json", json_serializer)
