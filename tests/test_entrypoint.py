import pytest

from fastmicro.entrypoint import Entrypoint

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_entrypoint_call(entrypoint: Entrypoint[User, Greeting]) -> None:
    input_message = User(name="Greg")

    output_message = await entrypoint.call(input_message, mock=True)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"
