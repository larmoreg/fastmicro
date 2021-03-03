import pytest

from fastmicro.entrypoint import Entrypoint

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_entrypoint_call(greet: Entrypoint[User, Greeting]) -> None:
    input_message = User(name="Greg")

    output_message = await greet.call(input_message)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"
