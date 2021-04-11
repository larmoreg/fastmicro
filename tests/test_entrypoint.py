import pytest

from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting, get_entrypoint, get_invalid


@pytest.mark.asyncio()
async def test_entrypoint_call(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> None:
    entrypoint = get_entrypoint(service, user_topic, greeting_topic)

    input_message = User(name="Greg")

    output_message = await entrypoint.call(input_message, mock=True)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_entrypoint_exception(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> None:
    entrypoint = get_invalid(service, user_topic, greeting_topic)

    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await entrypoint.call(input_message, mock=True)

    assert str(excinfo.value) == "Test"
