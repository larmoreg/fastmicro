import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_entrypoint_call(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    output_message = await entrypoint.call(input_message, mock=True)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_entrypoint_call_batch(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_messages = [User(name="Greg", delay=2), User(name="Cara", delay=1)]

    output_messages = await entrypoint.call_batch(input_messages, mock=True, batch_size=2)

    assert len(output_messages) == len(input_messages)
    for input_message, output_message in zip(input_messages, output_messages):
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_entrypoint_exception(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    invalid: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.call(input_message, mock=True)

    assert str(excinfo.value) == "Test"
