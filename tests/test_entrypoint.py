import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging
from fastmicro.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_entrypoint_call(greet: Entrypoint[User, Greeting]) -> None:
    input_message = User(name="Greg")

    output_message = await greet.call(input_message)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_entrypoint_process(
    messaging: Messaging,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    greet: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")
    input_serialized = await user_topic.serialize(input_message)

    await messaging.send(user_topic.name, input_serialized)
    await greet.process()
    output_serialized = await messaging.receive(greeting_topic.name, "test")

    output_message = await greeting_topic.deserialize(output_serialized)
    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_entrypoint_call_fail(fail: Entrypoint[User, Greeting]) -> None:
    with pytest.raises(RuntimeError, match="Test error"):
        await fail.call(User(name="Greg"))
