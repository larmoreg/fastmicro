import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting, get_entrypoint, get_invalid


@pytest.mark.asyncio()
async def test_service_process(
    service: Service,
    messaging: Messaging,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> None:
    entrypoint = get_entrypoint(service, user_topic, greeting_topic)

    name = service.name + "_" + entrypoint.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_message = User(name="Greg")
    input_header = await messaging.send(user_topic, input_message)

    await entrypoint.process(mock=True)

    async with messaging.receive(greeting_topic, name, "test") as output_header:
        assert output_header.parent == input_header.uuid
        output_message = output_header.message
        assert output_message
        assert output_message.name == "Greg"
        assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_service_exception(
    service: Service,
    messaging: Messaging,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> None:
    entrypoint = get_invalid(service, user_topic, greeting_topic)

    name = service.name + "_" + entrypoint.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_message = User(name="Greg")
    await messaging.send(user_topic, input_message)

    with pytest.raises(RuntimeError) as excinfo:
        await entrypoint.process(mock=True)

    assert str(excinfo.value) == "Test"
