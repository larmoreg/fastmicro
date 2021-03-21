import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_service_process(
    service: Service,
    messaging: Messaging,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    name = service.name + "_" + entrypoint.callback.__name__
    await messaging._prepare(user_topic.name, name)
    await messaging._prepare(greeting_topic.name, name)

    input_message = User(name="Greg")
    input_header = await messaging.send(user_topic, input_message)

    await entrypoint.process()

    async with messaging.receive(greeting_topic, name, "test") as output_header:
        assert output_header.parent == input_header.uuid
        output_message = output_header.message
        assert output_message
        assert output_message.name == "Greg"
        assert output_message.greeting == "Hello, Greg!"
