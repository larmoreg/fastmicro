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
    greet: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")
    input_header = await messaging.send(user_topic, input_message)

    async with messaging.receive(greeting_topic, service.name, "test") as output_header:
        assert output_header.parent == input_header.uuid
        output_message = output_header.message
        assert output_message
        assert output_message.name == "Greg"
        assert output_message.greeting == "Hello, Greg!"
