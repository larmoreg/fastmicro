import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_service_process(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    greet: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    await user_topic.send(input_message)
    async with greeting_topic.receive(service.name) as output_message:
        assert output_message.name == "Greg"
        assert output_message.greeting == "Hello, Greg!"
