import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_service_process(
    messaging: Messaging,
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    greet: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    await service.send(user_topic, input_message)
    await service.process()
    output_message = await service.receive(greeting_topic)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_entrypoint_duplicate(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
) -> None:
    @service.entrypoint(user_topic, reply_topic=greeting_topic, mock=True)
    async def _greet(user: User) -> Greeting:
        pass

    with pytest.raises(
        ValueError, match=f"Entrypoint already registered for topic {user_topic.name}"
    ):

        @service.entrypoint(user_topic, reply_topic=greeting_topic, mock=True)
        async def _greet2(user: User) -> Greeting:
            pass
