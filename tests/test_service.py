import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio()
async def test_service_process(
    service: Service,
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    name = service.name + "_" + entrypoint.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_message = User(name="Greg")
    async with messaging.transaction(user_topic.name):
        await messaging.send(user_topic, input_message)

    await entrypoint.process(mock=True)

    async with messaging.receive(greeting_topic, name, "test") as output_message:
        assert output_message.parent == input_message.uuid
        assert output_message
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_service_process_batch(
    service: Service,
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    name = service.name + "_" + entrypoint.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_messages = [User(name="Greg", delay=2), User(name="Cara", delay=1)]
    async with messaging.transaction(user_topic.name):
        await messaging.send_batch(user_topic, input_messages)

    await entrypoint.process(mock=True, batch_size=2)

    async with messaging.receive_batch(
        greeting_topic, name, "test", batch_size=2
    ) as output_messages:
        assert len(output_messages) == len(input_messages)
        for input_message, output_message in zip(input_messages, output_messages):
            assert output_message.name == input_message.name
            assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_service_exception(
    service: Service,
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    invalid: Entrypoint[User, Greeting],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_message = User(name="Greg")
    async with messaging.transaction(user_topic.name):
        await messaging.send(user_topic, input_message)

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.process(mock=True)

    assert str(excinfo.value) == "Test"
