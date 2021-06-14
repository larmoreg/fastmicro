import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging
from fastmicro.service import Service
from fastmicro.topic import Header, Topic

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
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_message = User(name="Greg")
    input_header = await messaging.send(user_topic, input_message)

    await entrypoint.process(mock=True)

    async with messaging.receive(greeting_topic, name, "test") as output_header:
        assert output_header.parent == input_header.uuid
        output_message = output_header.message
        assert output_message
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_service_process_batch(
    service: Service,
    messaging: Messaging,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    name = service.name + "_" + entrypoint.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_messages = [User(name="Greg"), User(name="Cara")]
    input_headers = await messaging.send_batch(user_topic, input_messages)

    await entrypoint.process(mock=True, batch_size=2)

    async with messaging.receive_batch(
        greeting_topic, name, "test", batch_size=2
    ) as output_headers:
        assert len(output_headers) == len(input_headers)
        for input_header, output_header in zip(
            sorted(input_headers, key=lambda x: str(x.uuid)),
            sorted(output_headers, key=lambda x: str(x.parent)),
        ):
            assert output_header.parent == input_header.uuid
            input_message = input_header.message
            assert input_message
            output_message = output_header.message
            assert output_message
            assert output_message.name == input_message.name
            assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_service_exception(
    service: Service,
    messaging: Messaging,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    invalid: Entrypoint[User, Greeting],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging._subscribe(user_topic.name, name)
    await messaging._subscribe(greeting_topic.name, name)

    input_message = User(name="Greg")
    await messaging.send(user_topic, input_message)

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.process(mock=True)

    assert str(excinfo.value) == "Test"
