import pytest
from typing import Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import UserABC, GreetingABC


@pytest.mark.asyncio()
async def test_service_process(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    entrypoint: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + entrypoint.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_message = user(name="Greg")
    await messaging.send(user_topic, input_message)

    await entrypoint.process()

    async with messaging.receive(greeting_topic, name, "test") as output_message:
        assert output_message.parent == input_message.uuid
        assert output_message
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_service_process_batch(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    entrypoint: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + entrypoint.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_messages = [user(name="Greg", delay=2), user(name="Cara", delay=1)]
    await messaging.send_batch(user_topic, input_messages)

    await entrypoint.process(batch_size=2)

    async with messaging.receive_batch(
        greeting_topic, name, "test", batch_size=2
    ) as output_messages:
        assert len(output_messages) == len(input_messages)
        for input_message, output_message in zip(
            sorted(input_messages, key=lambda x: str(x.name)),
            sorted(output_messages, key=lambda x: str(x.name)),
        ):
            assert output_message.name == input_message.name
            assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio()
async def test_service_exception(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_message = user(name="Greg")
    await messaging.send(user_topic, input_message)

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.process()

    assert str(excinfo.value) == "Test"


@pytest.mark.asyncio()
async def test_service_exception_batch(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_messages = [user(name="Greg", delay=2), user(name="Cara", delay=1)]
    await messaging.send_batch(user_topic, input_messages)

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.process(batch_size=2)

    assert str(excinfo.value) == "Test"


@pytest.mark.asyncio()
async def test_service_retry(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_message = user(name="Greg")
    await messaging.send(user_topic, input_message)

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.process(retries=1, sleep_time=1)

    assert str(excinfo.value) == "Test"


@pytest.mark.asyncio()
async def test_service_retry_batch(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_messages = [user(name="Greg", delay=2), user(name="Cara", delay=1)]
    await messaging.send_batch(user_topic, input_messages)

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.process(batch_size=2, retries=1, sleep_time=1)

    assert str(excinfo.value) == "Test"


@pytest.mark.asyncio()
async def test_service_resend(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_message = user(name="Greg")
    await messaging.send(user_topic, input_message)

    with pytest.raises(RuntimeError) as excinfo:
        for i in range(2):
            await invalid.process(resends=1)

    assert str(excinfo.value) == "Test"


@pytest.mark.asyncio()
async def test_service_resend_batch(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    name = service.name + "_" + invalid.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_messages = [user(name="Greg", delay=2), user(name="Cara", delay=1)]
    await messaging.send_batch(user_topic, input_messages)

    with pytest.raises(RuntimeError) as excinfo:
        for i in range(2):
            await invalid.process(batch_size=2, resends=1)

    assert str(excinfo.value) == "Test"
