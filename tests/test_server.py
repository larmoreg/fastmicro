import logging
import pytest
from uuid import uuid4

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.topic import Topic

from .conftest import User, Greeting


@pytest.mark.asyncio
async def test_process(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message
    test_message = Greeting(
        name=input_message.name, greeting=f"Hello, {input_message.name}!"
    )

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send([input_header])

        await _entrypoint.process()

        async with _entrypoint.reply_topic.receive("test", "test") as output_headers:
            assert len(output_headers) == 1
            output_header = output_headers[0]
            assert output_header.correlation_id == input_header.correlation_id
            assert not output_header.error
            assert output_header.message
            assert output_header.message == test_message


@pytest.mark.asyncio
async def test_timeout(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg", delay=1)
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send([input_header])

        await _entrypoint.process(processing_timeout=0.1)

        async with _entrypoint.reply_topic.receive("test", "test") as output_headers:
            assert len(output_headers) == 1
            output_header = output_headers[0]
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Timed out after 0.1 sec"
            assert not output_header.message


@pytest.mark.asyncio
async def test_exception(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _error_entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_error_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send([input_header])

        await _error_entrypoint.process()

        async with _error_entrypoint.reply_topic.receive(
            "test", "test"
        ) as output_headers:
            assert len(output_headers) == 1
            output_header = output_headers[0]
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Test"
            assert not output_header.message


@pytest.mark.asyncio
async def test_retries(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _error_entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_error_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send([input_header])

        await _error_entrypoint.process(retries=1, sleep_time=0.1)

        async with _error_entrypoint.reply_topic.receive(
            "test", "test"
        ) as output_headers:
            assert len(output_headers) == 1
            output_header = output_headers[0]
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Test"
            assert not output_header.message


@pytest.mark.asyncio
async def test_resends(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _error_entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_error_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send([input_header])

        for _ in range(2):
            await _error_entrypoint.process(resends=1)

        async with _error_entrypoint.reply_topic.receive(
            "test", "test"
        ) as output_headers:
            assert len(output_headers) == 1
            output_header = output_headers[0]
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Test"
            assert not output_header.message


@pytest.mark.asyncio
async def test_raises(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _error_entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_error_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send([input_header])

        with pytest.raises(RuntimeError) as excinfo:
            await _error_entrypoint.process(raises=True)

        assert str(excinfo.value) == "Test"
