import logging
import pytest
from uuid import uuid4

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC, TopicABC

from .conftest import User, Greeting


@pytest.mark.asyncio
async def test_process(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test")
        await user_topic.send(input_header)

        await _entrypoint.process()

        async with _entrypoint.reply_topic.receive("test", "test") as output_header:
            assert output_header.correlation_id == input_header.correlation_id
            assert not output_header.error
            assert output_header.message
            assert input_header.message
            assert output_header.message.name == input_header.message.name
            assert (
                output_header.message.greeting == f"Hello, {input_header.message.name}!"
            )


@pytest.mark.asyncio
async def test_process_batch(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_messages = [User(name="Cara"), User(name="Greg")]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test")
        await user_topic.send_batch(input_headers)

        await _entrypoint.process(batch_size=2)

        correlation_ids = set(
            input_header.correlation_id for input_header in input_headers
        )
        async with _entrypoint.reply_topic.receive_batch(
            "test", "test", batch_size=2
        ) as output_headers:
            for output_header in output_headers:
                if output_header.correlation_id in correlation_ids:
                    correlation_ids.remove(output_header.correlation_id)

                for input_header in input_headers:
                    if output_header.correlation_id == input_header.correlation_id:
                        assert not output_header.error
                        assert output_header.message
                        assert input_header.message
                        assert output_header.message.name == input_header.message.name
                        assert (
                            output_header.message.greeting
                            == f"Hello, {input_header.message.name}!"
                        )
        assert len(correlation_ids) == 0


@pytest.mark.asyncio
async def test_timeout(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg", delay=1)
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test")
        await user_topic.send(input_header)

        await _entrypoint.process(processing_timeout=0.1)

        async with _entrypoint.reply_topic.receive("test", "test") as output_header:
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Timed out after 0.1 sec"
            assert not output_header.message


@pytest.mark.asyncio
async def test_timeout_batch(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_messages = [User(name="Cara"), User(name="Greg", delay=1)]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test")
        await user_topic.send_batch(input_headers)

        await _entrypoint.process(batch_size=2, processing_timeout=0.1)

        correlation_ids = set(
            input_header.correlation_id for input_header in input_headers
        )
        async with _entrypoint.reply_topic.receive_batch(
            "test", "test", batch_size=2
        ) as output_headers:
            for output_header in output_headers:
                if output_header.correlation_id in correlation_ids:
                    correlation_ids.remove(output_header.correlation_id)
                    assert output_header.error == "Timed out after 0.1 sec"
                    assert not output_header.message
        assert len(correlation_ids) == 0


@pytest.mark.asyncio
async def test_exception(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send(input_header)

        await _invalid.process()

        async with _invalid.reply_topic.receive("test", "test") as output_header:
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Test"
            assert not output_header.message


@pytest.mark.asyncio
async def test_exception_batch(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_messages = [User(name="Cara"), User(name="Greg")]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send_batch(input_headers)

        await _invalid.process(batch_size=2)

        correlation_ids = [
            input_header.correlation_id for input_header in input_headers
        ]
        async with _invalid.reply_topic.receive_batch(
            "test", "test", batch_size=2
        ) as output_headers:
            for output_header in output_headers:
                if output_header.correlation_id in correlation_ids:
                    correlation_ids.remove(output_header.correlation_id)
                    assert output_header.error == "Test"
                    assert not output_header.message
        assert len(correlation_ids) == 0


@pytest.mark.asyncio
async def test_retries(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send(input_header)

        await _invalid.process(retries=1, sleep_time=0.1)

        async with _invalid.reply_topic.receive("test", "test") as output_header:
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Test"
            assert not output_header.message


@pytest.mark.asyncio
async def test_retries_batch(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_messages = [User(name="Cara"), User(name="Greg")]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send_batch(input_headers)

        await _invalid.process(batch_size=2, retries=1, sleep_time=0.1)

        correlation_ids = [
            input_header.correlation_id for input_header in input_headers
        ]
        async with _invalid.reply_topic.receive_batch(
            "test", "test", batch_size=2
        ) as output_headers:
            for output_header in output_headers:
                if output_header.correlation_id in correlation_ids:
                    correlation_ids.remove(output_header.correlation_id)
                    assert output_header.error == "Test"
                    assert not output_header.message
        assert len(correlation_ids) == 0


@pytest.mark.asyncio
async def test_resends(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send(input_header)

        for i in range(2):
            await _invalid.process(resends=1)

        async with _invalid.reply_topic.receive("test", "test") as output_header:
            assert output_header.correlation_id == input_header.correlation_id
            assert output_header.error == "Test"
            assert not output_header.message


@pytest.mark.asyncio
async def test_resends_batch(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_messages = [User(name="Cara"), User(name="Greg")]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send_batch(input_headers)

        for i in range(2):
            await _invalid.process(batch_size=2, resends=1)

        correlation_ids = [
            input_header.correlation_id for input_header in input_headers
        ]
        async with _invalid.reply_topic.receive_batch(
            "test", "test", batch_size=2
        ) as output_headers:
            for output_header in output_headers:
                if output_header.correlation_id in correlation_ids:
                    correlation_ids.remove(output_header.correlation_id)
                    assert output_header.error == "Test"
                    assert not output_header.message
        assert len(correlation_ids) == 0


@pytest.mark.asyncio
async def test_raises(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")
    input_header = user_topic.header_type(correlation_id=uuid4())
    input_header.message = input_message

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send(input_header)

        with pytest.raises(RuntimeError) as excinfo:
            await _invalid.process(raises=True)

        assert str(excinfo.value) == "Test"


@pytest.mark.asyncio
async def test_raises_batch(
    messaging: MessagingABC,
    user_topic: TopicABC[User],
    greeting_topic: TopicABC[Greeting],
    _invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_messages = [User(name="Cara"), User(name="Greg")]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)

    async with messaging:
        await user_topic.subscribe(_invalid.name)
        await greeting_topic.subscribe("test")
        await user_topic.send_batch(input_headers)

        with pytest.raises(RuntimeError) as excinfo:
            await _invalid.process(batch_size=2, raises=True)

        assert str(excinfo.value) == "Test"
