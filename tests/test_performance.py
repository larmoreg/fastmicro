import asyncio
import logging
import pytest
from timeit import default_timer as timer
from uuid import uuid4

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC

from .conftest import User, Greeting

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_serializer_performance(serializer_type) -> None:
    input_messages = [User(name=f"{i}") for i in range(1000)]

    tasks = [
        serializer_type.serialize(input_message.dict())
        for input_message in input_messages
    ]
    start = timer()
    temp_messages = await asyncio.gather(*tasks)
    end = timer()
    diff1 = end - start

    tasks = [
        serializer_type.deserialize(temp_message) for temp_message in temp_messages
    ]
    start = timer()
    output_messages = await asyncio.gather(*tasks)
    end = timer()
    diff2 = end - start

    diff = diff1 + diff2
    logger.info(f"{diff}s elapsed")
    logger.info("{} messages / s".format(1000 / diff))

    assert input_messages == output_messages


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_process_performance(
    messaging: MessagingABC,
    _entrypoint: Entrypoint[User, Greeting],
) -> None:
    await messaging.connect()

    await messaging.subscribe(_entrypoint.topic.name, _entrypoint.name)
    await messaging.subscribe(_entrypoint.reply_topic.name, _entrypoint.broadcast_name)

    input_headers = [messaging.header_type(correlation_id=uuid4()) for i in range(1000)]
    input_messages = [User(name=f"{i}") for i in range(1000)]
    await messaging.send_batch(_entrypoint.topic, input_headers, input_messages)

    start = timer()
    await _entrypoint.process(batch_size=1000)
    end = timer()

    diff = end - start
    logger.info(f"{diff}s elapsed")
    logger.info("{} messages / s".format(1000 / diff))

    async with messaging.receive_batch(
        _entrypoint.reply_topic,
        _entrypoint.broadcast_name,
        _entrypoint.consumer_name,
        batch_size=1000,
    ) as (output_headers, output_messages):
        assert len(output_messages) == len(input_messages)
        for input_message, output_message in zip(
            input_messages, sorted(output_messages, key=lambda x: int(x.name))
        ):
            assert output_message.name == input_message.name
            assert output_message.greeting == f"Hello, {input_message.name}!"

    await messaging.cleanup()


@pytest.mark.asyncio
async def test_entrypoint_call_performance(
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_messages = [User(name=f"{i}") for i in range(1000)]

    output_message = await entrypoint.call(
        input_messages[0],
    )

    start = timer()
    output_messages = await entrypoint.call_batch(
        input_messages,
        batch_size=1000,
    )
    end = timer()

    diff = end - start
    logger.info(f"{diff}s elapsed")
    logger.info("{} messages / s".format(1000 / diff))

    assert len(output_messages) == len(input_messages)
    for input_message, output_message in zip(
        input_messages, sorted(output_messages, key=lambda x: int(x.name))
    ):
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"
