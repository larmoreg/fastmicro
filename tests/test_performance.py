import asyncio
from datetime import date, datetime
import logging
from pydantic import BaseModel
import pytest
from timeit import default_timer as timer
from typing import Type
from uuid import UUID, uuid4

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC, TopicABC
from fastmicro.serializer import SerializerABC

from .conftest import User, Greeting

logger = logging.getLogger(__name__)


class Dummy(BaseModel):
    a: UUID
    b: date
    c: datetime
    d: bytes


@pytest.mark.asyncio
async def test_serializer_performance(serializer_type: Type[SerializerABC]) -> None:
    input_messages = [
        Dummy(
            a=uuid4(),
            b=date.today(),
            c=datetime.utcnow(),
            d=f"{i}".encode(),
        )
        for i in range(1000)
    ]

    serialize_tasks = [
        serializer_type.serialize(input_message.dict())
        for input_message in input_messages
    ]
    start = timer()
    serialized_messages = await asyncio.gather(*serialize_tasks)
    end = timer()
    diff1 = end - start

    deserialize_tasks = [
        serializer_type.deserialize(serialized_message)
        for serialized_message in serialized_messages
    ]
    start = timer()
    deserialized_messages = await asyncio.gather(*deserialize_tasks)
    end = timer()
    diff2 = end - start

    output_messages = [
        Dummy(**deserialized_message) for deserialized_message in deserialized_messages
    ]

    diff = diff1 + diff2
    logger.info(f"{diff}s elapsed")
    logger.info("{} messages / s".format(1000 / diff))

    assert input_messages == output_messages


@pytest.mark.asyncio
async def test_call_performance(entrypoint: Entrypoint[User, Greeting]) -> None:
    input_messages = [User(name=f"{i}") for i in range(1000)]

    output_message = await entrypoint.call(
        input_messages[0],
    )

    start = timer()
    output_messages = await entrypoint.call_batch(
        input_messages,
        batch_size=100,
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


@pytest.mark.asyncio
async def test_process_performance(
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

        start = timer()
        await _entrypoint.process(batch_size=2)
        end = timer()

        diff = end - start
        logger.info(f"{diff}s elapsed")
        logger.info("{} messages / s".format(1000 / diff))

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

    pass
