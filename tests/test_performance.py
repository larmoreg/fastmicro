import asyncio
from datetime import date, datetime
import logging
from pydantic import BaseModel
import pytest
from timeit import default_timer as timer
from typing import Type
from uuid import UUID, uuid4

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.topic import Topic
from fastmicro.serializer import SerializerABC
from fastmicro.service import Service

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
        serializer_type.serialize(input_message) for input_message in input_messages
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
async def test_call_performance(service: Service) -> None:
    input_messages = [User(name=f"{i}") for i in range(1000)]
    test_messages = [
        Greeting(name=input_message.name, greeting=f"Hello, {input_message.name}!")
        for input_message in input_messages
    ]

    await service.greet(input_messages[0])

    start = timer()
    output_messages = await service.greet(
        input_messages,
        batch_size=100,
    )
    end = timer()

    diff = end - start
    logger.info(f"{diff}s elapsed")
    logger.info("{} messages / s".format(1000 / diff))

    assert len(output_messages) == len(test_messages)
    assert sorted(output_messages, key=lambda x: int(x.name)) == test_messages


@pytest.mark.asyncio
async def test_process_performance(
    messaging: MessagingABC,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    _entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_messages = [User(name=f"{i}") for i in range(1000)]
    input_headers = list()
    for input_message in input_messages:
        input_header = user_topic.header_type(correlation_id=uuid4())
        input_header.message = input_message
        input_headers.append(input_header)
    test_messages = [
        Greeting(name=input_message.name, greeting=f"Hello, {input_message.name}!")
        for input_message in input_messages
    ]

    async with messaging:
        await user_topic.subscribe(_entrypoint.name)
        await greeting_topic.subscribe("test", latest=True)
        await user_topic.send(input_headers)

        start = timer()
        await _entrypoint.process(batch_size=1000)
        end = timer()

        diff = end - start
        logger.info(f"{diff}s elapsed")
        logger.info("{} messages / s".format(1000 / diff))

        async with _entrypoint.reply_topic.receive(
            "test", "test", batch_size=1000
        ) as output_headers:
            assert len(output_headers) == len(input_headers)
            for input_header, test_message, output_header in zip(
                input_headers,
                test_messages,
                sorted(output_headers, key=lambda x: int(x.message.name)),  # type: ignore
            ):
                assert output_header.correlation_id == input_header.correlation_id
                assert not output_header.error
                assert output_header.message
                assert output_header.message == test_message
