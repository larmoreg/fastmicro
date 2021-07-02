import logging
import pytest
from typing import Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MessagingABC
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import UserABC, GreetingABC

logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_entrypoint_call_performance(
    service: Service,
    messaging: MessagingABC,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    entrypoint: Entrypoint[UserABC, GreetingABC],
) -> None:
    from timeit import default_timer as timer

    name = service.name + "_" + entrypoint.callback.__name__
    await messaging.subscribe(user_topic.name, name)
    await messaging.subscribe(greeting_topic.name, name)

    input_messages = [user(name=f"Test{i}") for i in range(1000)]

    output_message = await entrypoint.call(
        input_messages[0],
        mock=True,
    )

    start = timer()
    output_messages = await entrypoint.call_batch(
        input_messages,
        mock=True,
        batch_size=100,
    )
    end = timer()

    diff = end - start
    logger.info(f"{diff}s elapsed")
    logger.info("{} messages / s".format(1000 / diff))

    assert len(output_messages) == len(input_messages)
    for input_message, output_message in zip(
        sorted(input_messages, key=lambda x: str(x.name)),
        sorted(output_messages, key=lambda x: str(x.name)),
    ):
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"
