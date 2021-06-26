import logging
import pytest

from fastmicro.entrypoint import Entrypoint
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import User, Greeting

logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_entrypoint_call_performance(
    service: Service,
    user_topic: Topic[User],
    greeting_topic: Topic[Greeting],
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    from timeit import default_timer as timer

    input_messages = [User(name=f"Test{i}") for i in range(1000)]

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
