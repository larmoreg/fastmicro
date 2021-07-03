import pytest
from typing import Type

from fastmicro.entrypoint import Entrypoint
from fastmicro.service import Service
from fastmicro.topic import Topic

from .conftest import UserABC, GreetingABC


@pytest.mark.asyncio()
async def test_entrypoint_call(
    service: Service,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    entrypoint: Entrypoint[UserABC, GreetingABC],
) -> None:
    input_message = user(name="Greg")

    output_message = await entrypoint.call(input_message, mock=True)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio()
async def test_entrypoint_call_batch(
    service: Service,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    entrypoint: Entrypoint[UserABC, GreetingABC],
) -> None:
    input_messages = [user(name="Greg", delay=2), user(name="Cara", delay=1)]

    output_messages = await entrypoint.call_batch(input_messages, mock=True, batch_size=2)

    assert len(output_messages) == len(input_messages)
    for input_message, output_message in zip(
        sorted(input_messages, key=lambda x: str(x.name)),
        sorted(output_messages, key=lambda x: str(x.name)),
    ):
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"


"""
@pytest.mark.asyncio()
async def test_entrypoint_exception(
    service: Service,
    user: Type[UserABC],
    greeting: Type[GreetingABC],
    user_topic: Topic[UserABC],
    greeting_topic: Topic[GreetingABC],
    invalid: Entrypoint[UserABC, GreetingABC],
) -> None:
    input_message = user(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.call(input_message, mock=True)

    assert str(excinfo.value) == "Test"
"""
