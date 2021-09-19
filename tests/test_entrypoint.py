import asyncio
import pytest

from fastmicro.entrypoint import Entrypoint

from .conftest import User, Greeting


@pytest.mark.asyncio
async def test_entrypoint_await(_entrypoint: Entrypoint[User, Greeting]) -> None:
    input_message = User(name="Greg")

    output_message = await _entrypoint(input_message)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio
async def test_entrypoint_call(
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    output_message = await entrypoint.call(input_message)

    assert output_message.name == "Greg"
    assert output_message.greeting == "Hello, Greg!"


@pytest.mark.asyncio
async def test_entrypoint_call_batch(
    entrypoint: Entrypoint[User, Greeting],
) -> None:
    input_messages = [User(name="Cara"), User(name="Greg")]

    output_messages = await entrypoint.call_batch(input_messages, batch_size=2)

    assert len(output_messages) == len(input_messages)
    for input_message, output_message in zip(
        input_messages, sorted(output_messages, key=lambda x: x.name)
    ):
        assert output_message.name == input_message.name
        assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio
async def test_entrypoint_exception(
    invalid: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.call(input_message)

    assert str(excinfo.value) == "Test"


@pytest.mark.asyncio
async def test_entrypoint_exception_batch(
    invalid: Entrypoint[User, Greeting],
) -> None:
    input_messages = [User(name="Cara"), User(name="Greg")]

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.call_batch(input_messages, batch_size=2)

    assert str(excinfo.value) == "Test"
