import asyncio
import logging
import pytest

from fastmicro.entrypoint import Entrypoint

from .conftest import User, Greeting


@pytest.mark.asyncio
async def test_await(_entrypoint: Entrypoint[User, Greeting]) -> None:
    input_message = User(name="Greg")

    output_message = await _entrypoint(input_message)

    assert output_message.name == input_message.name
    assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio
async def test_call(entrypoint: Entrypoint[User, Greeting]) -> None:
    input_message = User(name="Greg")

    output_message = await entrypoint.call(input_message)

    assert output_message.name == input_message.name
    assert output_message.greeting == f"Hello, {input_message.name}!"


@pytest.mark.asyncio
async def test_timeout(
    entrypoint: Entrypoint[User, Greeting], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg", delay=1)

    with pytest.raises(asyncio.TimeoutError):
        await entrypoint.call(input_message, timeout=0.1)


@pytest.mark.asyncio
async def test_exception(
    invalid: Entrypoint[User, Greeting], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.call(input_message)

    assert str(excinfo.value) == "Test"
