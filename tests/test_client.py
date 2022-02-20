import asyncio
import logging
import pytest

from fastmicro.service import Service

from .conftest import User, Greeting


@pytest.mark.asyncio
async def test_call(service: Service) -> None:
    input_message = User(name="Greg")
    test_message = Greeting(
        name=input_message.name, greeting=f"Hello, {input_message.name}!"
    )

    output_message = await service.greet(input_message)
    assert output_message == test_message


@pytest.mark.asyncio
async def test_timeout(service: Service, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.service")

    input_message = User(name="Greg", delay=1)

    with pytest.raises(asyncio.TimeoutError):
        await service.greet(input_message, processing_timeout=0.1)


@pytest.mark.asyncio
async def test_exception(
    error_service: Service, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.CRITICAL, logger="fastmicro.entrypoint")

    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await error_service.greet(input_message)

    assert str(excinfo.value) == "Test"
