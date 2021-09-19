import asyncio
import logging
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


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_exception(
    invalid: Entrypoint[User, Greeting],
) -> None:
    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        await invalid.call(input_message)

    assert str(excinfo.value) == "Test"


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_timeout(
    entrypoint: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_message = User(name="Greg")

    with pytest.raises(asyncio.TimeoutError):
        with caplog.at_level(logging.ERROR, logger="fastmicro.entrypoint"):
            await entrypoint.call(input_message, processing_timeout=1)

    assert len(caplog.records) == 1
    assert caplog.records[0].message == "Processing failed; skipping"


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_exception_batch(
    invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_messages = [User(name="Cara"), User(name="Greg")]

    with pytest.raises(RuntimeError) as excinfo:
        with caplog.at_level(logging.ERROR, logger="fastmicro.entrypoint"):
            await invalid.call_batch(input_messages, batch_size=2)

    assert str(excinfo.value) == "Test"

    assert len(caplog.records) == 1
    assert caplog.records[0].message == "Processing failed; skipping"


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_retry(
    invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        with caplog.at_level(logging.ERROR, logger="fastmicro.entrypoint"):
            await invalid.call(input_message, retries=1, sleep_time=1)

    assert str(excinfo.value) == "Test"

    assert len(caplog.records) == 2
    assert caplog.records[0].message == "Processing failed; retry 1 / 1"
    assert caplog.records[1].message == "Processing failed; skipping"


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_retry_batch(
    invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_messages = [User(name="Cara"), User(name="Greg")]

    with pytest.raises(RuntimeError) as excinfo:
        with caplog.at_level(logging.ERROR, logger="fastmicro.entrypoint"):
            await invalid.call_batch(
                input_messages, batch_size=2, retries=1, sleep_time=1
            )

    assert str(excinfo.value) == "Test"

    assert len(caplog.records) == 2
    assert caplog.records[0].message == "Processing failed; retry 1 / 1"
    assert caplog.records[1].message == "Processing failed; skipping"


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_resend(
    invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_message = User(name="Greg")

    with pytest.raises(RuntimeError) as excinfo:
        with caplog.at_level(logging.ERROR, logger="fastmicro.entrypoint"):
            await invalid.call(input_message, resends=1)

    assert str(excinfo.value) == "Test"

    assert len(caplog.records) == 2
    assert caplog.records[0].message == "Processing failed; resend 1 / 1"
    assert caplog.records[1].message == "Processing failed; skipping"


@pytest.mark.skip
@pytest.mark.asyncio
async def test_entrypoint_resend_batch(
    invalid: Entrypoint[User, Greeting],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_messages = [User(name="Cara"), User(name="Greg")]

    with pytest.raises(RuntimeError) as excinfo:
        with caplog.at_level(logging.ERROR, logger="fastmicro.entrypoint"):
            await invalid.call_batch(input_messages, batch_size=2, resends=1)

    assert str(excinfo.value) == "Test"

    assert len(caplog.records) == 2
    assert caplog.records[0].message == "Processing failed; resend 1 / 1"
    assert caplog.records[1].message == "Processing failed; skipping"
