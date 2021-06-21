import asyncio
import logging
import pydantic
import pytest
from typing import AsyncGenerator

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging
from fastmicro.messaging.memory import MemoryMessaging
from fastmicro.messaging.redis import RedisMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic
from fastmicro.types import HT

logger = logging.getLogger(__name__)


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


@pytest.fixture(params=[MemoryMessaging, RedisMessaging])
async def messaging(request, event_loop: asyncio.AbstractEventLoop) -> AsyncGenerator[Messaging[HT], None]:  # type: ignore
    _messaging: Messaging[HT] = request.param(loop=event_loop)
    await _messaging.connect()
    yield _messaging
    await _messaging.cleanup()


@pytest.fixture
async def service(messaging: Messaging[HT], event_loop: asyncio.AbstractEventLoop) -> Service:
    return Service(messaging, "test", loop=event_loop)


@pytest.fixture
def user_topic() -> Topic[User]:
    return Topic("user", User)


@pytest.fixture
def greeting_topic() -> Topic[Greeting]:
    return Topic("greeting", Greeting)


@pytest.fixture
def entrypoint(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(user: User) -> Greeting:
        logger.debug(f"Sleeping for {user.delay}s")
        await asyncio.sleep(user.delay)
        logger.debug(f"Waking up after {user.delay}s")
        return Greeting(name=user.name, greeting=f"Hello, {user.name}!")

    return greet


@pytest.fixture
def invalid(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(user: User) -> Greeting:
        raise RuntimeError("Test")

    return greet
