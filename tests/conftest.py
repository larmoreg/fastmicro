import asyncio
import pydantic
import pytest
from typing import AsyncGenerator

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging, MemoryMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


@pytest.fixture
async def messaging(event_loop: asyncio.AbstractEventLoop,) -> AsyncGenerator[Messaging, None]:
    _messaging = MemoryMessaging(loop=event_loop)
    await _messaging.connect()
    yield _messaging
    await _messaging.cleanup()


@pytest.fixture
async def service(messaging: Messaging, event_loop: asyncio.AbstractEventLoop) -> Service:
    return Service(messaging, "test", loop=event_loop)


@pytest.fixture
def user_topic() -> Topic[User]:
    return Topic("user", User)


@pytest.fixture
def greeting_topic() -> Topic[Greeting]:
    return Topic("greeting", Greeting)


def get_entrypoint(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(user: User) -> Greeting:
        await asyncio.sleep(user.delay)
        return Greeting(name=user.name, greeting=f"Hello, {user.name}!")

    return greet


def get_invalid(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic)
    async def greet(user: User) -> Greeting:
        raise RuntimeError("Test")

    return greet
