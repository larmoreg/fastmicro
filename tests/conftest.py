import asyncio
import pydantic
import pytest
from typing import AsyncGenerator

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import Messaging, RedisMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


@pytest.fixture
async def service(
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Service, None]:
    service = Service(RedisMessaging, "test", loop=event_loop)
    yield service


@pytest.fixture
async def messaging(
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Messaging, None]:
    messaging = RedisMessaging(loop=event_loop)
    await messaging.connect()
    yield messaging
    await messaging.cleanup()


@pytest.fixture
def user_topic() -> Topic[User]:
    return Topic("user", User)


@pytest.fixture
def greeting_topic() -> Topic[Greeting]:
    return Topic("greeting", Greeting)


@pytest.fixture
async def greet(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> AsyncGenerator[Entrypoint[User, Greeting], None]:
    @service.entrypoint(user_topic, greeting_topic)
    async def _greet(user: User) -> Greeting:
        await asyncio.sleep(user.delay)
        return Greeting(name=user.name, greeting=f"Hello, {user.name}!")

    await _greet.run()
    yield _greet
    await _greet.stop()
