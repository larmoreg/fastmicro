import asyncio
import pydantic
import pytest
from typing import AsyncGenerator

from fastmicro.entrypoint import Entrypoint
from fastmicro.messaging import MemoryMessaging, Messaging
from fastmicro.service import Service
from fastmicro.topic import Topic


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


@pytest.fixture
def messaging(event_loop: asyncio.AbstractEventLoop) -> Messaging:
    return MemoryMessaging(loop=event_loop)


@pytest.fixture
async def service(
    event_loop: asyncio.AbstractEventLoop, messaging: Messaging
) -> AsyncGenerator[Service, None]:
    service = Service(messaging, "test", loop=event_loop)
    yield service
    await service.stop()


@pytest.fixture
def user_topic(messaging: Messaging) -> Topic[User]:
    return Topic(messaging, "user", User)


@pytest.fixture
def greeting_topic(messaging: Messaging) -> Topic[Greeting]:
    return Topic(messaging, "greeting", Greeting)


@pytest.fixture
def greet(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, greeting_topic, mock=True)
    async def _greet(user: User) -> Greeting:
        await asyncio.sleep(user.delay)
        return Greeting(name=user.name, greeting=f"Hello, {user.name}!")

    return _greet
