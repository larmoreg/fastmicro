import asyncio
import pydantic
import pytest

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
    return MemoryMessaging()


@pytest.fixture
def service(messaging: Messaging) -> Service:
    return Service("test", messaging)


@pytest.fixture
def user_topic() -> Topic[User]:
    return Topic("user", User)


@pytest.fixture
def greeting_topic() -> Topic[Greeting]:
    return Topic("greeting", Greeting)


@pytest.fixture
def greet(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, reply_topic=greeting_topic, mock=True)
    async def _greet(user: User) -> Greeting:
        await asyncio.sleep(user.delay)
        return Greeting(name=user.name, greeting=f"Hello, {user.name}!")

    return _greet


@pytest.fixture
def fail(
    service: Service, user_topic: Topic[User], greeting_topic: Topic[Greeting]
) -> Entrypoint[User, Greeting]:
    @service.entrypoint(user_topic, reply_topic=greeting_topic, mock=True)
    async def _greet(user: User) -> Greeting:
        raise RuntimeError("Test error")

    return _greet
