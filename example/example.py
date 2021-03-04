#!/usr/bin/env python3

import asyncio
import logging

import pydantic

from fastmicro.messaging import PulsarMessaging
from fastmicro.service import Service
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class User(pydantic.BaseModel):
    name: str
    delay: int = 0


class Greeting(pydantic.BaseModel):
    name: str
    greeting: str


service = Service("test")
messaging = PulsarMessaging()
user_topic = Topic(messaging, "user", User)
greeting_topic = Topic(messaging, "greeting", Greeting)


@service.entrypoint(user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    logger.error(user)
    await asyncio.sleep(user.delay)
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    logger.error(greeting)
    return greeting


if __name__ == "__main__":
    # FIXME: this can't be killed
    service.run()
