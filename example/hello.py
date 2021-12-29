#!/usr/bin/env python3

import asyncio
from pydantic import BaseModel

from fastmicro.messaging.memory import Messaging
from fastmicro.service import Service


class User(BaseModel):
    name: str


class Greeting(BaseModel):
    name: str
    greeting: str


service = Service("test")
loop = asyncio.get_event_loop()
messaging = Messaging(loop=loop)
user_topic = messaging.topic("user", User)
greeting_topic = messaging.topic("greeting", Greeting)


@service.entrypoint(user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    return greeting


async def main() -> None:
    await service.start()

    async with messaging:
        user = User(name="Greg")
        print(user)
        greeting = await service.greet(user)
        print(greeting)

    await service.stop()


if __name__ == "__main__":
    loop.run_until_complete(main())
