#!/usr/bin/env python3

import asyncio
from pydantic import BaseModel

from fastmicro.messaging.memory import Messaging, Topic
from fastmicro.service import Service


class User(BaseModel):
    name: str


class Greeting(BaseModel):
    name: str
    greeting: str


messaging: Messaging = Messaging()
service = Service("test", messaging)
user_topic = Topic("user", messaging, User)
greeting_topic = Topic("greeting", messaging, Greeting)


@service.entrypoint(user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    return greeting


async def main() -> None:
    await service.start()

    user = User(name="Greg")
    print(user)
    greeting = await greet.call(user)
    print(greeting)

    await service.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
