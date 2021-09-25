#!/usr/bin/env python3

from pydantic import BaseModel

from fastmicro.messaging.redis import Messaging, Topic
from fastmicro.service import Service


class User(BaseModel):
    name: str


class Greeting(BaseModel):
    name: str
    greeting: str


messaging: Messaging = Messaging()
service = Service("test", messaging)
user_topic = Topic[User]("user", messaging)
greeting_topic = Topic[Greeting]("greeting", messaging)


@service.entrypoint(user_topic, greeting_topic)
async def greet(user: User) -> Greeting:
    print(user)
    greeting = Greeting(name=user.name, greeting=f"Hello, {user.name}!")
    print(greeting)
    return greeting


if __name__ == "__main__":
    service.run()
