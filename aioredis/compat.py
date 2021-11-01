import asyncio
import sys

if sys.version_info >= (3, 7):

    def get_event_loop():
        return asyncio.get_running_loop()

    def create_task_or_run(coro):
        try:
            asyncio.create_task(coro)
        except RuntimeError:
            asyncio.run(coro)


else:

    def get_event_loop():
        return asyncio.get_event_loop()

    def create_task_or_run(coro):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(coro)
        else:
            loop.run_until_complete(coro)


if sys.version_info >= (3, 8):
    from typing import Protocol, TypedDict
else:
    from typing_extensions import Protocol, TypedDict

__all__ = ("Protocol", "TypedDict")
