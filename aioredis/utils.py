from typing import TYPE_CHECKING, TypeVar, overload

if TYPE_CHECKING:
    from aioredis import Redis
    from aioredis.client import Pipeline


try:
    import hiredis  # noqa

    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False


_T = TypeVar("_T")


def from_url(url, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from aioredis.client import Redis

    return Redis.from_url(url, **kwargs)


class pipeline:
    def __init__(self, redis_obj: "Redis"):
        self.p: "Pipeline" = redis_obj.pipeline()

    async def __aenter__(self) -> "Pipeline":
        return self.p

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.p.execute()
        del self.p


# Mypy bug: https://github.com/python/mypy/issues/11005
@overload
def str_if_bytes(value: bytes) -> str:  # type: ignore[misc]
    ...


@overload
def str_if_bytes(value: _T) -> _T:
    ...


def str_if_bytes(value: object) -> object:
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def safe_str(value: object) -> str:
    return str(str_if_bytes(value))
