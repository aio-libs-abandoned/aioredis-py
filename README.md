# aioredis

---

## 📢🚨 Aioredis is now in redis-py 4.2.0rc1+ 🚨🚨

Aioredis is now in redis-py 4.2.0rc1+

To install, just do `pip install redis>=4.2.0rc1`. The code is almost the exact same. You will just need to import like so:

```python
from redis import asyncio as aioredis
```

This way you don't have to change all your code, just the imports.

https://github.com/redis/redis-py/releases/tag/v4.2.0rc1

Now that aioredis is under Redis officially, I hope there will never be an unmaintained, asyncio Redis lib in the Python ecosystem again. I will be helping out maintenance at Redis-py for the foreseeable future just to get some of the asyncio stuff out of the way. There are also some bugs that didn't make it into the [PR](https://github.com/redis/redis-py/pull/1899) that I'll be slowly migrating over throughout the next few weeks -- so long as my exams don't kill me beforehand :)

Thank you all so much for your commitment to this repository! Thank you so much to @abrookins @seandstewart @bmerry for all the commits and maintenance. And thank you to everyone here who has been adopting the new code base and squashing bugs. It's been an honor!

Cheers,
Andrew

---

asyncio (3156) Redis client library.

The library is intended to provide simple and clear interface to Redis
based on asyncio.

## Features

| Feature                      | Supported                |
|:-----------------------------|:-------------------------|
| hiredis parser               | :white_check_mark:       |
| Pure-python parser           | :white_check_mark:       |
| Low-level & High-level APIs  | :white_check_mark:       |
| Pipelining support           | :white_check_mark:       |
| Multi/Exec support           | :white_check_mark:       |
| Connections Pool             | :white_check_mark:       |
| Pub/Sub support              | :white_check_mark:       |
| Sentinel support             | :white_check_mark:       |
| ACL support                  | :white_check_mark:       |
| Streams support              | :white_check_mark:       |
| Redis Cluster support        | :no_entry_sign:          |
| Tested Python versions       | 3.6, 3.7, 3.8, 3.9, 3.10 |
| Tested for Redis servers     | 5.0, 6.0                 |
| Support for dev Redis server | through low-level API    |


## Installation

The easiest way to install aioredis is by using the package on PyPi:

    pip install aioredis

Recommended with hiredis for performance and stability reasons:

    pip install hiredis

## Requirements

-   Python 3.6+
-   hiredis (Optional but recommended)
-   async-timeout
-   typing-extensions

## Benchmarks

Benchmarks can be found here:
<https://github.com/popravich/python-redis-benchmark>

## Contribute

-   Issue Tracker: <https://github.com/aio-libs/aioredis/issues>
-   Google Group: <https://groups.google.com/g/aio-libs>
-   Gitter: <https://gitter.im/aio-libs/Lobby>
-   Source Code: <https://github.com/aio-libs/aioredis>
-   Contributor's guide: [devel](docs/devel.md)

Feel free to file an issue or make pull request if you find any bugs or
have some suggestions for library improvement.

## License

The aioredis is offered under a [MIT License](LICENSE).
