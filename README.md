# aioredis

asyncio (3156) Redis client library.

The library is intended to provide simple and clear interface to Redis
based on asyncio.

## Features

| Feature                      | Supported             |
|:-----------------------------|:----------------------|
| hiredis parser               | :white_check_mark:    |
| Pure-python parser           | :white_check_mark:    |
| Low-level & High-level APIs  | :white_check_mark:    |
| Pipelining support           | :white_check_mark:    |
| Multi/Exec support           | :white_check_mark:    |
| Connections Pool             | :white_check_mark:    |
| Pub/Sub support              | :white_check_mark:    |
| Sentinel support             | :white_check_mark:    |
| ACL support                  | :white_check_mark:    |
| Streams support              | :white_check_mark:    |
| Redis Cluster support        | :no_entry_sign:       |
| Tested Python versions       | 3.6, 3.7, 3.8, 3.9    |
| Tested for Redis servers     | 5.0, 6.0              |
| Support for dev Redis server | through low-level API |


## Installation

The easiest way to install aioredis is by using the package on PyPi:

    pip install aioredis

## Requirements

-   Python 3.6+
-   hiredis

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
