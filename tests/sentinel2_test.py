import pytest


@pytest.mark.run_loop
def test_sentinel_simple(start_sentinel, start_server, create_redis, loop):
    sentinel = start_sentinel('main', start_server('masterA'))
    redis = yield from create_redis(sentinel.tcp_address, loop=loop)
    info = yield from redis.role()
    assert info.role == 'sentinel'
