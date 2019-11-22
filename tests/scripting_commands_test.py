import pytest
import asyncio

from aioredis import ReplyError


async def test_eval(redis):
    await redis.delete('key:eval', 'value:eval')

    script = "return 42"
    res = await redis.eval(script)
    assert res == 42

    key, value = b'key:eval', b'value:eval'
    script = """
    if redis.call('setnx', KEYS[1], ARGV[1]) == 1
    then
        return 'foo'
    else
        return 'bar'
    end
    """
    res = await redis.eval(script, keys=[key], args=[value])
    assert res == b'foo'
    res = await redis.eval(script, keys=[key], args=[value])
    assert res == b'bar'

    script = "return 42"
    with pytest.raises(TypeError):
        await redis.eval(script, keys='not:list')

    with pytest.raises(TypeError):
        await redis.eval(script, keys=['valid', None])
    with pytest.raises(TypeError):
        await redis.eval(script, args=['valid', None])
    with pytest.raises(TypeError):
        await redis.eval(None)


async def test_evalsha(redis):
    script = b"return 42"
    sha_hash = await redis.script_load(script)
    assert len(sha_hash) == 40
    res = await redis.evalsha(sha_hash)
    assert res == 42

    key, arg1, arg2 = b'key:evalsha', b'1', b'2'
    script = "return {KEYS[1], ARGV[1], ARGV[2]}"
    sha_hash = await redis.script_load(script)
    res = await redis.evalsha(sha_hash, [key], [arg1, arg2])
    assert res == [key, arg1, arg2]

    with pytest.raises(ReplyError):
        await redis.evalsha(b'wrong sha hash')
    with pytest.raises(TypeError):
        await redis.evalsha(sha_hash, keys=['valid', None])
    with pytest.raises(TypeError):
        await redis.evalsha(sha_hash, args=['valid', None])
    with pytest.raises(TypeError):
        await redis.evalsha(None)


async def test_script_exists(redis):
    sha_hash1 = await redis.script_load(b'return 1')
    sha_hash2 = await redis.script_load(b'return 2')
    assert len(sha_hash1) == 40
    assert len(sha_hash2) == 40

    res = await redis.script_exists(sha_hash1, sha_hash1)
    assert res == [1, 1]

    no_sha = b'ffffffffffffffffffffffffffffffffffffffff'
    res = await redis.script_exists(no_sha)
    assert res == [0]

    with pytest.raises(TypeError):
        await redis.script_exists(None)
    with pytest.raises(TypeError):
        await redis.script_exists('123', None)


async def test_script_flush(redis):
    sha_hash1 = await redis.script_load(b'return 1')
    assert len(sha_hash1) == 40
    res = await redis.script_exists(sha_hash1)
    assert res == [1]
    res = await redis.script_flush()
    assert res is True
    res = await redis.script_exists(sha_hash1)
    assert res == [0]


async def test_script_load(redis):
    sha_hash1 = await redis.script_load(b'return 1')
    sha_hash2 = await redis.script_load(b'return 2')
    assert len(sha_hash1) == 40
    assert len(sha_hash2) == 40
    res = await redis.script_exists(sha_hash1, sha_hash1)
    assert res == [1, 1]


async def test_script_kill(create_redis, server, redis):
    script = "while (1) do redis.call('TIME') end"

    other_redis = await create_redis(server.tcp_address)

    ok = await redis.set('key1', 'value')
    assert ok is True

    fut = other_redis.eval(script, keys=['non-existent-key'], args=[10])
    await asyncio.sleep(0.1)
    resp = await redis.script_kill()
    assert resp is True

    with pytest.raises(ReplyError):
        await fut

    with pytest.raises(ReplyError):
        await redis.script_kill()
