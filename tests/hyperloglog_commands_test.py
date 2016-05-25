import pytest


pytestmark = pytest.redis_version(
    2, 8, 9, reason='HyperLogLog works only with redis>=2.8.9')


@pytest.mark.run_loop
def test_pfcount(redis):
    key = 'hll_pfcount'
    other_key = 'some-other-hll'

    # add initial data, cardinality changed so command returns 1
    is_changed = yield from redis.pfadd(key, 'foo', 'bar', 'zap')
    assert is_changed == 1

    # add more data, cardinality not changed so command returns 0
    is_changed = yield from redis.pfadd(key, 'zap', 'zap', 'zap')
    assert is_changed == 0

    # add event more data, cardinality not changed so command returns 0
    is_changed = yield from redis.pfadd(key, 'foo', 'bar')
    assert is_changed == 0

    # check cardinality of one key
    cardinality = yield from redis.pfcount(key)
    assert cardinality == 3

    # create new key (variable) for cardinality estimation
    is_changed = yield from redis.pfadd(other_key, 1, 2, 3)
    assert is_changed == 1

    # check cardinality of multiple keys
    cardinality = yield from redis.pfcount(key, other_key)
    assert cardinality == 6

    with pytest.raises(TypeError):
        yield from redis.pfcount(None)
    with pytest.raises(TypeError):
        yield from redis.pfcount(key, None)
    with pytest.raises(TypeError):
        yield from redis.pfcount(key, key, None)


@pytest.mark.run_loop
def test_pfadd(redis):
    key = 'hll_pfadd'
    values = ['a', 's', 'y', 'n', 'c', 'i', 'o']
    # add initial data, cardinality changed so command returns 1
    is_changed = yield from redis.pfadd(key, *values)
    assert is_changed == 1
    # add event more data, cardinality not changed so command returns 0
    is_changed = yield from redis.pfadd(key, 'i', 'o')
    assert is_changed == 0


@pytest.mark.run_loop
def test_pfadd_wrong_input(redis):
    with pytest.raises(TypeError):
        yield from redis.pfadd(None, 'value')


@pytest.mark.run_loop
def test_pfmerge(redis):
    key = 'hll_asyncio'
    key_other = 'hll_aioredis'

    key_dest = 'hll_aio'

    values = ['a', 's', 'y', 'n', 'c', 'i', 'o']
    values_other = ['a', 'i', 'o', 'r', 'e', 'd', 'i', 's']

    data_set = set(values + values_other)
    cardinality_merged = len(data_set)

    # add initial data, cardinality changed so command returns 1
    yield from redis.pfadd(key, *values)
    yield from redis.pfadd(key_other, *values_other)

    # check cardinality of one key
    cardinality = yield from redis.pfcount(key)
    assert cardinality == len(set(values_other))

    cardinality_other = yield from redis.pfcount(key_other)
    assert cardinality_other == len(set(values_other))

    yield from redis.pfmerge(key_dest, key, key_other)
    cardinality_dest = yield from redis.pfcount(key_dest)
    assert cardinality_dest == cardinality_merged

    with pytest.raises(TypeError):
        yield from redis.pfmerge(None, key)
    with pytest.raises(TypeError):
        yield from redis.pfmerge(key_dest, None)
    with pytest.raises(TypeError):
        yield from redis.pfmerge(key_dest, key, None)


@pytest.mark.run_loop
def test_pfmerge_wrong_input(redis):
    with pytest.raises(TypeError):
        yield from redis.pfmerge(None, 'value')
