import pytest

from aioredis.util import coerced_keys_dict


def test_simple():
    d = coerced_keys_dict()
    assert d == {}

    d = coerced_keys_dict({b'a': 'b', b'c': 'd'})
    assert 'a' in d
    assert b'a' in d
    assert 'c' in d
    assert b'c' in d
    assert d == {b'a': 'b', b'c': 'd'}


def test_invalid_init():
    d = coerced_keys_dict({'foo': 'bar'})
    assert d == {'foo': 'bar'}

    assert 'foo' not in d
    assert b'foo' not in d
    with pytest.raises(KeyError):
        d['foo']
    with pytest.raises(KeyError):
        d[b'foo']

    d = coerced_keys_dict()
    d.update({'foo': 'bar'})
    assert d == {'foo': 'bar'}

    assert 'foo' not in d
    assert b'foo' not in d
    with pytest.raises(KeyError):
        d['foo']
    with pytest.raises(KeyError):
        d[b'foo']


def test_valid_init():
    d = coerced_keys_dict({b'foo': 'bar'})
    assert d == {b'foo': 'bar'}
    assert 'foo' in d
    assert b'foo' in d
    assert d['foo'] == 'bar'
    assert d[b'foo'] == 'bar'

    d = coerced_keys_dict()
    d.update({b'foo': 'bar'})
    assert d == {b'foo': 'bar'}
    assert 'foo' in d
    assert b'foo' in d
    assert d['foo'] == 'bar'
    assert d[b'foo'] == 'bar'
