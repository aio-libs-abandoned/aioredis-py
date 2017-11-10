import pytest

from aioredis.util import parse_url


@pytest.mark.parametrize('url,expected_address,expected_options', [
    # redis scheme
    ('redis://', ('localhost', 6379), {}),
    ('redis://localhost:6379', ('localhost', 6379), {}),
    ('redis://localhost:6379/', ('localhost', 6379), {}),
    ('redis://localhost:6379/0', ('localhost', 6379), {'db': 0}),
    ('redis://localhost:6379/1', ('localhost', 6379), {'db': 1}),
    ('redis://localhost:6379?db=1', ('localhost', 6379), {'db': 1}),
    ('redis://localhost:6379/?db=1', ('localhost', 6379), {'db': 1}),
    ('redis://redis-host', ('redis-host', 6379), {}),
    ('redis://redis-host', ('redis-host', 6379), {}),
    ('redis://host:1234', ('host', 1234), {}),
    ('redis://user@localhost', ('localhost', 6379), {}),
    ('redis://:secret@localhost', ('localhost', 6379), {'password': 'secret'}),
    ('redis://user:secret@localhost',
     ('localhost', 6379), {'password': 'secret'}),
    ('redis://localhost?password=secret',
     ('localhost', 6379), {'password': 'secret'}),
    ('redis://localhost?encoding=utf-8',
     ('localhost', 6379), {'encoding': 'utf-8'}),
    ('redis://localhost?ssl=true',
     ('localhost', 6379), {'ssl': True}),
    ('redis://localhost?timeout=1.0',
     ('localhost', 6379), {'timeout': 1.0}),
    ('redis://localhost?timeout=10',
     ('localhost', 6379), {'timeout': 10.0}),
    # rediss scheme
    ('rediss://', ('localhost', 6379), {'ssl': True}),
    ('rediss://localhost:6379', ('localhost', 6379), {'ssl': True}),
    ('rediss://localhost:6379/', ('localhost', 6379), {'ssl': True}),
    ('rediss://localhost:6379/0', ('localhost', 6379), {'ssl': True, 'db': 0}),
    ('rediss://localhost:6379/1', ('localhost', 6379), {'ssl': True, 'db': 1}),
    ('rediss://localhost:6379?db=1',
     ('localhost', 6379), {'ssl': True, 'db': 1}),
    ('rediss://localhost:6379/?db=1',
     ('localhost', 6379), {'ssl': True, 'db': 1}),
    ('rediss://redis-host', ('redis-host', 6379), {'ssl': True}),
    ('rediss://redis-host', ('redis-host', 6379), {'ssl': True}),
    ('rediss://host:1234', ('host', 1234), {'ssl': True}),
    ('rediss://user@localhost', ('localhost', 6379), {'ssl': True}),
    ('rediss://:secret@localhost',
     ('localhost', 6379), {'ssl': True, 'password': 'secret'}),
    ('rediss://user:secret@localhost',
     ('localhost', 6379), {'ssl': True, 'password': 'secret'}),
    ('rediss://localhost?password=secret',
     ('localhost', 6379), {'ssl': True, 'password': 'secret'}),
    ('rediss://localhost?encoding=utf-8',
     ('localhost', 6379), {'ssl': True, 'encoding': 'utf-8'}),
    ('rediss://localhost?timeout=1.0',
     ('localhost', 6379), {'ssl': True, 'timeout': 1.0}),
    ('rediss://localhost?timeout=10',
     ('localhost', 6379), {'ssl': True, 'timeout': 10.0}),
    # unix scheme
    ('unix:///', '/', {}),
    ('unix:///redis.sock?db=12', '/redis.sock', {'db': 12}),
    ('unix:///redis.sock?encoding=utf-8',
     '/redis.sock', {'encoding': 'utf-8'}),
    ('unix:///redis.sock?ssl=true',
     '/redis.sock', {'ssl': True}),
    ('unix:///redis.sock?timeout=12',
     '/redis.sock', {'timeout': 12}),
    # no scheme
    ('/some/path/to/socket', '/some/path/to/socket', {}),
    ('/some/path/to/socket?db=1', '/some/path/to/socket?db=1', {}),
])
def test_good_url(url, expected_address, expected_options):
    address, options = parse_url(url)
    assert address == expected_address
    assert options == expected_options


@pytest.mark.parametrize('url,expected_error', [
    ('bad-scheme://localhost:6379/',
     ("Unsupported URI scheme", 'bad-scheme')),
    ('redis:///?db=1&db=2',
     ("Multiple parameters are not allowed", "db", "2")),
    ('redis:///?db=',
     ("Empty parameters are not allowed", "db", "")),
    ('redis:///?foo=',
     ("Empty parameters are not allowed", "foo", "")),
    ('unix://',
     ('Empty path is not allowed', 'unix://')),
    ('unix://host:123/',
     ('Netlocation is not allowed for unix scheme', 'host:123')),
    ('unix://user:pass@host:123/',
     ('Netlocation is not allowed for unix scheme', 'user:pass@host:123')),
    ('unix://user:pass@/',
     ('Netlocation is not allowed for unix scheme', 'user:pass@')),
    ('redis:///01',
     ('Expected integer without leading zeroes', '01')),
    ('rediss:///01',
     ('Expected integer without leading zeroes', '01')),
    ('redis:///?db=01',
     ('Expected integer without leading zeroes', '01')),
    ('rediss:///?db=01',
     ('Expected integer without leading zeroes', '01')),
    ('redis:///1?db=2',
     ('Single DB value expected, got path and query', 1, 2)),
    ('rediss:///1?db=2',
     ('Single DB value expected, got path and query', 1, 2)),
    ('redis://:passwd@localhost/?password=passwd',
     ('Single password value is expected, got in net location and query')),
    ('redis:///?ssl=1',
     ("Expected 'ssl' param to be 'true' or 'false' only", '1')),
    ('redis:///?ssl=True',
     ("Expected 'ssl' param to be 'true' or 'false' only", 'True')),
])
def test_url_assertions(url, expected_error):
    with pytest.raises(AssertionError) as exc_info:
        parse_url(url)
    assert exc_info.value.args == (expected_error,)


@pytest.mark.parametrize('url', [
    'redis:///bad-db-num',
    'redis:///0/1',
    'redis:///?db=bad-num',
    'redis:///?db=-1',
])
def test_db_num_assertions(url):
    with pytest.raises(AssertionError, match="Invalid decimal integer"):
        parse_url(url)
