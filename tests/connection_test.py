import pytest
import asyncio
import sys
from unittest import mock

from unittest.mock import patch

from aioredis import (
    ConnectionClosedError,
    ProtocolError,
    RedisConnection,
    RedisError,
    ReplyError,
    Channel,
    MaxClientsError,
    )
from _testutils import redis_version


async def test_connect_tcp(request, create_connection, server):
    conn = await create_connection(server.tcp_address)
    assert conn.db == 0
    assert isinstance(conn.address, tuple)
    assert conn.address[0] in ('127.0.0.1', '::1')
    assert conn.address[1] == server.tcp_address.port
    assert str(conn) == '<RedisConnection [db:0]>'

    conn = await create_connection(['localhost', server.tcp_address.port])
    assert conn.db == 0
    assert isinstance(conn.address, tuple)
    assert conn.address[0] in ('127.0.0.1', '::1')
    assert conn.address[1] == server.tcp_address.port
    assert str(conn) == '<RedisConnection [db:0]>'


async def test_connect_inject_connection_cls(
        request,
        create_connection,
        server):

    class MyConnection(RedisConnection):
        pass

    conn = await create_connection(
        server.tcp_address, connection_cls=MyConnection)

    assert isinstance(conn, MyConnection)


async def test_connect_inject_connection_cls_invalid(
        request,
        create_connection,
        server):

    with pytest.raises(AssertionError):
        await create_connection(
            server.tcp_address, connection_cls=type)


async def test_connect_tcp_timeout(request, create_connection, server):
    with patch('aioredis.connection.open_connection') as open_conn_mock:
        open_conn_mock.side_effect = lambda *a, **kw: asyncio.sleep(0.2)
        with pytest.raises(asyncio.TimeoutError):
            await create_connection(server.tcp_address, timeout=0.1)


async def test_connect_tcp_invalid_timeout(
        request, create_connection, server):
    with pytest.raises(ValueError):
        await create_connection(
            server.tcp_address, timeout=0)


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
async def test_connect_unixsocket(create_connection, server):
    conn = await create_connection(server.unixsocket, db=0)
    assert conn.db == 0
    assert conn.address == server.unixsocket
    assert str(conn) == '<RedisConnection [db:0]>'


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
async def test_connect_unixsocket_timeout(create_connection, server):
    with patch('aioredis.connection.open_unix_connection') as open_conn_mock:
        open_conn_mock.side_effect = lambda *a, **kw: asyncio.sleep(0.2)
        with pytest.raises(asyncio.TimeoutError):
            await create_connection(server.unixsocket, db=0, timeout=0.1)


@redis_version(2, 8, 0, reason="maxclients config setting")
async def test_connect_maxclients(create_connection, start_server):
    server = start_server('server-maxclients')
    conn = await create_connection(server.tcp_address)
    await conn.execute(b'CONFIG', b'SET', 'maxclients', 1)

    errors = (MaxClientsError, ConnectionClosedError, ConnectionError)
    with pytest.raises(errors):
        conn2 = await create_connection(server.tcp_address)
        await conn2.execute('ping')


async def test_select_db(create_connection, server):
    address = server.tcp_address
    conn = await create_connection(address)
    assert conn.db == 0

    with pytest.raises(ValueError):
        await create_connection(address, db=-1)
    with pytest.raises(TypeError):
        await create_connection(address, db=1.0)
    with pytest.raises(TypeError):
        await create_connection(address, db='bad value')
    with pytest.raises(TypeError):
        conn = await create_connection(address, db=None)
        await conn.select(None)
    with pytest.raises(ReplyError):
        await create_connection(address, db=100000)

    await conn.select(1)
    assert conn.db == 1
    await conn.select(2)
    assert conn.db == 2
    await conn.execute('select', 0)
    assert conn.db == 0
    await conn.execute(b'select', 1)
    assert conn.db == 1


async def test_protocol_error(create_connection, server):
    conn = await create_connection(server.tcp_address)

    reader = conn._reader

    with pytest.raises(ProtocolError):
        reader.feed_data(b'not good redis protocol response')
        await conn.select(1)

    assert len(conn._waiters) == 0


def test_close_connection__tcp(create_connection, loop, server):
    conn = loop.run_until_complete(create_connection(server.tcp_address))
    conn.close()
    with pytest.raises(ConnectionClosedError):
        loop.run_until_complete(conn.select(1))

    conn = loop.run_until_complete(create_connection(server.tcp_address))
    conn.close()
    fut = None
    with pytest.raises(ConnectionClosedError):
        fut = conn.select(1)
    assert fut is None

    conn = loop.run_until_complete(create_connection(server.tcp_address))
    conn.close()
    with pytest.raises(ConnectionClosedError):
        conn.execute_pubsub('subscribe', 'channel:1')


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
async def test_close_connection__socket(create_connection, server):
    conn = await create_connection(server.unixsocket)
    conn.close()
    with pytest.raises(ConnectionClosedError):
        await conn.select(1)

    conn = await create_connection(server.unixsocket)
    conn.close()
    with pytest.raises(ConnectionClosedError):
        await conn.execute_pubsub('subscribe', 'channel:1')


async def test_closed_connection_with_none_reader(
        create_connection, server):
    address = server.tcp_address
    conn = await create_connection(address)
    stored_reader = conn._reader
    conn._reader = None
    with pytest.raises(ConnectionClosedError):
        await conn.execute('blpop', 'test', 0)
    conn._reader = stored_reader
    conn.close()

    conn = await create_connection(address)
    stored_reader = conn._reader
    conn._reader = None
    with pytest.raises(ConnectionClosedError):
        await conn.execute_pubsub('subscribe', 'channel:1')
    conn._reader = stored_reader
    conn.close()


async def test_wait_closed(create_connection, server):
    address = server.tcp_address
    conn = await create_connection(address)
    reader_task = conn._reader_task
    conn.close()
    assert not reader_task.done()
    await conn.wait_closed()
    assert reader_task.done()


async def test_cancel_wait_closed(create_connection, loop, server):
    # Regression test: Don't throw error if wait_closed() is cancelled.
    address = server.tcp_address
    conn = await create_connection(address)
    reader_task = conn._reader_task
    conn.close()
    task = asyncio.ensure_future(conn.wait_closed())

    # Make sure the task is cancelled
    # after it has been started by the loop.
    loop.call_soon(task.cancel)

    await conn.wait_closed()
    assert reader_task.done()


async def test_auth(create_connection, server):
    conn = await create_connection(server.tcp_address)

    res = await conn.execute('CONFIG', 'SET', 'requirepass', 'pass')
    assert res == b'OK'

    conn2 = await create_connection(server.tcp_address)

    with pytest.raises(ReplyError):
        await conn2.select(1)

    res = await conn2.auth('pass')
    assert res is True
    res = await conn2.select(1)
    assert res is True

    conn3 = await create_connection(server.tcp_address, password='pass')

    res = await conn3.select(1)
    assert res is True

    res = await conn2.execute('CONFIG', 'SET', 'requirepass', '')
    assert res == b'OK'


async def test_decoding(create_connection, server):
    conn = await create_connection(server.tcp_address, encoding='utf-8')
    assert conn.encoding == 'utf-8'
    res = await conn.execute('set', '{prefix}:key1', 'value')
    assert res == 'OK'
    res = await conn.execute('get', '{prefix}:key1')
    assert res == 'value'

    res = await conn.execute('set', '{prefix}:key1', b'bin-value')
    assert res == 'OK'
    res = await conn.execute('get', '{prefix}:key1')
    assert res == 'bin-value'

    res = await conn.execute('get', '{prefix}:key1', encoding='ascii')
    assert res == 'bin-value'
    res = await conn.execute('get', '{prefix}:key1', encoding=None)
    assert res == b'bin-value'

    with pytest.raises(UnicodeDecodeError):
        await conn.execute('set', '{prefix}:key1', 'значение')
        await conn.execute('get', '{prefix}:key1', encoding='ascii')

    conn2 = await create_connection(server.tcp_address)
    res = await conn2.execute('get', '{prefix}:key1', encoding='utf-8')
    assert res == 'значение'


async def test_execute_exceptions(create_connection, server):
    conn = await create_connection(server.tcp_address)
    with pytest.raises(TypeError):
        await conn.execute(None)
    with pytest.raises(TypeError):
        await conn.execute("ECHO", None)
    with pytest.raises(TypeError):
        await conn.execute("GET", ('a', 'b'))
    assert len(conn._waiters) == 0


async def test_subscribe_unsubscribe(create_connection, server):
    conn = await create_connection(server.tcp_address)

    assert conn.in_pubsub == 0

    res = await conn.execute('subscribe', 'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]

    assert conn.in_pubsub == 1

    res = await conn.execute('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 0]]
    assert conn.in_pubsub == 0

    res = await conn.execute('subscribe', 'chan:1', 'chan:2')
    assert res == [[b'subscribe', b'chan:1', 1],
                   [b'subscribe', b'chan:2', 2],
                   ]
    assert conn.in_pubsub == 2

    res = await conn.execute('unsubscribe', 'non-existent')
    assert res == [[b'unsubscribe', b'non-existent', 2]]
    assert conn.in_pubsub == 2

    res = await conn.execute('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 1]]
    assert conn.in_pubsub == 1


async def test_psubscribe_punsubscribe(create_connection, server):
    conn = await create_connection(server.tcp_address)
    res = await conn.execute('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 1]]
    assert conn.in_pubsub == 1


async def test_bad_command_in_pubsub(create_connection, server):
    conn = await create_connection(server.tcp_address)

    res = await conn.execute('subscribe', 'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]

    msg = "Connection in SUBSCRIBE mode"
    with pytest.raises(RedisError, match=msg):
        await conn.execute('select', 1)
    with pytest.raises(RedisError, match=msg):
        conn.execute('get')


async def test_pubsub_messages(create_connection, server):
    sub = await create_connection(server.tcp_address)
    pub = await create_connection(server.tcp_address)
    res = await sub.execute('subscribe', 'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]

    assert b'chan:1' in sub.pubsub_channels
    chan = sub.pubsub_channels[b'chan:1']
    assert str(chan) == "<Channel name:b'chan:1', is_pattern:False, qsize:0>"
    assert chan.name == b'chan:1'
    assert chan.is_active is True

    res = await pub.execute('publish', 'chan:1', 'Hello!')
    assert res == 1
    msg = await chan.get()
    assert msg == b'Hello!'

    res = await sub.execute('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 2]]
    assert b'chan:*' in sub.pubsub_patterns
    chan2 = sub.pubsub_patterns[b'chan:*']
    assert chan2.name == b'chan:*'
    assert chan2.is_active is True

    res = await pub.execute('publish', 'chan:1', 'Hello!')
    assert res == 2

    msg = await chan.get()
    assert msg == b'Hello!'
    dest_chan, msg = await chan2.get()
    assert dest_chan == b'chan:1'
    assert msg == b'Hello!'


async def test_multiple_subscribe_unsubscribe(create_connection, server):
    sub = await create_connection(server.tcp_address)

    res = await sub.execute_pubsub('subscribe', 'chan:1')
    ch = sub.pubsub_channels['chan:1']
    assert res == [[b'subscribe', b'chan:1', 1]]
    res = await sub.execute_pubsub('subscribe', b'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]
    assert ch is sub.pubsub_channels['chan:1']
    res = await sub.execute_pubsub('subscribe', ch)
    assert res == [[b'subscribe', b'chan:1', 1]]
    assert ch is sub.pubsub_channels['chan:1']

    res = await sub.execute_pubsub('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 0]]
    res = await sub.execute_pubsub('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 0]]

    res = await sub.execute_pubsub('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 1]]
    res = await sub.execute_pubsub('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 1]]

    res = await sub.execute_pubsub('punsubscribe', 'chan:*')
    assert res == [[b'punsubscribe', b'chan:*', 0]]
    res = await sub.execute_pubsub('punsubscribe', 'chan:*')
    assert res == [[b'punsubscribe', b'chan:*', 0]]


async def test_execute_pubsub_errors(create_connection, server):
    sub = await create_connection(server.tcp_address)

    with pytest.raises(TypeError):
        sub.execute_pubsub('subscribe', "chan:1", None)
    with pytest.raises(TypeError):
        sub.execute_pubsub('subscribe')
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'subscribe',
            Channel('chan:1', is_pattern=True))
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'unsubscribe',
            Channel('chan:1', is_pattern=True))
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'psubscribe',
            Channel('chan:1', is_pattern=False))
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'punsubscribe',
            Channel('chan:1', is_pattern=False))


async def test_multi_exec(create_connection, server):
    conn = await create_connection(server.tcp_address)

    ok = await conn.execute('set', 'foo', 'bar')
    assert ok == b'OK'

    ok = await conn.execute("MULTI")
    assert ok == b'OK'
    queued = await conn.execute('getset', 'foo', 'baz')
    assert queued == b'QUEUED'
    res = await conn.execute("EXEC")
    assert res == [b'bar']

    ok = await conn.execute("MULTI")
    assert ok == b'OK'
    queued = await conn.execute('getset', 'foo', 'baz')
    assert queued == b'QUEUED'
    res = await conn.execute("DISCARD")
    assert res == b'OK'


async def test_multi_exec__enc(create_connection, server):
    conn = await create_connection(server.tcp_address, encoding='utf-8')

    ok = await conn.execute('set', 'foo', 'bar')
    assert ok == 'OK'

    ok = await conn.execute("MULTI")
    assert ok == 'OK'
    queued = await conn.execute('getset', 'foo', 'baz')
    assert queued == 'QUEUED'
    res = await conn.execute("EXEC")
    assert res == ['bar']

    ok = await conn.execute("MULTI")
    assert ok == 'OK'
    queued = await conn.execute('getset', 'foo', 'baz')
    assert queued == 'QUEUED'
    res = await conn.execute("DISCARD")
    assert res == 'OK'


async def test_connection_parser_argument(create_connection, server):
    klass = mock.MagicMock()
    klass.return_value = reader = mock.Mock()
    conn = await create_connection(server.tcp_address, parser=klass)

    assert klass.mock_calls == [
        mock.call(protocolError=ProtocolError, replyError=ReplyError),
    ]

    response = [False]

    def feed_gets(data, **kwargs):
        response[0] = data

    reader.gets.side_effect = lambda *args, **kwargs: response[0]
    reader.feed.side_effect = feed_gets
    assert b'+PONG\r\n' == await conn.execute('ping')


async def test_connection_idle_close(create_connection, start_server):
    server = start_server('idle')
    conn = await create_connection(server.tcp_address)
    ok = await conn.execute("config", "set", "timeout", 1)
    assert ok == b'OK'

    await asyncio.sleep(3)

    with pytest.raises(ConnectionClosedError):
        assert await conn.execute('ping') is None


@pytest.mark.parametrize('kwargs', [
    {},
    {'db': 1},
    {'encoding': 'utf-8'},
], ids=repr)
async def test_create_connection__tcp_url(
        create_connection, server_tcp_url, kwargs):
    url = server_tcp_url(**kwargs)
    db = kwargs.get('db', 0)
    enc = kwargs.get('encoding', None)
    conn = await create_connection(url)
    pong = b'PONG' if not enc else b'PONG'.decode(enc)
    assert await conn.execute('ping') == pong
    assert conn.db == db
    assert conn.encoding == enc


@pytest.mark.skipif('sys.platform == "win32"',
                    reason="No unix sockets on Windows")
@pytest.mark.parametrize('kwargs', [
    {},
    {'db': 1},
    {'encoding': 'utf-8'},
], ids=repr)
async def test_create_connection__unix_url(
        create_connection, server_unix_url, kwargs):
    url = server_unix_url(**kwargs)
    db = kwargs.get('db', 0)
    enc = kwargs.get('encoding', None)
    conn = await create_connection(url)
    pong = b'PONG' if not enc else b'PONG'.decode(enc)
    assert await conn.execute('ping') == pong
    assert conn.db == db
    assert conn.encoding == enc
