import pytest
import asyncio
import sys
from unittest import mock

from unittest.mock import patch

from aioredis.util import async_task
from aioredis import (
    ConnectionClosedError,
    ProtocolError,
    RedisConnection,
    RedisError,
    ReplyError,
    Channel,
    )


@pytest.mark.run_loop
def test_connect_tcp(request, create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)
    assert conn.db == 0
    assert isinstance(conn.address, tuple)
    assert conn.address[0] in ('127.0.0.1', '::1')
    assert conn.address[1] == server.tcp_address.port
    assert str(conn) == '<RedisConnection [db:0]>'

    conn = yield from create_connection(
        ['localhost', server.tcp_address.port], loop=loop)
    assert conn.db == 0
    assert isinstance(conn.address, tuple)
    assert conn.address[0] in ('127.0.0.1', '::1')
    assert conn.address[1] == server.tcp_address.port
    assert str(conn) == '<RedisConnection [db:0]>'


@pytest.mark.run_loop
def test_connect_inject_connection_cls(
        request,
        create_connection,
        loop,
        server):

    class MyConnection(RedisConnection):
        pass

    conn = yield from create_connection(
        server.tcp_address, loop=loop, connection_cls=MyConnection)

    assert isinstance(conn, MyConnection)


@pytest.mark.run_loop
def test_connect_inject_connection_cls_invalid(
        request,
        create_connection,
        loop,
        server):

    with pytest.raises(AssertionError):
        yield from create_connection(
            server.tcp_address, loop=loop, connection_cls=type)


@pytest.mark.run_loop
def test_connect_tcp_timeout(request, create_connection, loop, server):
    with patch('aioredis.connection.asyncio.open_connection') as\
            open_conn_mock:
        open_conn_mock.side_effect = lambda *a, **kw: asyncio.sleep(0.2,
                                                                    loop=loop)
        with pytest.raises(asyncio.TimeoutError):
            yield from create_connection(
                server.tcp_address, loop=loop, timeout=0.1)


@pytest.mark.run_loop
def test_connect_tcp_invalid_timeout(request, create_connection, loop, server):
    with pytest.raises(ValueError):
        yield from create_connection(
            server.tcp_address, loop=loop, timeout=0)


@pytest.mark.run_loop
@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
def test_connect_unixsocket(create_connection, loop, server):
    conn = yield from create_connection(
        server.unixsocket, db=0, loop=loop)
    assert conn.db == 0
    assert conn.address == server.unixsocket
    assert str(conn) == '<RedisConnection [db:0]>'


@pytest.mark.run_loop
@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
def test_connect_unixsocket_timeout(create_connection, loop, server):
    with patch('aioredis.connection.asyncio.open_unix_connection') as\
            open_conn_mock:
        open_conn_mock.side_effect = lambda *a, **kw: asyncio.sleep(0.2,
                                                                    loop=loop)
        with pytest.raises(asyncio.TimeoutError):
            yield from create_connection(
                server.unixsocket, db=0, loop=loop, timeout=0.1)


def test_global_loop(create_connection, loop, server):
    asyncio.set_event_loop(loop)

    conn = loop.run_until_complete(create_connection(
        server.tcp_address, db=0))
    assert conn.db == 0
    assert conn._loop is loop


@pytest.mark.run_loop
def test_select_db(create_connection, loop, server):
    address = server.tcp_address
    conn = yield from create_connection(address, loop=loop)
    assert conn.db == 0

    with pytest.raises(ValueError):
        yield from create_connection(address, db=-1, loop=loop)
    with pytest.raises(TypeError):
        yield from create_connection(address, db=1.0, loop=loop)
    with pytest.raises(TypeError):
        yield from create_connection(
            address, db='bad value', loop=loop)
    with pytest.raises(TypeError):
        conn = yield from create_connection(
            address, db=None, loop=loop)
        yield from conn.select(None)
    with pytest.raises(ReplyError):
        yield from create_connection(
            address, db=100000, loop=loop)

    yield from conn.select(1)
    assert conn.db == 1
    yield from conn.select(2)
    assert conn.db == 2
    yield from conn.execute('select', 0)
    assert conn.db == 0
    yield from conn.execute(b'select', 1)
    assert conn.db == 1


@pytest.mark.run_loop
def test_protocol_error(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)

    reader = conn._reader

    with pytest.raises(ProtocolError):
        reader.feed_data(b'not good redis protocol response')
        yield from conn.select(1)

    assert len(conn._waiters) == 0


def test_close_connection__tcp(create_connection, loop, server):
    conn = loop.run_until_complete(create_connection(
        server.tcp_address, loop=loop))
    conn.close()
    with pytest.raises(ConnectionClosedError):
        loop.run_until_complete(conn.select(1))

    conn = loop.run_until_complete(create_connection(
        server.tcp_address, loop=loop))
    conn.close()
    fut = None
    with pytest.raises(ConnectionClosedError):
        fut = conn.select(1)
    assert fut is None

    conn = loop.run_until_complete(create_connection(
        server.tcp_address, loop=loop))
    conn.close()
    with pytest.raises(ConnectionClosedError):
        conn.execute_pubsub('subscribe', 'channel:1')


@pytest.mark.run_loop
@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
def test_close_connection__socket(create_connection, loop, server):
    conn = yield from create_connection(
        server.unixsocket, loop=loop)
    conn.close()
    with pytest.raises(ConnectionClosedError):
        yield from conn.select(1)

    conn = yield from create_connection(
        server.unixsocket, loop=loop)
    conn.close()
    with pytest.raises(ConnectionClosedError):
        yield from conn.execute_pubsub('subscribe', 'channel:1')


@pytest.mark.run_loop
def test_closed_connection_with_none_reader(create_connection, loop, server):
    address = server.tcp_address
    conn = yield from create_connection(address, loop=loop)
    stored_reader = conn._reader
    conn._reader = None
    with pytest.raises(ConnectionClosedError):
        yield from conn.execute('blpop', 'test', 0)
    conn._reader = stored_reader
    conn.close()

    conn = yield from create_connection(address, loop=loop)
    stored_reader = conn._reader
    conn._reader = None
    with pytest.raises(ConnectionClosedError):
        yield from conn.execute_pubsub('subscribe', 'channel:1')
    conn._reader = stored_reader
    conn.close()


@pytest.mark.run_loop
def test_wait_closed(create_connection, loop, server):
    address = server.tcp_address
    conn = yield from create_connection(address, loop=loop)
    reader_task = conn._reader_task
    conn.close()
    assert not reader_task.done()
    yield from conn.wait_closed()
    assert reader_task.done()


@pytest.mark.run_loop
def test_cancel_wait_closed(create_connection, loop, server):
    # Regression test: Don't throw error if wait_closed() is cancelled.
    address = server.tcp_address
    conn = yield from create_connection(address, loop=loop)
    reader_task = conn._reader_task
    conn.close()
    task = async_task(conn.wait_closed(), loop=loop)

    # Make sure the task is cancelled
    # after it has been started by the loop.
    loop.call_soon(task.cancel)

    yield from conn.wait_closed()
    assert reader_task.done()


@pytest.mark.run_loop
def test_auth(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)

    res = yield from conn.execute('CONFIG', 'SET', 'requirepass', 'pass')
    assert res == b'OK'

    conn2 = yield from create_connection(
        server.tcp_address, loop=loop)

    with pytest.raises(ReplyError):
        yield from conn2.select(1)

    res = yield from conn2.auth('pass')
    assert res is True
    res = yield from conn2.select(1)
    assert res is True

    conn3 = yield from create_connection(
        server.tcp_address, password='pass', loop=loop)

    res = yield from conn3.select(1)
    assert res is True

    res = yield from conn2.execute('CONFIG', 'SET', 'requirepass', '')
    assert res == b'OK'


@pytest.mark.run_loop
def test_decoding(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, encoding='utf-8', loop=loop)
    assert conn.encoding == 'utf-8'
    res = yield from conn.execute('set', '{prefix}:key1', 'value')
    assert res == 'OK'
    res = yield from conn.execute('get', '{prefix}:key1')
    assert res == 'value'

    res = yield from conn.execute('set', '{prefix}:key1', b'bin-value')
    assert res == 'OK'
    res = yield from conn.execute('get', '{prefix}:key1')
    assert res == 'bin-value'

    res = yield from conn.execute('get', '{prefix}:key1', encoding='ascii')
    assert res == 'bin-value'
    res = yield from conn.execute('get', '{prefix}:key1', encoding=None)
    assert res == b'bin-value'

    with pytest.raises(UnicodeDecodeError):
        yield from conn.execute('set', '{prefix}:key1', 'значение')
        yield from conn.execute('get', '{prefix}:key1', encoding='ascii')

    conn2 = yield from create_connection(
        server.tcp_address, loop=loop)
    res = yield from conn2.execute('get', '{prefix}:key1', encoding='utf-8')
    assert res == 'значение'


@pytest.mark.run_loop
def test_execute_exceptions(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)
    with pytest.raises(TypeError):
        yield from conn.execute(None)
    with pytest.raises(TypeError):
        yield from conn.execute("ECHO", None)
    with pytest.raises(TypeError):
        yield from conn.execute("GET", ('a', 'b'))
    assert len(conn._waiters) == 0


@pytest.mark.run_loop
def test_subscribe_unsubscribe(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)

    assert conn.in_pubsub == 0

    res = yield from conn.execute('subscribe', 'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]

    assert conn.in_pubsub == 1

    res = yield from conn.execute('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 0]]
    assert conn.in_pubsub == 0

    res = yield from conn.execute('subscribe', 'chan:1', 'chan:2')
    assert res == [[b'subscribe', b'chan:1', 1],
                   [b'subscribe', b'chan:2', 2],
                   ]
    assert conn.in_pubsub == 2

    res = yield from conn.execute('unsubscribe', 'non-existent')
    assert res == [[b'unsubscribe', b'non-existent', 2]]
    assert conn.in_pubsub == 2

    res = yield from conn.execute('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 1]]
    assert conn.in_pubsub == 1


@pytest.mark.run_loop
def test_psubscribe_punsubscribe(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)
    res = yield from conn.execute('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 1]]
    assert conn.in_pubsub == 1


@pytest.mark.run_loop
def test_bad_command_in_pubsub(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop)

    res = yield from conn.execute('subscribe', 'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]

    msg = "Connection in SUBSCRIBE mode"
    with pytest.raises(RedisError, match=msg):
        yield from conn.execute('select', 1)
    with pytest.raises(RedisError, match=msg):
        conn.execute('get')


@pytest.mark.run_loop
def test_pubsub_messages(create_connection, loop, server):
    sub = yield from create_connection(
        server.tcp_address, loop=loop)
    pub = yield from create_connection(
        server.tcp_address, loop=loop)
    res = yield from sub.execute('subscribe', 'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]

    assert b'chan:1' in sub.pubsub_channels
    chan = sub.pubsub_channels[b'chan:1']
    assert str(chan) == "<Channel name:b'chan:1', is_pattern:False, qsize:0>"
    assert chan.name == b'chan:1'
    assert chan.is_active is True

    res = yield from pub.execute('publish', 'chan:1', 'Hello!')
    assert res == 1
    msg = yield from chan.get()
    assert msg == b'Hello!'

    res = yield from sub.execute('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 2]]
    assert b'chan:*' in sub.pubsub_patterns
    chan2 = sub.pubsub_patterns[b'chan:*']
    assert chan2.name == b'chan:*'
    assert chan2.is_active is True

    res = yield from pub.execute('publish', 'chan:1', 'Hello!')
    assert res == 2

    msg = yield from chan.get()
    assert msg == b'Hello!'
    dest_chan, msg = yield from chan2.get()
    assert dest_chan == b'chan:1'
    assert msg == b'Hello!'


@pytest.mark.run_loop
def test_multiple_subscribe_unsubscribe(create_connection, loop, server):
    sub = yield from create_connection(server.tcp_address, loop=loop)

    res = yield from sub.execute_pubsub('subscribe', 'chan:1')
    ch = sub.pubsub_channels['chan:1']
    assert res == [[b'subscribe', b'chan:1', 1]]
    res = yield from sub.execute_pubsub('subscribe', b'chan:1')
    assert res == [[b'subscribe', b'chan:1', 1]]
    assert ch is sub.pubsub_channels['chan:1']
    res = yield from sub.execute_pubsub('subscribe', ch)
    assert res == [[b'subscribe', b'chan:1', 1]]
    assert ch is sub.pubsub_channels['chan:1']

    res = yield from sub.execute_pubsub('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 0]]
    res = yield from sub.execute_pubsub('unsubscribe', 'chan:1')
    assert res == [[b'unsubscribe', b'chan:1', 0]]

    res = yield from sub.execute_pubsub('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 1]]
    res = yield from sub.execute_pubsub('psubscribe', 'chan:*')
    assert res == [[b'psubscribe', b'chan:*', 1]]

    res = yield from sub.execute_pubsub('punsubscribe', 'chan:*')
    assert res == [[b'punsubscribe', b'chan:*', 0]]
    res = yield from sub.execute_pubsub('punsubscribe', 'chan:*')
    assert res == [[b'punsubscribe', b'chan:*', 0]]


@pytest.mark.run_loop
def test_execute_pubsub_errors(create_connection, loop, server):
    sub = yield from create_connection(
        server.tcp_address, loop=loop)

    with pytest.raises(TypeError):
        sub.execute_pubsub('subscribe', "chan:1", None)
    with pytest.raises(TypeError):
        sub.execute_pubsub('subscribe')
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'subscribe',
            Channel('chan:1', is_pattern=True, loop=loop))
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'unsubscribe',
            Channel('chan:1', is_pattern=True, loop=loop))
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'psubscribe',
            Channel('chan:1', is_pattern=False, loop=loop))
    with pytest.raises(ValueError):
        sub.execute_pubsub(
            'punsubscribe',
            Channel('chan:1', is_pattern=False, loop=loop))


@pytest.mark.run_loop
def test_multi_exec(create_connection, loop, server):
    conn = yield from create_connection(server.tcp_address, loop=loop)

    ok = yield from conn.execute('set', 'foo', 'bar')
    assert ok == b'OK'

    ok = yield from conn.execute("MULTI")
    assert ok == b'OK'
    queued = yield from conn.execute('getset', 'foo', 'baz')
    assert queued == b'QUEUED'
    res = yield from conn.execute("EXEC")
    assert res == [b'bar']

    ok = yield from conn.execute("MULTI")
    assert ok == b'OK'
    queued = yield from conn.execute('getset', 'foo', 'baz')
    assert queued == b'QUEUED'
    res = yield from conn.execute("DISCARD")
    assert res == b'OK'


@pytest.mark.run_loop
def test_multi_exec__enc(create_connection, loop, server):
    conn = yield from create_connection(
        server.tcp_address, loop=loop, encoding='utf-8')

    ok = yield from conn.execute('set', 'foo', 'bar')
    assert ok == 'OK'

    ok = yield from conn.execute("MULTI")
    assert ok == 'OK'
    queued = yield from conn.execute('getset', 'foo', 'baz')
    assert queued == 'QUEUED'
    res = yield from conn.execute("EXEC")
    assert res == ['bar']

    ok = yield from conn.execute("MULTI")
    assert ok == 'OK'
    queued = yield from conn.execute('getset', 'foo', 'baz')
    assert queued == 'QUEUED'
    res = yield from conn.execute("DISCARD")
    assert res == 'OK'


@pytest.mark.run_loop
def test_connection_parser_argument(create_connection, server, loop):
    klass = mock.MagicMock()
    klass.return_value = reader = mock.Mock()
    conn = yield from create_connection(server.tcp_address,
                                        parser=klass, loop=loop)

    def ret_once(*args, _called=[False]):
        if not _called[0]:
            _called[0] = True
            return b'xxx'
        return False

    reader.gets.side_effect = ret_once
    assert b'xxx' == (yield from conn.execute('ping'))

    assert klass.mock_calls == [
        mock.call(protocolError=ProtocolError, replyError=ReplyError),
        mock.call().feed(b'+PONG\r\n'),
        mock.call().gets(),
        mock.call().gets(),
    ]
