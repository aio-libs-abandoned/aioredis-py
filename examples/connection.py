import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        conn = yield from aioredis.create_connection(('localhost', 6379))

        ok = yield from conn.execute('set', 'my-key', 'some value')
        assert ok == b'OK', ok

        value = yield from conn.execute('get', 'my-key')
        print('raw value:', value)

        # optionally close connection
        conn.close()
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
