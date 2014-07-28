import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        conn = yield from aioredis.create_connection(
            ('localhost', 6379), encoding='utf-8')

        ok = yield from conn.execute('set', 'my-key', 'some value')
        assert ok == 'OK', ok

        str_value = yield from conn.execute('get', 'my-key')
        raw_value = yield from conn.execute('get', 'my-key', encoding=None)
        assert str_value == 'some value'
        assert raw_value == b'some value'

        print('str value:', str_value)
        print('raw value:', raw_value)

        # optionally close connection
        conn.close()
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
