aioredis
========

asyncio (PEP 3156) Redis support

.. image:: https://travis-ci.org/aio-libs/aioredis.svg?branch=master
   :target: https://travis-ci.org/aio-libs/aioredis

Documentation
-------------

http://aioredis.readthedocs.org/

Usage examples
--------------

Simple low-level interface:

.. code:: python

    import asyncio
    import aioredis

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        conn = yield from aioredis.create_connection(
            ('localhost', 6379), loop=loop)
        yield from conn.execute('set', 'my-key', 'value')
        val = yield from conn.execute('get', 'my-key')
        print(val)
        conn.close()
    loop.run_until_complete(go())
    # will print 'value'

Simple high-level interface:

.. code:: python

    import asyncio
    import aioredis

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        redis = yield from aioredis.create_redis(
            ('localhost', 6379), loop=loop)
        yield from redis.set('my-key', 'value')
        val = yield from redis.get('my-key')
        print(val)
        redis.close()
    loop.run_until_complete(go())
    # will print 'value'

Connections pool:

.. code:: python

    import asyncio
    import aioredis

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        pool = yield from aioredis.create_pool(
            ('localhost', 6379),
            minsize=5, maxsize=10,
            loop=loop)
        with (yield from pool) as redis:    # high-level redis API instance
            yield from redis.set('my-key', 'value')
            print((yield from redis.get('my-key')))
        pool.clear()    # closing all open connections

    loop.run_until_complete(go())


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* hiredis_

.. note::

    hiredis is preferred requirement.
    Pure-python fallback protocol parser is TBD.


License
-------

The aioredis is offered under MIT license.

.. _Python: https://www.python.org
.. _asyncio: https://pypi.python.org/pypi/asyncio
.. _hiredis: https://pypi.python.org/pypi/hiredis
