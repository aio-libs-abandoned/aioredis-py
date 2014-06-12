aioredis
========

asyncio (PEP 3156) Redis support

.. image:: https://travis-ci.org/popravich/aioredis.svg?branch=master
   :target: https://travis-ci.org/popravich/aioredis

Documentation
-------------

TBD.

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
    loop.run_until_complete(go())
    # will print 'value'


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* hiredis_

License
-------

The aioredis is offered under MIT license.

.. _Python: https://www.python.org
.. _asyncio: https://pypi.python.org/pypi/asyncio
.. _hiredis: http://pypi.python.org/pypi/hiredis
