aioredis
========

asyncio (PEP 3156) Redis client library.

.. image:: https://travis-ci.org/aio-libs/aioredis.svg?branch=master
   :target: https://travis-ci.org/aio-libs/aioredis


.. image:: https://codecov.io/gh/aio-libs/aioredis/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/aio-libs/aioredis

Features
--------

================================  ==============================
hiredis_ parser                     Yes
Pure-python parser                  TBD
Low-level & High-level APIs         Yes
Connections Pool                    Yes
Pipelining support                  Yes
Pub/Sub support                     Yes
SSL/TLS support                     Yes
Redis Cluster support               WIP
Trollius (python 2.7)               No
Tested python versions              `3.3, 3.4, 3.5`_
Tested for Redis server             `2.6, 2.8, 3.0`_
Support for dev Redis server        through low-level API
================================  ==============================

Documentation
-------------

http://aioredis.readthedocs.io/

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
        yield from pool.clear()    # closing all open connections

    loop.run_until_complete(go())


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* hiredis_

.. note::

    hiredis is preferred requirement.
    Pure-python fallback protocol parser is TBD.

Discussion list
---------------

*aio-libs* google group: https://groups.google.com/forum/#!forum/aio-libs

License
-------

The aioredis is offered under MIT license.

.. _Python: https://www.python.org
.. _asyncio: https://pypi.python.org/pypi/asyncio
.. _hiredis: https://pypi.python.org/pypi/hiredis
.. _3.3, 3.4, 3.5:
.. _2.6, 2.8, 3.0:
.. _travis: https://travis-ci.org/aio-libs/aioredis
