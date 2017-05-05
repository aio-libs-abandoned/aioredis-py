aioredis
========

asyncio (PEP 3156) Redis client library.

.. image:: https://travis-ci.org/aio-libs/aioredis.svg?branch=master
   :target: https://travis-ci.org/aio-libs/aioredis


.. image:: https://codecov.io/gh/aio-libs/aioredis/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/aio-libs/aioredis

.. image:: https://ci.appveyor.com/api/projects/status/wngyx6s98o6hsxmt/branch/master?svg=true
   :target: https://ci.appveyor.com/project/popravich/aioredis

Features
--------

================================  ==============================
hiredis_ parser                     Yes
Pure-python parser                  Yes
Low-level & High-level APIs         Yes
Connections Pool                    Yes
Pipelining support                  Yes
Pub/Sub support                     Yes
SSL/TLS support                     Yes
Sentinel support                    Yes [1]_
Redis Cluster support               WIP
Trollius (python 2.7)               No
Tested CPython versions             `3.3, 3.4, 3.5, 3.6 <travis_>`_
Tested PyPy3 versions               `5.7.1 <travis_>`_
Tested for Redis server             `2.6, 2.8, 3.0, 3.2 <travis_>`_
Support for dev Redis server        through low-level API
================================  ==============================


.. [1] Sentinel support is available in master branch.
   This feature is not yet stable and may have some issues.

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

    async def go():
        conn = await aioredis.create_connection(
            ('localhost', 6379), loop=loop)
        await conn.execute('set', 'my-key', 'value')
        val = await conn.execute('get', 'my-key')
        print(val)
        conn.close()
        await conn.wait_closed()
    loop.run_until_complete(go())
    # will print 'value'

Simple high-level interface:

.. code:: python

    import asyncio
    import aioredis

    loop = asyncio.get_event_loop()

    async def go():
        redis = await aioredis.create_redis(
            ('localhost', 6379), loop=loop)
        await redis.set('my-key', 'value')
        val = await redis.get('my-key')
        print(val)
        redis.close()
        await redis.wait_closed()
    loop.run_until_complete(go())
    # will print 'value'

Connections pool:

.. code:: python

    import asyncio
    import aioredis

    loop = asyncio.get_event_loop()

    async def go():
        pool = await aioredis.create_pool(
            ('localhost', 6379),
            minsize=5, maxsize=10,
            loop=loop)
        await pool.execute('set', 'my-key', 'value')
        print(await pool.execute('get', 'my-key'))
        # graceful shutdown
        pool.close()
        await pool.wait_closed()

    loop.run_until_complete(go())


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* hiredis_

.. note::

    hiredis is preferred requirement.
    Pure-python protocol parser is implemented as well and can be used
    through ``parser`` parameter.

Discussion list
---------------

*aio-libs* google group: https://groups.google.com/forum/#!forum/aio-libs

License
-------

The aioredis is offered under MIT license.

.. _Python: https://www.python.org
.. _asyncio: https://pypi.python.org/pypi/asyncio
.. _hiredis: https://pypi.python.org/pypi/hiredis
.. _travis: https://travis-ci.org/aio-libs/aioredis
