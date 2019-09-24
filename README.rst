aioredis
========

asyncio (PEP 3156) Redis client library.

.. image:: https://travis-ci.com/aio-libs/aioredis.svg?branch=master
   :target: https://travis-ci.com/aio-libs/aioredis


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
Sentinel support                    Yes
Redis Cluster support               WIP
Trollius (python 2.7)               No
Tested CPython versions             `3.5.3, 3.6, 3.7 <travis_>`_ [1]_
Tested PyPy3 versions               `pypy3.5-7.0 pypy3.6-7.1.1 <travis_>`_
Tested for Redis server             `2.6, 2.8, 3.0, 3.2, 4.0 5.0 <travis_>`_
Support for dev Redis server        through low-level API
================================  ==============================

.. [1] For Python 3.3, 3.4 support use aioredis v0.3.

Documentation
-------------

http://aioredis.readthedocs.io/

Usage example
-------------

Simple high-level interface with connections pool:

.. code:: python

    import asyncio
    import aioredis

    async def go():
        redis = await aioredis.create_redis_pool(
            'redis://localhost')
        await redis.set('my-key', 'value')
        val = await redis.get('my-key', encoding='utf-8')
        print(val)
        redis.close()
        await redis.wait_closed()

    asyncio.run(go())
    # will print 'value'

Requirements
------------

* Python_ 3.5.3+
* hiredis_

.. note::

    hiredis is preferred requirement.
    Pure-python protocol parser is implemented as well and can be used
    through ``parser`` parameter.

Benchmarks
----------

Benchmarks can be found here: https://github.com/popravich/python-redis-benchmark

Discussion list
---------------

*aio-libs* google group: https://groups.google.com/forum/#!forum/aio-libs

Or gitter room: https://gitter.im/aio-libs/Lobby

License
-------

The aioredis is offered under MIT license.

.. _Python: https://www.python.org
.. _hiredis: https://pypi.python.org/pypi/hiredis
.. _travis: https://travis-ci.com/aio-libs/aioredis
