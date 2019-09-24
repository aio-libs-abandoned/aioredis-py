.. aioredis documentation master file, created by
   sphinx-quickstart on Thu Jun 12 22:57:11 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aioredis
========

asyncio (:pep:`3156`) Redis client library.

The library is intended to provide simple and clear interface to Redis
based on :term:`asyncio`.


Features
--------

================================  ==============================
:term:`hiredis` parser              Yes
Pure-python parser                  Yes
Low-level & High-level APIs         Yes
Connections Pool                    Yes
Pipelining support                  Yes
Pub/Sub support                     Yes
Sentinel support                    Yes
Redis Cluster support               WIP
Trollius (python 2.7)               No
Tested CPython versions             `3.5.3, 3.6, 3.7 <travis_>`_ [1]_
Tested PyPy3 versions               `pypy3.5-7.0 pypy3.6-7.1.1 <travis_>`_
Tested for Redis server             `2.6, 2.8, 3.0, 3.2, 4.0 5.0 <travis_>`_
Support for dev Redis server        through low-level API
================================  ==============================

.. [1] For Python 3.3, 3.4 support use aioredis v0.3.

Installation
------------

The easiest way to install aioredis is by using the package on PyPi::

   pip install aioredis

Requirements
------------

- Python 3.5.3+
- :term:`hiredis`

Benchmarks
----------

Benchmarks can be found here: https://github.com/popravich/python-redis-benchmark

Contribute
----------

- Issue Tracker: https://github.com/aio-libs/aioredis/issues
- Google Group: https://groups.google.com/forum/#!forum/aio-libs
- Gitter: https://gitter.im/aio-libs/Lobby
- Source Code: https://github.com/aio-libs/aioredis
- Contributor's guide: :doc:`devel`

Feel free to file an issue or make pull request if you find any bugs or have
some suggestions for library improvement.

License
-------

The aioredis is offered under `MIT license`_.

----

Contents
========

.. toctree::
   :maxdepth: 3

   start
   api_reference
   mixins
   abc
   mpsc
   sentinel
   examples
   devel
   migration
   releases
   glossary

.. ::
   todo insert after start
   advanced

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _MIT license: https://github.com/aio-libs/aioredis/blob/master/LICENSE
.. _travis: https://travis-ci.com/aio-libs/aioredis
