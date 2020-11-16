.. highlight:: bash

.. _github: https://github.com/aio-libs/aioredis

Contributing
============

To start contributing you must read all the following.

First you must fork/clone repo from `github`_::

   $ git clone git@github.com:aio-libs/aioredis.git

Next, you should install all python dependencies, it is as easy as running
single command::

   $ make devel

this command will install:

* ``sphinx`` for building documentation;
* ``pytest`` for running tests;
* ``flake8`` for code linting;
* and few other packages.

Make sure you have provided a ``towncrier`` note.
Just add short description running following commands::

    $ echo "Short description" > CHANGES/filename.type

This will create new file in ``CHANGES`` directory.
Filename should consist of the ticket ID or other unique identifier.
Five default types are:

* .feature - signifying new feature
* .bugfix - signifying a bug fix
* .doc - documentation improvement
* .removal - deprecation or removal of public API
* .misc - a ticket has been closed, but not in interest of users

You can check if everything is correct by typing::

    $ towncrier --draft

To produce the news file::

    $ towncrier

Code style
----------

Code **must** be pep8 compliant.

You can check it with following command::

   $ make flake


Running tests
-------------

You can run tests in any of the following ways::

   # will run tests in a verbose mode
   $ make test
   # or
   $ pytest

   # or with particular Redis server
   $ pytest --redis-server=/usr/local/bin/redis-server tests/errors_test.py

   # will run tests with coverage report
   $ make cov
   # or
   $ pytest --cov

SSL tests
~~~~~~~~~

Running SSL tests requires following additional programs to be installed:

* ``openssl`` -- to generate test key and certificate;

* ``socat`` -- to make SSL proxy;

To install these on Ubuntu and generate test key & certificate run::

   $ sudo apt-get install socat openssl
   $ make certificate

Different Redis server versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run tests against different redises use ``--redis-server`` command line
option::

   $ pytest --redis-server=/path/to/custom/redis-server

UVLoop
~~~~~~

To run tests with :term:`uvloop`::

   $ pip install uvloop
   $ pytest --uvloop

.. note:: Until Python 3.5.2 EventLoop has no ``create_future`` method
   so aioredis won't benefit from uvloop's futures.


Writing tests
-------------

:mod:`aioredis` uses :term:`pytest` tool.

Tests are located under ``/tests`` directory.


Fixtures
~~~~~~~~

There is a number of fixtures that can be used to write tests:


.. attribute:: loop

   Current event loop used for test.
   This is a function-scope fixture.
   Using this fixture will always create new event loop and
   set global one to None.

   .. code-block:: python

      def test_with_loop(loop):
          @asyncio.coroutine
          def do_something():
              pass
          loop.run_until_complete(do_something())

.. function:: unused_port()

   Finds and returns free TCP port.

   .. code-block:: python

      def test_bind(unused_port):
          port = unused_port()
          assert 1024 < port <= 65535

.. cofunction:: create_connection(\*args, \**kw)

   Wrapper around :func:`aioredis.create_connection`.
   Only difference is that it registers connection to be closed after test case,
   so you should not be worried about unclosed connections.

.. cofunction:: create_redis(\*args, \**kw)

   Wrapper around :func:`aioredis.create_redis`.

.. cofunction:: create_pool(\*args, \**kw)

   Wrapper around :func:`aioredis.create_pool`.

.. attribute:: redis

   Redis client instance.

.. attribute:: pool

   RedisPool instance.

.. attribute:: server

   Redis server instance info. Namedtuple with following properties:

      name
         server instance name.

      port
         Bind port.

      unixsocket
         Bind unixsocket path.

      version
         Redis server version tuple.

.. attribute:: serverB

   Second predefined Redis server instance info.

.. function:: start_server(name)

   Start Redis server instance.
   Redis instances are cached by name.

   :return: server info tuple, see :attr:`server`.
   :rtype: tuple

.. function:: ssl_proxy(unsecure_port)

   Start SSL proxy.

   :param int unsecure_port: Redis server instance port
   :return: secure_port and ssl_context pair
   :rtype: tuple


``redis_version`` tests helper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In ``tests`` directory there is a :mod:`_testutils` module with a simple
helper --- :func:`redis_version` --- a function that add a pytest mark to a test
allowing to run it with requested Redis server versions.

.. function:: _testutils.redis_version(\*version, reason)

   Marks test with minimum redis version to run.

   Example:

   .. code-block:: python

      from _testutil import redis_version

      @redis_version(3, 2, 0, reason="HSTRLEN new in redis 3.2.0")
      def test_hstrlen(redis):
          pass
