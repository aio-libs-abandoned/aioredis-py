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
   $ py.test

   # will run tests with coverage report
   $ make cov
   # or
   $ py.test --cov


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

   $ py.test --redis-server=/path/to/custom/redis-server

UVLoop
~~~~~~

To run tests with :term:`uvloop`::

   $ pip install uvloop
   $ py.test --uvloop

.. note:: Until Python 3.5.2 EventLoop has no ``create_future`` method
   so aioredis won't benefit from uvloop's futures.


Writing tests
-------------

:mod:`aioredis` uses :term:`pytest` tool.

Tests are located under ``/tests`` directory.

Pure Python 3.5 tests (ie the ones using ``async``/``await`` syntax) must be
prefixed with ``py35_``, for instance see::

   tests/py35_generic_commands_tests.py
   tests/py35_pool_test.py


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


Helpers
~~~~~~~

:mod:`aioredis` also updates :term:`pytest`'s namespace with several helpers.

.. function:: pytest.redis_version(\*version, reason)

   Marks test with minimum redis version to run.

   Example:

   .. code-block:: python

      @pytest.redis_version(3, 2, 0, reason="HSTRLEN new in redis 3.2.0")
      def test_hstrlen(redis):
          pass


.. function:: pytest.logs(logger, level=None)

   Adopted version of :meth:`unittest.TestCase.assertEqual`,
   see it for details.

   Example:

   .. code-block:: python

      def test_logs(create_connection, server):
          with pytest.logs('aioredis', 'DEBUG') as cm:
              conn yield from create_connection(server.tcp_address)
          assert cm.output[0].startswith(
            'DEBUG:aioredis:Creating tcp connection')


.. function:: pytest.assert_almost_equal(first, second, places=None, \
                                         msg=None, delta=None)

   Adopted version of :meth:`unittest.TestCase.assertAlmostEqual`.


.. function:: pytest.raises_regex(exc_type, message)

   Adopted version of :meth:`unittest.TestCase.assertRaisesRegex`.
