# Contributing

To start contributing you must read all the following.

First you must fork/clone repo from
[github](https://github.com/aio-libs/aioredis):

    $ git clone git@github.com:aio-libs/aioredis.git

Next, you should install all python dependencies, it is as easy as
running single command:

    $ make devel

this command will install:

-   `mkdocs` for building documentation;
-   `pytest` for running tests;
-   `flake8` and `black` for code linting;
-   and few other packages.

Make sure you have provided a `towncrier` note. Just add short
description running following commands:

    $ echo "Short description" > CHANGES/filename.type

This will create new file in `CHANGES` directory. Filename should
consist of the ticket ID or other unique identifier. Five default types
are:

-   .feature - signifying new feature
-   .bugfix - signifying a bug fix
-   .doc - documentation improvement
-   .removal - deprecation or removal of public API
-   .misc - a ticket has been closed, but not in interest of users

You can check if everything is correct by typing:

    $ towncrier --draft

To produce the news file:

    $ towncrier

## Submissions

Starting with v2.0, aioredis is a running, *asyncio-native port of redis-py*. When
submitting a change, if we find it is the result of an issue with the source
implementation, then we may redirect your change to that library to be triaged and
merged there first.

In general, updates to the high-level Redis client will likely be re-routed via
redis-py, while changes to the lower-level connection client will likely be accepted
directly.

## Code style

Code **must** be pep8 compliant.

You can check it with following command:

    $ make lint

## Running tests

You can run tests in any of the following ways:

    # first install aioredis (must use -e for tests to work)
    $ pip install -e .

    # will run tests in a verbose mode
    $ make test
    # or
    $ pytest

    # or with particular Redis server
    $ pytest --redis-url=redis://localhost:6379/2 tests/errors_test.py

    # will run tests with coverage report
    $ make cov
    # or
    $ pytest --cov

### Different Redis server versions

To run tests against different Redis versions, you must have them installed on your host
machine and running. Then you can pass an explicit url to the tests with `--redis-url`:

    $ pytest --redis-url=redis://localhost:6379/2

### UVLoop

To run tests with uvloop:

    $ pip install uvloop
    $ pytest --uvloop

## Writing tests

aioredis uses pytest.

Tests are located under `/tests` directory.

### Redis Version Tests Helpers

In `tests.conftest` there are `@skip_if_server_version_*` decorators which will
automatically skip tests if the server version doesn't meet the required version
specification. If you're adding support for a new feature of Redis, then this is a good
tool to use.

--8<-- "includes/glossary.md"
