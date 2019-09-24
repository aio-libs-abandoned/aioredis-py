import pytest
import logging

from collections import namedtuple

__all__ = [
    'assert_almost_equal',
    'redis_version',
    'logs',
]


def assert_almost_equal(first, second, places=None, msg=None, delta=None):
    assert not (places is None and delta is None), \
        "Both places and delta are not set, please set one"
    if delta is not None:
        assert abs(first - second) <= delta
    else:
        assert round(abs(first - second), places) == 0


def redis_version(*version, reason):
    assert 1 < len(version) <= 3, version
    assert all(isinstance(v, int) for v in version), version
    return pytest.mark.redis_version(version=version, reason=reason)


def logs(logger, level=None):
    """Catches logs for given logger and level.

    See unittest.TestCase.assertLogs for details.
    """
    return _AssertLogsContext(logger, level)


_LoggingWatcher = namedtuple("_LoggingWatcher", ["records", "output"])


class _CapturingHandler(logging.Handler):
    """
    A logging handler capturing all (raw and formatted) logging output.
    """

    def __init__(self):
        logging.Handler.__init__(self)
        self.watcher = _LoggingWatcher([], [])

    def flush(self):
        pass

    def emit(self, record):
        self.watcher.records.append(record)
        msg = self.format(record)
        self.watcher.output.append(msg)


class _AssertLogsContext:
    """Standard unittest's _AssertLogsContext context manager
    adopted to raise pytest failure.
    """
    LOGGING_FORMAT = "%(levelname)s:%(name)s:%(message)s"

    def __init__(self, logger_name, level):
        self.logger_name = logger_name
        if level:
            self.level = level
        else:
            self.level = logging.INFO
        self.msg = None

    def __enter__(self):
        if isinstance(self.logger_name, logging.Logger):
            logger = self.logger = self.logger_name
        else:
            logger = self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter(self.LOGGING_FORMAT)
        handler = _CapturingHandler()
        handler.setFormatter(formatter)
        self.watcher = handler.watcher
        self.old_handlers = logger.handlers[:]
        self.old_level = logger.level
        self.old_propagate = logger.propagate
        logger.handlers = [handler]
        logger.setLevel(self.level)
        logger.propagate = False
        return handler.watcher

    def __exit__(self, exc_type, exc_value, tb):
        self.logger.handlers = self.old_handlers
        self.logger.propagate = self.old_propagate
        self.logger.setLevel(self.old_level)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return False
        if len(self.watcher.records) == 0:
            pytest.fail(
                "no logs of level {} or higher triggered on {}"
                .format(logging.getLevelName(self.level), self.logger.name))
