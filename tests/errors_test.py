from aioredis.errors import ReplyError
from aioredis.errors import MaxClientsError


class TestReplyError:

    def test_return_default_class(self):
        assert isinstance(ReplyError(None), ReplyError)

    def test_return_adhoc_class(self):
        class MyError(ReplyError):
            _REPLY = "my error"

        assert isinstance(ReplyError("my error"), MyError)


class TestMaxClientsError:

    def test_return_max_clients_error(self):
        assert isinstance(
            ReplyError("ERR max number of clients reached"),
            MaxClientsError
        )
