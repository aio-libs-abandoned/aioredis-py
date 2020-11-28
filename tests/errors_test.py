from aioredis.errors import MaxClientsError, ReplyError


def test_return_default_class():
    assert isinstance(ReplyError(None), ReplyError)


def test_return_adhoc_class():
    class MyError(ReplyError):
        MATCH_REPLY = "my error"

    assert isinstance(ReplyError("my error"), MyError)


def test_return_max_clients_error():
    assert isinstance(ReplyError("ERR max number of clients reached"), MaxClientsError)
