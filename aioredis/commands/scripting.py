import asyncio


class ScriptingCommandsMixin:
    """Set commands mixin.

    For commands details see: http://redis.io/commands#scripting
    """

    @asyncio.coroutine
    def eval(self, script, keys=None, args=None):
        if keys is not None and not isinstance(keys, list):
            raise TypeError("keys argument must be list of redis keys")
        if args is not None and not isinstance(args, list):
            raise TypeError("args argument must be list")
        if keys is not None and any(k is None for k in keys):
            raise TypeError("any of keys must not be None")
        keys = [] if keys is None else keys
        args = [] if args is None else args
        numkeys = len(keys)
        _args = keys + args
        return (yield from self._conn.execute(
            b'EVAL', script, numkeys, *_args))

    @asyncio.coroutine
    def evalsha(self, sha_hash, keys=None, args=None):
        if keys is not None and not isinstance(keys, list):
            raise TypeError("keys argument must be list of redis keys")
        if args is not None and not isinstance(args, list):
            raise TypeError("args argument must be list")
        if keys is not None and any(k is None for k in keys):
            raise TypeError("any of keys must not be None")
        keys = [] if keys is None else keys
        args = [] if args is None else args
        numkeys = len(keys)
        _args = keys + args
        return (yield from self._conn.execute(
            b'EVALSHA', sha_hash, numkeys, *_args))

    @asyncio.coroutine
    def script_exists(self, sha_hash, *sha_hashes):
        return (yield from self._conn.execute(b'SCRIPT', b'EXISTS',
                                              sha_hash, *sha_hashes))

    @asyncio.coroutine
    def script_kill(self):
        return (yield from self._conn.execute(b'SCRIPT', b'KILL'))

    @asyncio.coroutine
    def script_flush(self):
        return (yield from self._conn.execute(b"SCRIPT",  b"FLUSH"))

    @asyncio.coroutine
    def script_load(self, script):
        return (yield from self._conn.execute(b"SCRIPT",  b"LOAD", script))

