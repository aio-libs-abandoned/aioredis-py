import asyncio


class ScriptingCommandsMixin:
    """Set commands mixin.

    For commands details see: http://redis.io/commands#scripting
    """

    @asyncio.coroutine
    def eval(self, script, keys=None, args=None):
        """EVAL and EVALSHA are used to evaluate scripts using the Lua
        interpreter built into Redis.

        :raises TypeError: if keys argument is not None or list
        :raises TypeError: if args argument is not None or list
        :raises TypeError: if any of keys is None

        """
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
        """Evaluates a script cached on the server side by its SHA1 digest.
        Scripts are cached on the server side using the scrip_load command.

        :raises ValueError: if sha_hash has wrong length.
        :raises TypeError: if keys argument is not None or list
        :raises TypeError: if args argument is not None or list
        :raises TypeError: if any of keys is None
        """
        if len(sha_hash) != 40:
            raise ValueError('Not valid sha hash, length of sha_hash argument '
                             'should be 40')
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
        """Returns information about the existence of the
        scripts in the script cache.

        :raises ValueError: if sha_hash or any of sha_hashes has wrong length.
        """
        if len(sha_hash) != 40:
            raise ValueError('Not valid sha hash, length of sha_hash argument '
                             'should be 40')
        if any((len(sha) != 40 for sha in sha_hashes)):
            raise ValueError('Each of sha_hashes should have length of 40')
        return (yield from self._conn.execute(b'SCRIPT', b'EXISTS',
                                              sha_hash, *sha_hashes))

    @asyncio.coroutine
    def script_kill(self):
        """Kills the currently executing Lua script, assuming no write
        operation was yet performed by the script.
        """
        return (yield from self._conn.execute(b'SCRIPT', b'KILL'))

    @asyncio.coroutine
    def script_flush(self):
        """Flush the Lua scripts cache."""
        return (yield from self._conn.execute(b"SCRIPT",  b"FLUSH"))

    @asyncio.coroutine
    def script_load(self, script):
        """Load a script into the scripts cache, without executing it."""
        return (yield from self._conn.execute(b"SCRIPT",  b"LOAD", script))
