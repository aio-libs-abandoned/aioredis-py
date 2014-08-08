from aioredis.util import wait_ok


class ScriptingCommandsMixin:
    """Set commands mixin.

    For commands details see: http://redis.io/commands#scripting
    """

    def eval(self, script, keys=[], args=[]):
        """Execute a Lua script server side."""
        return self._conn.execute(b'EVAL', script, len(keys), *(keys + args))

    def evalsha(self, digest, keys=[], args=[]):
        """Execute a Lua script server side by its SHA1 digest."""
        return self._conn.execute(
            b'EVALSHA', digest, len(keys), *(keys + args))

    def script_exists(self, digest, *digests):
        """Check existence of scripts in the script cache."""
        return self._conn.execute(b'SCRIPT', b'EXISTS', digest, *digests)

    def script_kill(self):
        """Kill the script currently in execution."""
        fut = self._conn.execute(b'SCRIPT', b'KILL')
        return wait_ok(fut)

    def script_flush(self):
        """Remove all the scripts from the script cache."""
        fut = self._conn.execute(b"SCRIPT",  b"FLUSH")
        return wait_ok(fut)

    def script_load(self, script):
        """Load the specified Lua script into the script cache."""
        return self._conn.execute(b"SCRIPT",  b"LOAD", script)
