import asyncio
from ..util import wait_convert, PY_35, _ScanIter


class RedisClusterMixin:
    """Redis cluster mixin."""

    @asyncio.coroutine
    def keys(self, *, pattern=None, **kwargs):
        if 'encoding' not in kwargs:
            kwargs['encoding'] = self._encoding
        if pattern is None:
            pattern = '*'
        res = (yield from self.execute(b'KEYS', pattern, many=True, **kwargs))
        return [item for part in res for item in part]

    @asyncio.coroutine
    def scan(self, cursor=0, match=None, count=None):
        """Incrementally iterate the keys space.

        :param match - str
        :param count - int
        :param cursor
        Usage example:

        >>> match = 'key*'
        ... for keys in (yield from cluster.scan(match=match))
        ...     print('Matched:', keys)

        """

        entities = self.get_nodes_entities()
        futures = []
        @asyncio.coroutine
        def scan_corr(ent, cur=cursor):
            """

            :param address - address tuple
            :param cur
            Usage example:
            """
            if not cur:
                cur = b'0'
            ks = []
            while cur:
                fut = (yield from self._execute_node(
                    ent, b'SCAN', cursor=cur, match=match, count=count))
                cur, values = fut
                ks.extend(values)
            return ks

        for entity in entities:
            futures.append(scan_corr(entity, cur=cursor))

        return (yield from asyncio.gather(*futures))

    if PY_35:
        def iscan(self, *, match=None, count=None):
            """Incrementally iterate the keys space using async for.

            :param match - str
            :param count - int
            Usage example:

            >>> async for key in conn.iscan(match='key*'):
            ...     print('Matched:', key)

            """
            return _ScanIter(
                lambda cur: self.scan(cur, match=match, count=count))
