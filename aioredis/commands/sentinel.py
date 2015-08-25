import asyncio

from aioredis.util import wait_ok


def nativestr(x):
    return x if isinstance(x, str) else x.decode('utf-8', 'replace')

SENTINEL_STATE_TYPES = {
    'can-failover-its-master': int,
    'config-epoch': int,
    'down-after-milliseconds': int,
    'failover-timeout': int,
    'info-refresh': int,
    'last-hello-message': int,
    'last-ok-ping-reply': int,
    'last-ping-reply': int,
    'last-ping-sent': int,
    'master-link-down-time': int,
    'master-port': int,
    'num-other-sentinels': int,
    'num-slaves': int,
    'o-down-time': int,
    'pending-commands': int,
    'parallel-syncs': int,
    'port': int,
    'quorum': int,
    'role-reported-time': int,
    's-down-time': int,
    'slave-priority': int,
    'slave-repl-offset': int,
    'voted-leader-epoch': int
}


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in zip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result['flags'].split(','))
    for name, flag in (('is_master', 'master'), ('is_slave', 'slave'),
                       ('is_sdown', 's_down'), ('is_odown', 'o_down'),
                       ('is_sentinel', 'sentinel'),
                       ('is_disconnected', 'disconnected'),
                       ('is_master_down', 'master_down')):
        result[name] = flag in flags
    return result


@asyncio.coroutine
def parse_sentinel_masters(fut):
    response = yield from fut
    result = {}
    for item in response:
        state = parse_sentinel_state(map(nativestr, item))
        result[state['name']] = state
    return result


@asyncio.coroutine
def parse_sentinel_slaves_and_sentinels(fut):
    response = yield from fut
    return [parse_sentinel_state(map(nativestr, item)) for item in response]


@asyncio.coroutine
def parse_sentinel_master(fut):
    response = yield from fut
    return parse_sentinel_state(map(nativestr, response))


@asyncio.coroutine
def parse_sentinel_get_master(fut):
    response = yield from fut
    if response in (b'QUEUED', 'QUEUED'):
        return response
    return response and (response[0], int(response[1])) or None


class SentinelCommandsMixin:

    def sentinel_get_master_addr_by_name(self, service_name):
        "Returns a (host, port) pair for the given ``service_name``"
        fut = self._conn.execute(b'SENTINEL', b'GET-MASTER-ADDR-BY-NAME',
                                 service_name)
        return parse_sentinel_get_master(fut)

    def sentinel_master(self, service_name):
        "Returns a dictionary containing the specified masters state."
        fut = self._conn.execute(b'SENTINEL', b'MASTER', service_name)
        return parse_sentinel_master(fut)

    def sentinel_masters(self):
        "Returns a list of dictionaries containing each master's state."
        fut = self._conn.execute(b'SENTINEL', b'MASTERS')
        return parse_sentinel_masters(fut)

    def sentinel_slaves(self, service_name):
        "Returns a list of slaves for ``service_name``"
        fut = self._conn.execute(b'SENTINEL', b'SLAVES', service_name)
        return parse_sentinel_slaves_and_sentinels(fut)

    def sentinel_sentinels(self, service_name):
        "Returns a list of sentinels for ``service_name``"
        fut = self._conn.execute(b'SENTINEL', b'SENTINELS', service_name)
        return parse_sentinel_slaves_and_sentinels(fut)

    def sentinel_monitor(self, name, ip, port, quorum):
        "Add a new master to Sentinel to be monitored"
        fut = self._conn.execute(b'SENTINEL', b'MONITOR', name, ip,
                                 port, quorum)
        return wait_ok(fut)

    def sentinel_remove(self, name):
        "Remove a master from Sentinel's monitoring"
        fut = self._conn.execute(b'SENTINEL', b'REMOVE', name)
        return wait_ok(fut)

    def sentinel_set(self, name, option, value):
        "Set Sentinel monitoring parameters for a given master"
        fut = self._conn.execute(b'SENTINEL', b'SET', name, option, value)
        return wait_ok(fut)

    def sentinel_failover(self, name):
        "Remove a master from Sentinel's monitoring"
        fut = self._conn.execute(b'SENTINEL', b'FAILOVER', name)
        return wait_ok(fut)
