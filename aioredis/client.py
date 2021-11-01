import asyncio
import datetime
import hashlib
import inspect
import re
import time as mod_time
import warnings
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
)

from aioredis.compat import Protocol, TypedDict, create_task_or_run, get_event_loop
from aioredis.connection import (
    Connection,
    ConnectionPool,
    EncodableT,
    SSLConnection,
    UnixDomainSocketConnection,
)
from aioredis.exceptions import (
    ConnectionError,
    DataError,
    ExecAbortError,
    ModuleError,
    NoScriptError,
    PubSubError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)
from aioredis.lock import Lock
from aioredis.utils import safe_str, str_if_bytes

AbsExpiryT = Union[int, datetime.datetime]
ExpiryT = Union[int, datetime.timedelta]
ZScoreBoundT = Union[float, str]  # str allows for the [ or ( prefix
BitfieldOffsetT = Union[int, str]  # str allows for #x syntax
_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT  # Main redis key space
PatternT = _StringLikeT  # Patterns matched against keys, fields etc
FieldT = EncodableT  # Fields within hash tables, streams and geo commands
KeysT = Union[KeyT, Sequence[KeyT]]
ChannelT = _StringLikeT
GroupT = _StringLikeT  # Consumer group
ConsumerT = _StringLikeT  # Consumer name
StreamIdT = Union[int, _StringLikeT]
ScriptTextT = _StringLikeT
# Mapping is not covariant in the key type, which prevents
# Mapping[_StringLikeT, X from accepting arguments of type Dict[str, X]. Using
# a TypeVar instead of a Union allows mappings with any of the permitted types
# to be passed. Care is needed if there is more than one such mapping in a
# type signature because they will all be required to be the same key type.
AnyKeyT = TypeVar("AnyKeyT", bytes, str, memoryview)
AnyFieldT = TypeVar("AnyFieldT", bytes, str, memoryview)
AnyChannelT = ChannelT
PubSubHandler = Callable[[Dict[str, str]], None]

SYM_EMPTY = b""
EMPTY_RESPONSE = "EMPTY_RESPONSE"

_KeyT = TypeVar("_KeyT", bound=KeyT)
_ArgT = TypeVar("_ArgT", KeyT, EncodableT)
_RedisT = TypeVar("_RedisT", bound="Redis")
_NormalizeKeysT = TypeVar("_NormalizeKeysT", bound=Mapping[ChannelT, object])


def list_or_args(
    keys: Union[_KeyT, Iterable[_KeyT]], args: Optional[Iterable[_ArgT]]
) -> List[Union[_KeyT, _ArgT]]:
    # returns a single new list combining keys and args
    key_list: List[Union[_KeyT, _ArgT]]
    try:
        iter(keys)  # type: ignore[arg-type]
        keys = cast(Iterable[_KeyT], keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (bytes, str)):
            key_list = [keys]
        else:
            key_list = list(keys)
    except TypeError:
        key_list = [cast(memoryview, keys)]
    if args:
        key_list.extend(args)
    return key_list


def timestamp_to_datetime(response):
    """Converts a unix timestamp to a Python datetime object"""
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


class CaseInsensitiveDict(dict):
    """Case insensitive dict implementation. Assumes string keys only."""

    def __init__(self, data):
        for k, v in data.items():
            self[k.upper()] = v

    def __contains__(self, k):
        return super().__contains__(k.upper())

    def __delitem__(self, k):
        super().__delitem__(k.upper())

    def __getitem__(self, k):
        return super().__getitem__(k.upper())

    def get(self, k, default=None):
        return super().get(k.upper(), default)

    def __setitem__(self, k, v):
        super().__setitem__(k.upper(), v)

    def update(self, data):
        data = CaseInsensitiveDict(data)
        super().update(data)


def parse_debug_object(response):
    """Parse the results of Redis's DEBUG OBJECT command into a Python dict"""
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = str_if_bytes(response)
    response = "type:" + response
    response = dict(kv.split(":") for kv in response.split())

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ("refcount", "serializedlength", "lru", "lru_seconds_idle")
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def parse_object(response, infotype):
    """Parse the results of an OBJECT command"""
    if infotype in ("idletime", "refcount"):
        return int_or_none(response)
    return response


def parse_info(response):
    """Parse the result of Redis's INFO command into a Python dict"""
    info: Dict[str, Any] = {}
    response = str_if_bytes(response)

    def get_value(value):
        if "," not in value or "=" not in value:
            try:
                if "." in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}
            for item in value.split(","):
                k, v = item.rsplit("=", 1)
                sub_dict[k] = get_value(v)
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith("#"):
            if line.find(":") != -1:
                # Split, the info fields keys and values.
                # Note that the value may contain ':'. but the 'host:'
                # pseudo-command is the only case where the key contains ':'
                key, value = line.split(":", 1)
                if key == "cmdstat_host":
                    key, value = line.rsplit(":", 1)

                if key == "module":
                    # Hardcode a list for key 'modules' since there could be
                    # multiple lines that started with 'module'
                    info.setdefault("modules", []).append(get_value(value))
                else:
                    info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault("__raw__", []).append(line)

    return info


def parse_memory_stats(response, **kwargs):
    """Parse the results of MEMORY STATS"""
    stats = pairs_to_dict(response, decode_keys=True, decode_string_values=True)
    for key, value in stats.items():
        if key.startswith("db."):
            stats[key] = pairs_to_dict(
                value, decode_keys=True, decode_string_values=True
            )
    return stats


SENTINEL_STATE_TYPES = {
    "can-failover-its-master": int,
    "config-epoch": int,
    "down-after-milliseconds": int,
    "failover-timeout": int,
    "info-refresh": int,
    "last-hello-message": int,
    "last-ok-ping-reply": int,
    "last-ping-reply": int,
    "last-ping-sent": int,
    "master-link-down-time": int,
    "master-port": int,
    "num-other-sentinels": int,
    "num-slaves": int,
    "o-down-time": int,
    "pending-commands": int,
    "parallel-syncs": int,
    "port": int,
    "quorum": int,
    "role-reported-time": int,
    "s-down-time": int,
    "slave-priority": int,
    "slave-repl-offset": int,
    "voted-leader-epoch": int,
}


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result["flags"].split(","))
    for name, flag in (
        ("is_master", "master"),
        ("is_slave", "slave"),
        ("is_sdown", "s_down"),
        ("is_odown", "o_down"),
        ("is_sentinel", "sentinel"),
        ("is_disconnected", "disconnected"),
        ("is_master_down", "master_down"),
    ):
        result[name] = flag in flags
    return result


def parse_sentinel_master(response):
    return parse_sentinel_state(map(str_if_bytes, response))


def parse_sentinel_masters(response):
    result = {}
    for item in response:
        state = parse_sentinel_state(map(str_if_bytes, item))
        result[state["name"]] = state
    return result


def parse_sentinel_slaves_and_sentinels(response):
    return [parse_sentinel_state(map(str_if_bytes, item)) for item in response]


def parse_sentinel_get_master(response):
    return response and (response[0], int(response[1])) or None


def pairs_to_dict(response, decode_keys=False, decode_string_values=False):
    """Create a dict given a list of key/value pairs"""
    if response is None:
        return {}
    if decode_keys or decode_string_values:
        # the iter form is faster, but I don't know how to make that work
        # with a str_if_bytes() map
        keys = response[::2]
        if decode_keys:
            keys = map(str_if_bytes, keys)
        values = response[1::2]
        if decode_string_values:
            values = map(str_if_bytes, values)
        return dict(zip(keys, values))
    else:
        it = iter(response)
        return dict(zip(it, it))


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in zip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except Exception:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options.get("withscores"):
        return response
    score_cast_func = options.get("score_cast_func", float)
    it = iter(response)
    return list(zip(it, map(score_cast_func, it)))


def sort_return_tuples(response, **options):
    """
    If ``groups`` is specified, return the response as a list of
    n-element tuples with n being the value found in options['groups']
    """
    if not response or not options.get("groups"):
        return response
    n = options["groups"]
    return list(zip(*(response[i::n] for i in range(n))))


def int_or_none(response):
    if response is None:
        return None
    return int(response)


def parse_stream_list(response):
    if response is None:
        return None
    data = []
    for r in response:
        if r is not None:
            data.append((r[0], pairs_to_dict(r[1])))
        else:
            data.append((None, None))
    return data


def pairs_to_dict_with_str_keys(response):
    return pairs_to_dict(response, decode_keys=True)


def parse_list_of_dicts(response):
    return list(map(pairs_to_dict_with_str_keys, response))


def parse_xclaim(response, **options):
    if options.get("parse_justid", False):
        return response
    return parse_stream_list(response)


def parse_xinfo_stream(response):
    data = pairs_to_dict(response, decode_keys=True)
    first = data["first-entry"]
    if first is not None:
        data["first-entry"] = (first[0], pairs_to_dict(first[1]))
    last = data["last-entry"]
    if last is not None:
        data["last-entry"] = (last[0], pairs_to_dict(last[1]))
    return data


def parse_xread(response):
    if response is None:
        return []
    return [[r[0], parse_stream_list(r[1])] for r in response]


def parse_xpending(response, **options):
    if options.get("parse_detail", False):
        return parse_xpending_range(response)
    consumers = [{"name": n, "pending": int(p)} for n, p in response[3] or []]
    return {
        "pending": response[0],
        "min": response[1],
        "max": response[2],
        "consumers": consumers,
    }


def parse_xpending_range(response):
    k = ("message_id", "consumer", "time_since_delivered", "times_delivered")
    return [dict(zip(k, r)) for r in response]


def float_or_none(response):
    if response is None:
        return None
    return float(response)


def bool_ok(response):
    return str_if_bytes(response) == "OK"


def parse_zadd(response, **options):
    if response is None:
        return None
    if options.get("as_score"):
        return float(response)
    return int(response)


def parse_client_list(response, **options):
    clients = []
    for c in str_if_bytes(response).splitlines():
        # Values might contain '='
        clients.append(dict(pair.split("=", 1) for pair in c.split(" ")))
    return clients


def parse_config_get(response, **options):
    response = [str_if_bytes(i) if i is not None else None for i in response]
    return response and pairs_to_dict(response) or {}


def parse_scan(response, **options):
    cursor, r = response
    return int(cursor), r


def parse_hscan(response, **options):
    cursor, r = response
    return int(cursor), r and pairs_to_dict(r) or {}


def parse_zscan(response, **options):
    score_cast_func = options.get("score_cast_func", float)
    cursor, r = response
    it = iter(r)
    return int(cursor), list(zip(it, map(score_cast_func, it)))


def parse_slowlog_get(response, **options):
    space: Union[str, bytes] = " " if options.get("decode_responses", False) else b" "
    return [
        {
            "id": item[0],
            "start_time": int(item[1]),
            "duration": int(item[2]),
            # Redis Enterprise injects another entry at index [3], which has
            # the complexity info (i.e. the value N in case the command has
            # an O(N) complexity) instead of the command.
            "command": (
                space.join(item[3])
                if isinstance(item[3], list)
                else space.join(item[4])
            ),
        }
        for item in response
    ]


def parse_cluster_info(response, **options):
    response = str_if_bytes(response)
    return dict(line.split(":") for line in response.splitlines() if line)


def _parse_node_line(line):
    line_items = line.split(" ")
    node_id, addr, flags, master_id, ping, pong, epoch, connected = line.split(" ")[:8]
    slots = [sl.split("-") for sl in line_items[8:]]
    node_dict = {
        "node_id": node_id,
        "flags": flags,
        "master_id": master_id,
        "last_ping_sent": ping,
        "last_pong_rcvd": pong,
        "epoch": epoch,
        "slots": slots,
        "connected": True if connected == "connected" else False,
    }
    return addr, node_dict


def parse_cluster_nodes(response, **options):
    raw_lines = str_if_bytes(response).splitlines()
    return dict(_parse_node_line(line) for line in raw_lines)


def parse_georadius_generic(response, **options):
    if options["store"] or options["store_dist"]:
        # `store` and `store_diff` cant be combined
        # with other command arguments.
        return response

    if type(response) != list:
        response_list = [response]
    else:
        response_list = response

    if not options["withdist"] and not options["withcoord"] and not options["withhash"]:
        # just a bunch of places
        return response_list

    cast: Dict[str, Callable] = {
        "withdist": float,
        "withcoord": lambda ll: (float(ll[0]), float(ll[1])),
        "withhash": int,
    }

    # zip all output results with each casting functino to get
    # the properly native Python value.
    f = [lambda x: x]
    f += [cast[o] for o in ["withdist", "withhash", "withcoord"] if options[o]]
    return [list(map(lambda fv: fv[0](fv[1]), zip(f, r))) for r in response_list]


def parse_pubsub_numsub(response, **options):
    return list(zip(response[0::2], response[1::2]))


def parse_client_kill(response, **options):
    if isinstance(response, int):
        return response
    return str_if_bytes(response) == "OK"


def parse_acl_getuser(response, **options):
    if response is None:
        return None
    data = pairs_to_dict(response, decode_keys=True)

    # convert everything but user-defined data in 'keys' to native strings
    data["flags"] = list(map(str_if_bytes, data["flags"]))
    data["passwords"] = list(map(str_if_bytes, data["passwords"]))
    data["commands"] = str_if_bytes(data["commands"])

    # split 'commands' into separate 'categories' and 'commands' lists
    commands, categories = [], []
    for command in data["commands"].split(" "):
        if "@" in command:
            categories.append(command)
        else:
            commands.append(command)

    data["commands"] = commands
    data["categories"] = categories
    data["enabled"] = "on" in data["flags"]
    return data


def parse_acl_log(response, **options):
    if response is None:
        return None
    if isinstance(response, list):
        data = []
        for log in response:
            log_data = pairs_to_dict(log, True, True)
            client_info = log_data.get("client-info", "")
            log_data["client-info"] = parse_client_info(client_info)

            # float() is lossy comparing to the "double" in C
            log_data["age-seconds"] = float(log_data["age-seconds"])
            data.append(log_data)
    else:
        data = bool_ok(response)
    return data


def parse_client_info(value):
    """
    Parsing client-info in ACL Log in following format.
    "key1=value1 key2=value2 key3=value3"
    """
    client_info = {}
    infos = value.split(" ")
    for info in infos:
        key, value = info.split("=")
        client_info[key] = value

    # Those fields are definded as int in networking.c
    for int_key in {
        "id",
        "age",
        "idle",
        "db",
        "sub",
        "psub",
        "multi",
        "qbuf",
        "qbuf-free",
        "obl",
        "oll",
        "omem",
    }:
        client_info[int_key] = int(client_info[int_key])
    return client_info


def parse_module_result(response):
    if isinstance(response, ModuleError):
        raise response
    return True


class ResponseCallbackProtocol(Protocol):
    def __call__(self, response: Any, **kwargs):
        ...


class AsyncResponseCallbackProtocol(Protocol):
    async def __call__(self, response: Any, **kwargs):
        ...


ResponseCallbackT = Union[ResponseCallbackProtocol, AsyncResponseCallbackProtocol]


_R = TypeVar("_R")


class Redis:
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """

    RESPONSE_CALLBACKS = {
        **string_keys_to_dict(
            "AUTH EXPIRE EXPIREAT HEXISTS HMSET MOVE MSETNX PERSIST "
            "PSETEX RENAMENX SISMEMBER SMOVE SETEX SETNX",
            bool,
        ),
        **string_keys_to_dict(
            "BITCOUNT BITPOS DECRBY DEL EXISTS GEOADD GETBIT HDEL HLEN "
            "HSTRLEN INCRBY LINSERT LLEN LPUSHX PFADD PFCOUNT RPUSHX SADD "
            "SCARD SDIFFSTORE SETBIT SETRANGE SINTERSTORE SREM STRLEN "
            "SUNIONSTORE UNLINK XACK XDEL XLEN XTRIM ZCARD ZLEXCOUNT ZREM "
            "ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE",
            int,
        ),
        **string_keys_to_dict("INCRBYFLOAT HINCRBYFLOAT", float),
        **string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            "LPUSH RPUSH",
            lambda r: isinstance(r, int) and r or str_if_bytes(r) == "OK",
        ),
        **string_keys_to_dict("SORT", sort_return_tuples),
        **string_keys_to_dict("ZSCORE ZINCRBY GEODIST", float_or_none),
        **string_keys_to_dict(
            "FLUSHALL FLUSHDB LSET LTRIM MSET PFMERGE READONLY READWRITE "
            "RENAME SAVE SELECT SHUTDOWN SLAVEOF SWAPDB WATCH UNWATCH ",
            bool_ok,
        ),
        **string_keys_to_dict("BLPOP BRPOP", lambda r: r and tuple(r) or None),
        **string_keys_to_dict(
            "SDIFF SINTER SMEMBERS SUNION", lambda r: r and set(r) or set()
        ),
        **string_keys_to_dict(
            "ZPOPMAX ZPOPMIN ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE",
            zset_score_pairs,
        ),
        **string_keys_to_dict(
            "BZPOPMIN BZPOPMAX", lambda r: r and (r[0], r[1], float(r[2])) or None
        ),
        **string_keys_to_dict("ZRANK ZREVRANK", int_or_none),
        **string_keys_to_dict("XREVRANGE XRANGE", parse_stream_list),
        **string_keys_to_dict("XREAD XREADGROUP", parse_xread),
        **string_keys_to_dict("BGREWRITEAOF BGSAVE", lambda r: True),
        "ACL CAT": lambda r: list(map(str_if_bytes, r)),
        "ACL DELUSER": int,
        "ACL GENPASS": str_if_bytes,
        "ACL GETUSER": parse_acl_getuser,
        "ACL LIST": lambda r: list(map(str_if_bytes, r)),
        "ACL LOAD": bool_ok,
        "ACL LOG": parse_acl_log,
        "ACL SAVE": bool_ok,
        "ACL SETUSER": bool_ok,
        "ACL USERS": lambda r: list(map(str_if_bytes, r)),
        "ACL WHOAMI": str_if_bytes,
        "CLIENT GETNAME": str_if_bytes,
        "CLIENT ID": int,
        "CLIENT KILL": parse_client_kill,
        "CLIENT LIST": parse_client_list,
        "CLIENT SETNAME": bool_ok,
        "CLIENT UNBLOCK": lambda r: r and int(r) == 1 or False,
        "CLIENT PAUSE": bool_ok,
        "CLUSTER ADDSLOTS": bool_ok,
        "CLUSTER COUNT-FAILURE-REPORTS": lambda x: int(x),
        "CLUSTER COUNTKEYSINSLOT": lambda x: int(x),
        "CLUSTER DELSLOTS": bool_ok,
        "CLUSTER FAILOVER": bool_ok,
        "CLUSTER FORGET": bool_ok,
        "CLUSTER INFO": parse_cluster_info,
        "CLUSTER KEYSLOT": lambda x: int(x),
        "CLUSTER MEET": bool_ok,
        "CLUSTER NODES": parse_cluster_nodes,
        "CLUSTER REPLICATE": bool_ok,
        "CLUSTER RESET": bool_ok,
        "CLUSTER SAVECONFIG": bool_ok,
        "CLUSTER SET-CONFIG-EPOCH": bool_ok,
        "CLUSTER SETSLOT": bool_ok,
        "CLUSTER SLAVES": parse_cluster_nodes,
        "CONFIG GET": parse_config_get,
        "CONFIG RESETSTAT": bool_ok,
        "CONFIG SET": bool_ok,
        "DEBUG OBJECT": parse_debug_object,
        "GEOHASH": lambda r: list(map(str_if_bytes, r)),
        "GEOPOS": lambda r: list(
            map(lambda ll: (float(ll[0]), float(ll[1])) if ll is not None else None, r)
        ),
        "GEORADIUS": parse_georadius_generic,
        "GEORADIUSBYMEMBER": parse_georadius_generic,
        "HGETALL": lambda r: r and pairs_to_dict(r) or {},
        "HSCAN": parse_hscan,
        "INFO": parse_info,
        "LASTSAVE": timestamp_to_datetime,
        "MEMORY PURGE": bool_ok,
        "MEMORY STATS": parse_memory_stats,
        "MEMORY USAGE": int_or_none,
        "MODULE LOAD": parse_module_result,
        "MODULE UNLOAD": parse_module_result,
        "MODULE LIST": lambda r: [pairs_to_dict(m) for m in r],
        "OBJECT": parse_object,
        "PING": lambda r: str_if_bytes(r) == "PONG",
        "PUBSUB NUMSUB": parse_pubsub_numsub,
        "RANDOMKEY": lambda r: r and r or None,
        "SCAN": parse_scan,
        "SCRIPT EXISTS": lambda r: list(map(bool, r)),
        "SCRIPT FLUSH": bool_ok,
        "SCRIPT KILL": bool_ok,
        "SCRIPT LOAD": str_if_bytes,
        "SENTINEL GET-MASTER-ADDR-BY-NAME": parse_sentinel_get_master,
        "SENTINEL MASTER": parse_sentinel_master,
        "SENTINEL MASTERS": parse_sentinel_masters,
        "SENTINEL MONITOR": bool_ok,
        "SENTINEL REMOVE": bool_ok,
        "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels,
        "SENTINEL SET": bool_ok,
        "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels,
        "SET": lambda r: r and str_if_bytes(r) == "OK",
        "SLOWLOG GET": parse_slowlog_get,
        "SLOWLOG LEN": int,
        "SLOWLOG RESET": bool_ok,
        "SSCAN": parse_scan,
        "TIME": lambda x: (int(x[0]), int(x[1])),
        "XCLAIM": parse_xclaim,
        "XGROUP CREATE": bool_ok,
        "XGROUP DELCONSUMER": int,
        "XGROUP DESTROY": bool,
        "XGROUP SETID": bool_ok,
        "XINFO CONSUMERS": parse_list_of_dicts,
        "XINFO GROUPS": parse_list_of_dicts,
        "XINFO STREAM": parse_xinfo_stream,
        "XPENDING": parse_xpending,
        "ZADD": parse_zadd,
        "ZSCAN": parse_zscan,
    }

    response_callbacks: MutableMapping[Union[str, bytes], ResponseCallbackT]

    @classmethod
    def from_url(cls, url: str, **kwargs):
        """
        Return a Redis client object configured from the given URL

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: Union[str, int] = 0,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[str, Any]] = None,
        connection_pool: Optional[ConnectionPool] = None,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        ssl: bool = False,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: Optional[str] = None,
        ssl_check_hostname: bool = False,
        max_connections: Optional[int] = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: Optional[str] = None,
        username: Optional[str] = None,
    ):
        kwargs: Dict[str, Any]
        if not connection_pool:
            kwargs = {
                "db": db,
                "username": username,
                "password": password,
                "socket_timeout": socket_timeout,
                "encoding": encoding,
                "encoding_errors": encoding_errors,
                "decode_responses": decode_responses,
                "retry_on_timeout": retry_on_timeout,
                "max_connections": max_connections,
                "health_check_interval": health_check_interval,
                "client_name": client_name,
            }
            # based on input, setup appropriate connection args
            if unix_socket_path is not None:
                kwargs.update(
                    {
                        "path": unix_socket_path,
                        "connection_class": UnixDomainSocketConnection,
                    }
                )
            else:
                # TCP specific options
                kwargs.update(
                    {
                        "host": host,
                        "port": port,
                        "socket_connect_timeout": socket_connect_timeout,
                        "socket_keepalive": socket_keepalive,
                        "socket_keepalive_options": socket_keepalive_options,
                    }
                )

                if ssl:
                    kwargs.update(
                        {
                            "connection_class": SSLConnection,
                            "ssl_keyfile": ssl_keyfile,
                            "ssl_certfile": ssl_certfile,
                            "ssl_cert_reqs": ssl_cert_reqs,
                            "ssl_ca_certs": ssl_ca_certs,
                            "ssl_check_hostname": ssl_check_hostname,
                        }
                    )
            connection_pool = ConnectionPool(**kwargs)
        self.connection_pool = connection_pool
        self.single_connection_client = single_connection_client
        self.connection: Optional[Connection] = None

        self.response_callbacks = CaseInsensitiveDict(self.__class__.RESPONSE_CALLBACKS)

    def __repr__(self):
        return f"{self.__class__.__name__}<{self.connection_pool!r}>"

    def __await__(self):
        return self.initialize().__await__()

    async def initialize(self: _RedisT) -> _RedisT:
        if self.single_connection_client and self.connection is None:
            self.connection = await self.connection_pool.get_connection("_")
        return self

    def set_response_callback(self, command: str, callback: ResponseCallbackT):
        """Set a custom Response Callback"""
        self.response_callbacks[command] = callback

    def pipeline(
        self, transaction: bool = True, shard_hint: Optional[str] = None
    ) -> "Pipeline":
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )

    async def transaction(
        self,
        func: Callable[["Pipeline"], Union[Any, Awaitable[Any]]],
        *watches: KeyT,
        shard_hint: Optional[str] = None,
        value_from_callable: bool = False,
        watch_delay: Optional[float] = None,
    ):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        pipe: Pipeline
        async with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        await pipe.watch(*watches)
                    func_value = func(pipe)
                    if inspect.isawaitable(func_value):
                        func_value = await func_value
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue

    def lock(
        self,
        name: KeyT,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking_timeout: Optional[float] = None,
        lock_class: Optional[Type[Lock]] = None,
        thread_local=True,
    ) -> Lock:
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage."""
        if lock_class is None:
            lock_class = Lock
        return lock_class(
            self,
            name,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def pubsub(self, **kwargs) -> "PubSub":
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        return PubSub(self.connection_pool, **kwargs)

    def monitor(self) -> "Monitor":
        return Monitor(self.connection_pool)

    def client(self) -> "Redis":
        return self.__class__(
            connection_pool=self.connection_pool, single_connection_client=True
        )

    async def __aenter__(self: _RedisT) -> _RedisT:
        return await self.initialize()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def __del__(self):
        try:
            create_task_or_run(self.close())
        except Exception:
            pass

    async def close(self):
        conn = self.connection
        if conn:
            self.connection = None
            await self.connection_pool.release(conn)

    # COMMAND EXECUTION AND PROTOCOL PARSING
    async def execute_command(self, *args, **options):
        """Execute a command and return a parsed response"""
        await self.initialize()
        pool = self.connection_pool
        command_name = args[0]
        conn = self.connection or await pool.get_connection(command_name, **options)
        try:
            await conn.send_command(*args)
            return await self.parse_response(conn, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            await conn.disconnect()
            if not (conn.retry_on_timeout and isinstance(e, TimeoutError)):
                raise
            await conn.send_command(*args)
            return await self.parse_response(conn, command_name, **options)
        finally:
            if not self.connection:
                await pool.release(conn)

    async def parse_response(
        self, connection: Connection, command_name: Union[str, bytes], **options
    ):
        """Parses a response from the Redis server"""
        try:
            response = await connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in options:
                return options[EMPTY_RESPONSE]
            raise
        if command_name in self.response_callbacks:
            # Mypy bug: https://github.com/python/mypy/issues/10977
            command_name = cast(str, command_name)
            retval = self.response_callbacks[command_name](response, **options)
            return await retval if inspect.isawaitable(retval) else retval
        return response

    # SERVER INFORMATION

    # ACL methods
    def acl_cat(self, category: Optional[str] = None) -> Awaitable:
        """
        Returns a list of categories or commands within a category.

        If ``category`` is not supplied, returns a list of all categories.
        If ``category`` is supplied, returns a list of all commands within
        that category.
        """
        pieces: List[EncodableT] = [category] if category else []
        return self.execute_command("ACL CAT", *pieces)

    def acl_deluser(self, username: str) -> Awaitable:
        """Delete the ACL for the specified ``username``"""
        return self.execute_command("ACL DELUSER", username)

    def acl_genpass(self) -> Awaitable:
        """Generate a random password value"""
        return self.execute_command("ACL GENPASS")

    def acl_getuser(self, username: str) -> Awaitable:
        """
        Get the ACL details for the specified ``username``.

        If ``username`` does not exist, return None
        """
        return self.execute_command("ACL GETUSER", username)

    def acl_list(self) -> Awaitable:
        """Return a list of all ACLs on the server"""
        return self.execute_command("ACL LIST")

    def acl_log(self, count: Optional[int] = None) -> Awaitable:
        """
        Get ACL logs as a list.
        :param int count: Get logs[0:count].
        :rtype: List.
        """
        args = []
        if count is not None:
            if not isinstance(count, int):
                raise DataError("ACL LOG count must be an integer")
            args.append(count)

        return self.execute_command("ACL LOG", *args)

    def acl_log_reset(self) -> Awaitable:
        """
        Reset ACL logs.
        :rtype: Boolean.
        """
        args = [b"RESET"]
        return self.execute_command("ACL LOG", *args)

    def acl_load(self) -> Awaitable:
        """
        Load ACL rules from the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to load ACL rules from an aclfile.
        """
        return self.execute_command("ACL LOAD")

    def acl_save(self) -> Awaitable:
        """
        Save ACL rules to the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to save ACL rules to an aclfile.
        """
        return self.execute_command("ACL SAVE")

    def acl_setuser(  # noqa: C901
        self,
        username: str,
        enabled: bool = False,
        nopass: bool = False,
        passwords: Optional[Union[str, Iterable[str]]] = None,
        hashed_passwords: Optional[Union[str, Iterable[str]]] = None,
        categories: Optional[Iterable[str]] = None,
        commands: Optional[Iterable[str]] = None,
        keys: Optional[Iterable[KeyT]] = None,
        reset: bool = False,
        reset_keys: bool = False,
        reset_passwords: bool = False,
    ) -> Awaitable:
        """
        Create or update an ACL user.

        Create or update the ACL for ``username``. If the user already exists,
        the existing ACL is completely overwritten and replaced with the
        specified values.

        ``enabled`` is a boolean indicating whether the user should be allowed
        to authenticate or not. Defaults to ``False``.

        ``nopass`` is a boolean indicating whether the can authenticate without
        a password. This cannot be True if ``passwords`` are also specified.

        ``passwords`` if specified is a list of plain text passwords
        to add to or remove from the user. Each password must be prefixed with
        a '+' to add or a '-' to remove. For convenience, the value of
        ``passwords`` can be a simple prefixed string when adding or
        removing a single password.

        ``hashed_passwords`` if specified is a list of SHA-256 hashed passwords
        to add to or remove from the user. Each hashed password must be
        prefixed with a '+' to add or a '-' to remove. For convenience,
        the value of ``hashed_passwords`` can be a simple prefixed string when
        adding or removing a single password.

        ``categories`` if specified is a list of strings representing category
        permissions. Each string must be prefixed with either a '+' to add the
        category permission or a '-' to remove the category permission.

        ``commands`` if specified is a list of strings representing command
        permissions. Each string must be prefixed with either a '+' to add the
        command permission or a '-' to remove the command permission.

        ``keys`` if specified is a list of key patterns to grant the user
        access to. Keys patterns allow '*' to support wildcard matching. For
        example, '*' grants access to all keys while 'cache:*' grants access
        to all keys that are prefixed with 'cache:'. ``keys`` should not be
        prefixed with a '~'.

        ``reset`` is a boolean indicating whether the user should be fully
        reset prior to applying the new ACL. Setting this to True will
        remove all existing passwords, flags and privileges from the user and
        then apply the specified rules. If this is False, the user's existing
        passwords, flags and privileges will be kept and any new specified
        rules will be applied on top.

        ``reset_keys`` is a boolean indicating whether the user's key
        permissions should be reset prior to applying any new key permissions
        specified in ``keys``. If this is False, the user's existing
        key permissions will be kept and any new specified key permissions
        will be applied on top.

        ``reset_passwords`` is a boolean indicating whether to remove all
        existing passwords and the 'nopass' flag from the user prior to
        applying any new passwords specified in 'passwords' or
        'hashed_passwords'. If this is False, the user's existing passwords
        and 'nopass' status will be kept and any new specified passwords
        or hashed_passwords will be applied on top.
        """
        encoder = self.connection_pool.get_encoder()
        pieces: List[Union[str, bytes]] = [username]

        if reset:
            pieces.append(b"reset")

        if reset_keys:
            pieces.append(b"resetkeys")

        if reset_passwords:
            pieces.append(b"resetpass")

        if enabled:
            pieces.append(b"on")
        else:
            pieces.append(b"off")

        if (passwords or hashed_passwords) and nopass:
            raise DataError(
                "Cannot set 'nopass' and supply " "'passwords' or 'hashed_passwords'"
            )

        if passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            converted_passwords = list_or_args(passwords, [])
            for i, raw_password in enumerate(converted_passwords):
                password = encoder.encode(raw_password)
                if password.startswith(b"+"):
                    pieces.append(b">%s" % password[1:])
                elif password.startswith(b"-"):
                    pieces.append(b"<%s" % password[1:])
                else:
                    raise DataError(
                        "Password %d must be prefixeed with a "
                        '"+" to add or a "-" to remove' % i
                    )

        if hashed_passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            parsed_hashed_passwords = list_or_args(hashed_passwords, [])
            for i, raw_hashed_password in enumerate(parsed_hashed_passwords):
                hashed_password = encoder.encode(raw_hashed_password)
                if hashed_password.startswith(b"+"):
                    pieces.append(b"#%s" % hashed_password[1:])
                elif hashed_password.startswith(b"-"):
                    pieces.append(b"!%s" % hashed_password[1:])
                else:
                    raise DataError(
                        "Hashed %d password must be prefixeed "
                        'with a "+" to add or a "-" to remove' % i
                    )

        if nopass:
            pieces.append(b"nopass")

        if categories:
            for raw_category in categories:
                category = encoder.encode(raw_category)
                # categories can be prefixed with one of (+@, +, -@, -)
                if category.startswith(b"+@"):
                    pieces.append(category)
                elif category.startswith(b"+"):
                    pieces.append(b"+@%s" % category[1:])
                elif category.startswith(b"-@"):
                    pieces.append(category)
                elif category.startswith(b"-"):
                    pieces.append(b"-@%s" % category[1:])
                else:
                    raise DataError(
                        f'Category "{encoder.decode(category, force=True)}" must be '
                        'prefixed with "+" or "-"'
                    )
        if commands:
            for raw_cmd in commands:
                cmd = encoder.encode(raw_cmd)
                if not cmd.startswith(b"+") and not cmd.startswith(b"-"):
                    raise DataError(
                        f'Command "{encoder.decode(cmd, force=True)}" must be '
                        'prefixed with "+" or "-"'
                    )
                pieces.append(cmd)

        if keys:
            for raw_key in keys:
                key = encoder.encode(raw_key)
                pieces.append(b"~%s" % key)

        return self.execute_command("ACL SETUSER", *pieces)

    def acl_users(self) -> Awaitable:
        """Returns a list of all registered users on the server."""
        return self.execute_command("ACL USERS")

    def acl_whoami(self) -> Awaitable:
        """Get the username for the current connection"""
        return self.execute_command("ACL WHOAMI")

    def bgrewriteaof(self) -> Awaitable:
        """Tell the Redis server to rewrite the AOF file from data in memory."""
        return self.execute_command("BGREWRITEAOF")

    def bgsave(self) -> Awaitable:
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        return self.execute_command("BGSAVE")

    def client_kill(self, address: str) -> Awaitable:
        """Disconnects the client at ``address`` (ip:port)"""
        return self.execute_command("CLIENT KILL", address)

    def client_kill_filter(
        self,
        _id: Optional[str] = None,
        _type: Optional[str] = None,
        addr: Optional[str] = None,
        skipme: Optional[bool] = None,
    ) -> Awaitable:
        """
        Disconnects client(s) using a variety of filter options
        :param _id: Kills a client by its unique ID field
        :param _type: Kills a client by type where type is one of 'normal',
        'master', 'slave' or 'pubsub'
        :param addr: Kills a client by its 'address:port'
        :param skipme: If True, then the client calling the command
        will not get killed even if it is identified by one of the filter
        options. If skipme is not provided, the server defaults to skipme=True
        """
        args: List[Union[bytes, str]] = []
        if _type is not None:
            client_types = ("normal", "master", "slave", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT KILL type must be one of {client_types!r}")
            args.extend((b"TYPE", _type))
        if skipme is not None:
            if not isinstance(skipme, bool):
                raise DataError("CLIENT KILL skipme must be a bool")
            if skipme:
                args.extend((b"SKIPME", b"YES"))
            else:
                args.extend((b"SKIPME", b"NO"))
        if _id is not None:
            args.extend((b"ID", _id))
        if addr is not None:
            args.extend((b"ADDR", addr))
        if not args:
            raise DataError(
                "CLIENT KILL <filter> <value> ... ... <filter> "
                "<value> must specify at least one filter"
            )
        return self.execute_command("CLIENT KILL", *args)

    def client_list(self, _type: Optional[str] = None) -> Awaitable:
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        """
        "Returns a list of currently connected clients"
        if _type is not None:
            client_types = ("normal", "master", "replica", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT LIST _type must be one of {client_types!r}")
            return self.execute_command("CLIENT LIST", b"TYPE", _type)
        return self.execute_command("CLIENT LIST")

    def client_getname(self) -> Awaitable:
        """Returns the current connection name"""
        return self.execute_command("CLIENT GETNAME")

    def client_id(self) -> Awaitable:
        """Returns the current connection id"""
        return self.execute_command("CLIENT ID")

    def client_setname(self, name: str) -> Awaitable:
        """Sets the current connection name"""
        return self.execute_command("CLIENT SETNAME", name)

    def client_unblock(self, client_id: int, error: bool = False) -> Awaitable:
        """
        Unblocks a connection by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.
        """
        args = ["CLIENT UNBLOCK", int(client_id)]
        if error:
            args.append(b"ERROR")
        return self.execute_command(*args)

    def client_pause(self, timeout: int) -> Awaitable:
        """
        Suspend all the Redis clients for the specified amount of time
        :param timeout: milliseconds to pause clients
        """
        if not isinstance(timeout, int):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        return self.execute_command("CLIENT PAUSE", str(timeout))

    def readwrite(self) -> Awaitable:
        """Disables read queries for a connection to a Redis Cluster slave node"""
        return self.execute_command("READWRITE")

    def readonly(self) -> Awaitable:
        """Enables read queries for a connection to a Redis Cluster replica node"""
        return self.execute_command("READONLY")

    def config_get(self, pattern: str = "*") -> Awaitable:
        """Return a dictionary of configuration based on the ``pattern``"""
        return self.execute_command("CONFIG GET", pattern)

    def config_set(self, name: str, value: EncodableT) -> Awaitable:
        """Set config item ``name`` with ``value``"""
        return self.execute_command("CONFIG SET", name, value)

    def config_resetstat(self) -> Awaitable:
        """Reset runtime statistics"""
        return self.execute_command("CONFIG RESETSTAT")

    def config_rewrite(self) -> Awaitable:
        """Rewrite config file with the minimal change to reflect running config"""
        return self.execute_command("CONFIG REWRITE")

    def dbsize(self) -> Awaitable:
        """Returns the number of keys in the current database"""
        return self.execute_command("DBSIZE")

    def debug_object(self, key: KeyT) -> Awaitable:
        """Returns version specific meta information about a given key"""
        return self.execute_command("DEBUG OBJECT", key)

    def echo(self, value: EncodableT) -> Awaitable:
        """Echo the string back from the server"""
        return self.execute_command("ECHO", value)

    def flushall(self, asynchronous: bool = False) -> Awaitable:
        """
        Delete all keys in all databases on the current host.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.
        """
        args = []
        if asynchronous:
            args.append(b"ASYNC")
        return self.execute_command("FLUSHALL", *args)

    def flushdb(self, asynchronous: bool = False) -> Awaitable:
        """
        Delete all keys in the current database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.
        """
        args = []
        if asynchronous:
            args.append(b"ASYNC")
        return self.execute_command("FLUSHDB", *args)

    def swapdb(self, first: int, second: int) -> Awaitable:
        """Swap two databases"""
        return self.execute_command("SWAPDB", first, second)

    def info(self, section: Optional[str] = None) -> Awaitable:
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        if section is None:
            return self.execute_command("INFO")
        else:
            return self.execute_command("INFO", section)

    def lastsave(self) -> Awaitable:
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.execute_command("LASTSAVE")

    def migrate(
        self,
        host: str,
        port: int,
        keys: KeysT,
        destination_db: int,
        timeout: int,
        copy: bool = False,
        replace: bool = False,
        auth: Optional[str] = None,
    ) -> Awaitable:
        """
        Migrate 1 or more keys from the current Redis server to a different
        server specified by the ``host``, ``port`` and ``destination_db``.

        The ``timeout``, specified in milliseconds, indicates the maximum
        time the connection between the two servers can be idle before the
        command is interrupted.

        If ``copy`` is True, the specified ``keys`` are NOT deleted from
        the source server.

        If ``replace`` is True, this operation will overwrite the keys
        on the destination server if they exist.

        If ``auth`` is specified, authenticate to the destination server with
        the password provided.
        """
        keys = list_or_args(keys, [])
        if not keys:
            raise DataError("MIGRATE requires at least one key")
        pieces: List[EncodableT] = []
        if copy:
            pieces.append(b"COPY")
        if replace:
            pieces.append(b"REPLACE")
        if auth:
            pieces.append(b"AUTH")
            pieces.append(auth)
        pieces.append(b"KEYS")
        pieces.extend(keys)
        return self.execute_command(
            "MIGRATE", host, port, "", destination_db, timeout, *pieces
        )

    def object(self, infotype: str, key: KeyT) -> Awaitable:
        """Return the encoding, idletime, or refcount about the key"""
        return self.execute_command("OBJECT", infotype, key, infotype=infotype)

    def memory_stats(self) -> Awaitable:
        """Return a dictionary of memory stats"""
        return self.execute_command("MEMORY STATS")

    def memory_usage(self, key: KeyT, samples: Optional[int] = None) -> Awaitable:
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.
        """
        args = []
        if isinstance(samples, int):
            args.extend([b"SAMPLES", samples])
        return self.execute_command("MEMORY USAGE", key, *args)

    def memory_purge(self) -> Awaitable:
        """Attempts to purge dirty pages for reclamation by allocator"""
        return self.execute_command("MEMORY PURGE")

    def ping(self) -> Awaitable:
        """Ping the Redis server"""
        return self.execute_command("PING")

    def save(self) -> Awaitable:
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command("SAVE")

    def sentinel_get_master_addr_by_name(self, service_name: str) -> Awaitable:
        """Returns a (host, port) pair for the given ``service_name``"""
        return self.execute_command("SENTINEL GET-MASTER-ADDR-BY-NAME", service_name)

    def sentinel_master(self, service_name: str) -> Awaitable:
        """Returns a dictionary containing the specified masters state."""
        return self.execute_command("SENTINEL MASTER", service_name)

    def sentinel_masters(self) -> Awaitable:
        """Returns a list of dictionaries containing each master's state."""
        return self.execute_command("SENTINEL MASTERS")

    def sentinel_monitor(self, name: str, ip: str, port: int, quorum: int) -> Awaitable:
        """Add a new master to Sentinel to be monitored"""
        return self.execute_command("SENTINEL MONITOR", name, ip, port, quorum)

    def sentinel_remove(self, name: str) -> Awaitable:
        """Remove a master from Sentinel's monitoring"""
        return self.execute_command("SENTINEL REMOVE", name)

    def sentinel_sentinels(self, service_name: str) -> Awaitable:
        """Returns a list of sentinels for ``service_name``"""
        return self.execute_command("SENTINEL SENTINELS", service_name)

    def sentinel_set(self, name: str, option: str, value: EncodableT) -> Awaitable:
        """Set Sentinel monitoring parameters for a given master"""
        return self.execute_command("SENTINEL SET", name, option, value)

    def sentinel_slaves(self, service_name: str) -> Awaitable:
        """Returns a list of slaves for ``service_name``"""
        return self.execute_command("SENTINEL SLAVES", service_name)

    def shutdown(self, save: bool = False, nosave: bool = False) -> None:
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.  If the "save" option is set,
        a data flush will be attempted even if there is no persistence
        configured.  If the "nosave" option is set, no data flush will be
        attempted.  The "save" and "nosave" options cannot both be set.
        """
        if save and nosave:
            raise DataError("SHUTDOWN save and nosave cannot both be set")
        args = ["SHUTDOWN"]
        if save:
            args.append("SAVE")
        if nosave:
            args.append("NOSAVE")
        try:
            self.execute_command(*args)
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    def slaveof(
        self, host: Optional[str] = None, port: Optional[int] = None
    ) -> Awaitable:
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.
        """
        if host is None and port is None:
            return self.execute_command("SLAVEOF", b"NO", b"ONE")
        return self.execute_command("SLAVEOF", host, port)

    def slowlog_get(self, num: Optional[int] = None) -> Awaitable:
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args: List[EncodableT] = ["SLOWLOG GET"]
        if num is not None:
            args.append(num)
        decode_responses = self.connection_pool.connection_kwargs.get(
            "decode_responses", False
        )
        return self.execute_command(*args, decode_responses=decode_responses)

    def slowlog_len(self) -> Awaitable:
        """Get the number of items in the slowlog"""
        return self.execute_command("SLOWLOG LEN")

    def slowlog_reset(self) -> Awaitable:
        """Remove all items in the slowlog"""
        return self.execute_command("SLOWLOG RESET")

    def time(self) -> Awaitable:
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        return self.execute_command("TIME")

    def wait(self, num_replicas: int, timeout: int) -> Awaitable:
        """
        Redis synchronous replication
        That returns the number of replicas that processed the query when
        we finally have at least ``num_replicas``, or when the ``timeout`` was
        reached.
        """
        return self.execute_command("WAIT", num_replicas, timeout)

    # BASIC KEY COMMANDS
    def append(self, key: KeyT, value: EncodableT) -> Awaitable:
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return self.execute_command("APPEND", key, value)

    def bitcount(
        self, key: KeyT, start: Optional[int] = None, end: Optional[int] = None
    ) -> Awaitable:
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        params: List[EncodableT] = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or (end is not None and start is None):
            raise DataError("Both start and end must be specified")
        return self.execute_command("BITCOUNT", *params)

    def bitfield(
        self, key: KeyT, default_overflow: Optional[str] = None
    ) -> "BitFieldOperation":
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.
        """
        return BitFieldOperation(self, key, default_overflow=default_overflow)

    def bitop(self, operation: str, dest: KeyT, *keys: KeyT) -> Awaitable:
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        return self.execute_command("BITOP", operation, dest, *keys)

    def bitpos(
        self,
        key: KeyT,
        bit: int,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> Awaitable:
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` difines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """
        if bit not in (0, 1):
            raise DataError("bit must be 0 or 1")
        params = [key, bit]

        if start is not None:
            params.append(start)
            if end is not None:
                params.append(end)
        elif end is not None:
            raise DataError("start argument is not set, when end is specified")
        return self.execute_command("BITPOS", *params)

    def decr(self, name: KeyT, amount: int = 1) -> Awaitable:
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        # An alias for ``decr()``, because it is already implemented
        # as DECRBY redis command.
        return self.decrby(name, amount)

    def decrby(self, name: KeyT, amount: int = 1) -> Awaitable:
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.execute_command("DECRBY", name, amount)

    def delete(self, *names: KeyT) -> Awaitable:
        """Delete one or more keys specified by ``names``"""
        return self.execute_command("DEL", *names)

    def dump(self, name: KeyT) -> Awaitable:
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        return self.execute_command("DUMP", name)

    def exists(self, *names: KeyT) -> Awaitable:
        """Returns the number of ``names`` that exist"""
        return self.execute_command("EXISTS", *names)

    def expire(self, name: KeyT, time: ExpiryT) -> Awaitable:
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command("EXPIRE", name, time)

    def expireat(self, name: KeyT, when: AbsExpiryT) -> Awaitable:
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            when = int(mod_time.mktime(when.timetuple()))
        return self.execute_command("EXPIREAT", name, when)

    def get(self, name: KeyT) -> Awaitable:
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return self.execute_command("GET", name)

    def getbit(self, name: KeyT, offset: int) -> Awaitable:
        """Returns a boolean indicating the value of ``offset`` in ``name``"""
        return self.execute_command("GETBIT", name, offset)

    def getrange(self, key: KeyT, start: int, end: int) -> Awaitable:
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return self.execute_command("GETRANGE", key, start, end)

    def getset(self, name: KeyT, value: EncodableT) -> Awaitable:
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return self.execute_command("GETSET", name, value)

    def incr(self, name: KeyT, amount: int = 1) -> Awaitable:
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.incrby(name, amount)

    def incrby(self, name: KeyT, amount: int = 1) -> Awaitable:
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.execute_command("INCRBY", name, amount)

    def incrbyfloat(self, name: KeyT, amount: float = 1.0) -> Awaitable:
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        return self.execute_command("INCRBYFLOAT", name, amount)

    def keys(self, pattern: PatternT = "*") -> Awaitable:
        """Returns a list of keys matching ``pattern``"""
        return self.execute_command("KEYS", pattern)

    def mget(self, keys: KeysT, *args: EncodableT) -> Awaitable:
        """
        Returns a list of values ordered identically to ``keys``
        """
        encoded_args = list_or_args(keys, args)
        options: Dict[str, Union[EncodableT, Iterable[EncodableT]]] = {}
        if not encoded_args:
            options[EMPTY_RESPONSE] = []
        return self.execute_command("MGET", *encoded_args, **options)

    def mset(self, mapping: Mapping[AnyKeyT, EncodableT]) -> Awaitable:
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().
        """
        items: List[EncodableT] = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("MSET", *items)

    def msetnx(self, mapping: Mapping[AnyKeyT, EncodableT]) -> Awaitable:
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.
        """
        items: List[EncodableT] = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("MSETNX", *items)

    def move(self, name: KeyT, db: int) -> Awaitable:
        """Moves the key ``name`` to a different Redis database ``db``"""
        return self.execute_command("MOVE", name, db)

    def persist(self, name: KeyT) -> Awaitable:
        """Removes an expiration on ``name``"""
        return self.execute_command("PERSIST", name)

    def pexpire(self, name: KeyT, time: ExpiryT) -> Awaitable:
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds() * 1000)
        return self.execute_command("PEXPIRE", name, time)

    def pexpireat(self, name: KeyT, when: AbsExpiryT) -> Awaitable:
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(mod_time.mktime(when.timetuple())) * 1000 + ms
        return self.execute_command("PEXPIREAT", name, when)

    def psetex(self, name: KeyT, time_ms: ExpiryT, value: EncodableT) -> Awaitable:
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        if isinstance(time_ms, datetime.timedelta):
            time_ms = int(time_ms.total_seconds() * 1000)
        return self.execute_command("PSETEX", name, time_ms, value)

    def pttl(self, name: KeyT) -> Awaitable:
        """Returns the number of milliseconds until the key ``name`` will expire"""
        return self.execute_command("PTTL", name)

    def randomkey(self) -> Awaitable:
        """Returns the name of a random key"""
        return self.execute_command("RANDOMKEY")

    def rename(self, src: KeyT, dst: KeyT) -> Awaitable:
        """
        Rename key ``src`` to ``dst``
        """
        return self.execute_command("RENAME", src, dst)

    def renamenx(self, src: KeyT, dst: KeyT) -> Awaitable:
        """Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"""
        return self.execute_command("RENAMENX", src, dst)

    def restore(
        self,
        name: KeyT,
        ttl: float,
        value: EncodableT,
        replace: bool = False,
        absttl: bool = False,
    ) -> Awaitable:
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.

        ``replace`` allows an existing key on ``name`` to be overridden. If
        it's not specified an error is raised on collision.

        ``absttl`` if True, specified ``ttl`` should represent an absolute Unix
        timestamp in milliseconds in which the key will expire. (Redis 5.0 or
        greater).
        """
        params = [name, ttl, value]
        if replace:
            params.append("REPLACE")
        if absttl:
            params.append("ABSTTL")
        return self.execute_command("RESTORE", *params)

    def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> Awaitable:
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.

        ``keepttl`` if True, retain the time to live associated with the key.
            (Available since Redis 6.0)
        """
        pieces: List[EncodableT] = [name, value]
        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                ex = int(ex.total_seconds())
            pieces.append(ex)
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                px = int(px.total_seconds() * 1000)
            pieces.append(px)

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if keepttl:
            pieces.append("KEEPTTL")

        return self.execute_command("SET", *pieces)

    def setbit(self, name: KeyT, offset: int, value: int) -> Awaitable:
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.execute_command("SETBIT", name, offset, value)

    def setex(
        self, name: KeyT, time: Union[int, datetime.timedelta], value: EncodableT
    ) -> Awaitable:
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command("SETEX", name, time, value)

    def setnx(self, name: KeyT, value: EncodableT) -> Awaitable:
        """Set the value of key ``name`` to ``value`` if key doesn't exist"""
        return self.execute_command("SETNX", name, value)

    def setrange(self, name: KeyT, offset: int, value: EncodableT) -> Awaitable:
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        return self.execute_command("SETRANGE", name, offset, value)

    def strlen(self, name: KeyT) -> Awaitable:
        """Return the number of bytes stored in the value of ``name``"""
        return self.execute_command("STRLEN", name)

    def substr(self, name: KeyT, start: int, end: int = -1) -> Awaitable:
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command("SUBSTR", name, start, end)

    def touch(self, *args: KeyT) -> Awaitable:
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.
        """
        return self.execute_command("TOUCH", *args)

    def ttl(self, name: KeyT) -> Awaitable:
        """Returns the number of seconds until the key ``name`` will expire"""
        return self.execute_command("TTL", name)

    def type(self, name: KeyT) -> Awaitable:
        """Returns the type of key ``name``"""
        return self.execute_command("TYPE", name)

    def unlink(self, *names: KeyT) -> Awaitable:
        """Unlink one or more keys specified by ``names``"""
        return self.execute_command("UNLINK", *names)

    # LIST COMMANDS
    def blpop(self, keys: KeysT, timeout: int = 0) -> Awaitable:
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        return self.execute_command("BLPOP", *list_or_args(keys, (timeout,)))

    def brpop(self, keys: KeysT, timeout: int = 0) -> Awaitable:
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        return self.execute_command("BRPOP", *list_or_args(keys, (timeout,)))

    def brpoplpush(self, src: KeyT, dst: KeyT, timeout: int = 0) -> Awaitable:
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        return self.execute_command("BRPOPLPUSH", src, dst, timeout)

    def lindex(self, name: KeyT, index: int) -> Awaitable:
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command("LINDEX", name, index)

    def linsert(
        self, name: KeyT, where: str, refvalue: EncodableT, value: EncodableT
    ) -> Awaitable:
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return self.execute_command("LINSERT", name, where, refvalue, value)

    def llen(self, name: KeyT) -> Awaitable:
        """Return the length of the list ``name``"""
        return self.execute_command("LLEN", name)

    def lpop(self, name: KeyT) -> Awaitable:
        """Remove and return the first item of the list ``name``"""
        return self.execute_command("LPOP", name)

    def lpush(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Push ``values`` onto the head of the list ``name``"""
        return self.execute_command("LPUSH", name, *values)

    def lpushx(self, name: KeyT, value: EncodableT) -> Awaitable:
        """Push ``value`` onto the head of the list ``name`` if ``name`` exists"""
        return self.execute_command("LPUSHX", name, value)

    def lrange(self, name: KeyT, start: int, end: int) -> Awaitable:
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command("LRANGE", name, start, end)

    def lrem(self, name: KeyT, count: int, value: EncodableT) -> Awaitable:
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return self.execute_command("LREM", name, count, value)

    def lset(self, name: KeyT, index: int, value: EncodableT) -> Awaitable:
        """Set ``position`` of list ``name`` to ``value``"""
        return self.execute_command("LSET", name, index, value)

    def ltrim(self, name: KeyT, start: int, end: int) -> Awaitable:
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command("LTRIM", name, start, end)

    def rpop(self, name: KeyT) -> Awaitable:
        """Remove and return the last item of the list ``name``"""
        return self.execute_command("RPOP", name)

    def rpoplpush(self, src: KeyT, dst: KeyT) -> Awaitable:
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.execute_command("RPOPLPUSH", src, dst)

    def rpush(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Push ``values`` onto the tail of the list ``name``"""
        return self.execute_command("RPUSH", name, *values)

    def rpushx(self, name: KeyT, value: EncodableT) -> Awaitable:
        """Push ``value`` onto the tail of the list ``name`` if ``name`` exists"""
        return self.execute_command("RPUSHX", name, value)

    def lpos(
        self,
        name: KeyT,
        value: EncodableT,
        rank: Optional[int] = None,
        count: Optional[int] = None,
        maxlen: Optional[int] = None,
    ) -> Awaitable:
        """
        Get position of ``value`` within the list ``name``

         If specified, ``rank`` indicates the "rank" of the first element to
         return in case there are multiple copies of ``value`` in the list.
         By default, LPOS returns the position of the first occurrence of
         ``value`` in the list. When ``rank`` 2, LPOS returns the position of
         the second ``value`` in the list. If ``rank`` is negative, LPOS
         searches the list in reverse. For example, -1 would return the
         position of the last occurrence of ``value`` and -2 would return the
         position of the next to last occurrence of ``value``.

         If specified, ``count`` indicates that LPOS should return a list of
         up to ``count`` positions. A ``count`` of 2 would return a list of
         up to 2 positions. A ``count`` of 0 returns a list of all positions
         matching ``value``. When ``count`` is specified and but ``value``
         does not exist in the list, an empty list is returned.

         If specified, ``maxlen`` indicates the maximum number of list
         elements to scan. A ``maxlen`` of 1000 will only return the
         position(s) of items within the first 1000 entries in the list.
         A ``maxlen`` of 0 (the default) will scan the entire list.
        """
        pieces: List[EncodableT] = [name, value]
        if rank is not None:
            pieces.extend(["RANK", rank])

        if count is not None:
            pieces.extend(["COUNT", count])

        if maxlen is not None:
            pieces.extend(["MAXLEN", maxlen])

        return self.execute_command("LPOS", *pieces)

    def sort(
        self,
        name: KeyT,
        start: Optional[int] = None,
        num: Optional[int] = None,
        by: Optional[KeyT] = None,
        get: Optional[KeysT] = None,
        desc: bool = False,
        alpha: bool = False,
        store: Optional[KeyT] = None,
        groups: bool = False,
    ) -> Awaitable:
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")

        pieces: List[EncodableT] = [name]
        if by is not None:
            pieces.append(b"BY")
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append(b"LIMIT")
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, (bytes, str)):
                pieces.append(b"GET")
                pieces.append(get)
            else:
                for g in get:
                    pieces.append(b"GET")
                    pieces.append(g)
        if desc:
            pieces.append(b"DESC")
        if alpha:
            pieces.append(b"ALPHA")
        if store is not None:
            pieces.append(b"STORE")
            pieces.append(store)

        if groups:
            if not get or isinstance(get, (bytes, str)) or len(get) < 2:
                raise DataError(
                    'when using "groups" the "get" argument '
                    "must be specified and contain at least "
                    "two keys"
                )
            options: Dict[str, Optional[int]] = {"groups": len(get)}
        else:
            options = {"groups": None}

        return self.execute_command("SORT", *pieces, **options)

    # SCAN COMMANDS
    def scan(
        self,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
    ) -> Awaitable:
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        pieces: List[EncodableT] = [cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        if _type is not None:
            pieces.extend([b"TYPE", _type])
        return self.execute_command("SCAN", *pieces)

    async def scan_iter(
        self,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
    ) -> AsyncIterator:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        cursor = None
        while cursor != 0:
            cursor, data = await self.scan(
                cursor=cursor or 0, match=match, count=count, _type=_type
            )
            for d in data:
                yield d

    def sscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> Awaitable:
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces: List[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        return self.execute_command("SSCAN", *pieces)

    async def sscan_iter(
        self, name: KeyT, match: Optional[PatternT] = None, count: Optional[int] = None
    ) -> AsyncIterator:
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = None
        while cursor != 0:
            cursor, data = await self.sscan(
                name, cursor=cursor or 0, match=match, count=count
            )
            for d in data:
                yield d

    def hscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
    ) -> Awaitable:
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces: List[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        return self.execute_command("HSCAN", *pieces)

    async def hscan_iter(
        self, name: str, match: Optional[PatternT] = None, count: Optional[int] = None
    ) -> AsyncIterator:
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = None
        while cursor != 0:
            cursor, data = await self.hscan(
                name, cursor=cursor or 0, match=match, count=count
            )
            for it in data.items():
                yield it

    def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        score_cast_func: Union[Type, Callable] = float,
    ) -> Awaitable:
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces: List[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        options = {"score_cast_func": score_cast_func}
        return self.execute_command("ZSCAN", *pieces, **options)

    async def zscan_iter(
        self,
        name: KeyT,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        score_cast_func: Union[Type, Callable] = float,
    ) -> AsyncIterator:
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = None
        while cursor != 0:
            cursor, data = await self.zscan(
                name,
                cursor=cursor or 0,
                match=match,
                count=count,
                score_cast_func=score_cast_func,
            )
            for d in data:
                yield d

    # SET COMMANDS
    def sadd(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Add ``value(s)`` to set ``name``"""
        return self.execute_command("SADD", name, *values)

    def scard(self, name: KeyT) -> Awaitable:
        """Return the number of elements in set ``name``"""
        return self.execute_command("SCARD", name)

    def sdiff(self, keys: KeysT, *args: EncodableT) -> Awaitable:
        """Return the difference of sets specified by ``keys``"""
        parsed_args = list_or_args(keys, args)
        return self.execute_command("SDIFF", *parsed_args)

    def sdiffstore(self, dest: KeyT, keys: KeysT, *args: EncodableT) -> Awaitable:
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        parsed_args = list_or_args(keys, args)
        return self.execute_command("SDIFFSTORE", dest, *parsed_args)

    def sinter(self, keys: KeysT, *args: EncodableT) -> Awaitable:
        """Return the intersection of sets specified by ``keys``"""
        parsed_args = list_or_args(keys, args)
        return self.execute_command("SINTER", *parsed_args)

    def sinterstore(self, dest: KeyT, keys: KeysT, *args: EncodableT) -> Awaitable:
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        parsed_args = list_or_args(keys, args)
        return self.execute_command("SINTERSTORE", dest, *parsed_args)

    def sismember(self, name: KeyT, value: EncodableT) -> Awaitable:
        """Return a boolean indicating if ``value`` is a member of set ``name``"""
        return self.execute_command("SISMEMBER", name, value)

    def smembers(self, name: KeyT) -> Awaitable:
        """Return all members of the set ``name``"""
        return self.execute_command("SMEMBERS", name)

    def smove(self, src: KeyT, dst: KeyT, value: EncodableT) -> Awaitable:
        """Move ``value`` from set ``src`` to set ``dst`` atomically"""
        return self.execute_command("SMOVE", src, dst, value)

    def spop(self, name: KeyT, count: Optional[int] = None) -> Awaitable:
        """Remove and return a random member of set ``name``"""
        args = (count is not None) and [count] or []
        return self.execute_command("SPOP", name, *args)

    def srandmember(self, name: KeyT, number: Optional[int] = None) -> Awaitable:
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        members of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        args = (number is not None) and [number] or []
        return self.execute_command("SRANDMEMBER", name, *args)

    def srem(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Remove ``values`` from set ``name``"""
        return self.execute_command("SREM", name, *values)

    def sunion(self, keys: KeysT, *args: EncodableT) -> Awaitable:
        """Return the union of sets specified by ``keys``"""
        parsed_args = list_or_args(keys, args)
        return self.execute_command("SUNION", *parsed_args)

    def sunionstore(self, dest: KeyT, keys: KeysT, *args: EncodableT) -> Awaitable:
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        parsed_args = list_or_args(keys, args)
        return self.execute_command("SUNIONSTORE", dest, *parsed_args)

    # STREAMS COMMANDS
    def xack(self, name: KeyT, groupname: GroupT, *ids: StreamIdT) -> Awaitable:
        """
        Acknowledges the successful processing of one or more messages.
        name: name of the stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowlege.
        """
        return self.execute_command("XACK", name, groupname, *ids)

    def xadd(
        self,
        name: KeyT,
        fields: Dict[FieldT, EncodableT],
        id: StreamIdT = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> Awaitable:
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size
        approximate: actual stream length may be slightly more than maxlen

        """
        pieces: List[EncodableT] = []
        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 1:
                raise DataError("XADD maxlen must be a positive integer")
            pieces.append(b"MAXLEN")
            if approximate:
                pieces.append(b"~")
            pieces.append(str(maxlen))
        pieces.append(id)
        if not isinstance(fields, dict) or len(fields) == 0:
            raise DataError("XADD fields must be a non-empty dict")
        for pair in fields.items():
            pieces.extend(pair)
        return self.execute_command("XADD", name, *pieces)

    def xclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        message_ids: Union[List[StreamIdT], Tuple[StreamIdT]],
        idle: Optional[int] = None,
        time: Optional[int] = None,
        retrycount: Optional[int] = None,
        force: bool = False,
        justid: bool = False,
    ) -> Awaitable:
        """
        Changes the ownership of a pending message.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds
        message_ids: non-empty list or tuple of message IDs to claim
        idle: optional. Set the idle time (last time it was delivered) of the
         message in ms
        time: optional integer. This is the same as idle but instead of a
         relative amount of milliseconds, it sets the idle time to a specific
         Unix time (in milliseconds).
        retrycount: optional integer. set the retry counter to the specified
         value. This counter is incremented every time a message is delivered
         again.
        force: optional boolean, false by default. Creates the pending message
         entry in the PEL even if certain specified IDs are not already in the
         PEL assigned to a different client.
        justid: optional boolean, false by default. Return just an array of IDs
         of messages successfully claimed, without returning the actual message
        """
        if not isinstance(min_idle_time, int) or min_idle_time < 0:
            raise DataError("XCLAIM min_idle_time must be a non negative " "integer")
        if not isinstance(message_ids, (list, tuple)) or not message_ids:
            raise DataError(
                "XCLAIM message_ids must be a non empty list or "
                "tuple of message IDs to claim"
            )

        kwargs = {}
        pieces: List[EncodableT] = [name, groupname, consumername, str(min_idle_time)]
        pieces.extend(list(message_ids))

        if idle is not None:
            if not isinstance(idle, int):
                raise DataError("XCLAIM idle must be an integer")
            pieces.extend((b"IDLE", str(idle)))
        if time is not None:
            if not isinstance(time, int):
                raise DataError("XCLAIM time must be an integer")
            pieces.extend((b"TIME", str(time)))
        if retrycount is not None:
            if not isinstance(retrycount, int):
                raise DataError("XCLAIM retrycount must be an integer")
            pieces.extend((b"RETRYCOUNT", str(retrycount)))

        if force:
            if not isinstance(force, bool):
                raise DataError("XCLAIM force must be a boolean")
            pieces.append(b"FORCE")
        if justid:
            if not isinstance(justid, bool):
                raise DataError("XCLAIM justid must be a boolean")
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True
        return self.execute_command("XCLAIM", *pieces, **kwargs)

    def xdel(self, name: KeyT, *ids: StreamIdT) -> Awaitable:
        """
        Deletes one or more messages from a stream.
        name: name of the stream.
        *ids: message ids to delete.
        """
        return self.execute_command("XDEL", name, *ids)

    def xgroup_create(
        self, name: KeyT, groupname: GroupT, id: StreamIdT = "$", mkstream: bool = False
    ) -> Awaitable:
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        pieces: List[EncodableT] = ["XGROUP CREATE", name, groupname, id]
        if mkstream:
            pieces.append(b"MKSTREAM")
        return self.execute_command(*pieces)

    def xgroup_delconsumer(
        self, name: KeyT, groupname: GroupT, consumername: ConsumerT
    ) -> Awaitable:
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it
        was deleted.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to delete
        """
        return self.execute_command("XGROUP DELCONSUMER", name, groupname, consumername)

    def xgroup_destroy(self, name: KeyT, groupname: GroupT) -> Awaitable:
        """
        Destroy a consumer group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        return self.execute_command("XGROUP DESTROY", name, groupname)

    def xgroup_setid(self, name: KeyT, groupname: GroupT, id: StreamIdT) -> Awaitable:
        """
        Set the consumer group last delivered ID to something else.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        return self.execute_command("XGROUP SETID", name, groupname, id)

    def xinfo_consumers(self, name: KeyT, groupname: GroupT) -> Awaitable:
        """
        Returns general information about the consumers in the group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        return self.execute_command("XINFO CONSUMERS", name, groupname)

    def xinfo_groups(self, name: KeyT) -> Awaitable:
        """
        Returns general information about the consumer groups of the stream.
        name: name of the stream.
        """
        return self.execute_command("XINFO GROUPS", name)

    def xinfo_stream(self, name: KeyT) -> Awaitable:
        """
        Returns general information about the stream.
        name: name of the stream.
        """
        return self.execute_command("XINFO STREAM", name)

    def xlen(self, name: KeyT) -> Awaitable:
        """
        Returns the number of elements in a given stream.
        """
        return self.execute_command("XLEN", name)

    def xpending(self, name: KeyT, groupname: GroupT) -> Awaitable:
        """
        Returns information about pending messages of a group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        return self.execute_command("XPENDING", name, groupname)

    def xpending_range(
        self,
        name: KeyT,
        groupname: GroupT,
        min: Optional[StreamIdT],
        max: Optional[StreamIdT],
        count: Optional[int],
        consumername: Optional[ConsumerT] = None,
    ) -> Awaitable:
        """
        Returns information about pending messages, in a range.
        name: name of the stream.
        groupname: name of the consumer group.
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).
        """
        pieces: List[EncodableT] = [name, groupname]
        if min is not None or max is not None or count is not None:
            if min is None or max is None or count is None:
                raise DataError(
                    "XPENDING must be provided with min, max "
                    "and count parameters, or none of them. "
                )
            if not isinstance(count, int) or count < -1:
                raise DataError("XPENDING count must be a integer >= -1")
            pieces.extend((min, max, str(count)))
        if consumername is not None:
            if min is None or max is None or count is None:
                raise DataError(
                    "if XPENDING is provided with consumername,"
                    " it must be provided with min, max and"
                    " count parameters"
                )
            pieces.append(consumername)
        return self.execute_command("XPENDING", *pieces, parse_detail=True)

    def xrange(
        self,
        name: KeyT,
        min: StreamIdT = "-",
        max: StreamIdT = "+",
        count: Optional[int] = None,
    ) -> Awaitable:
        """
        Read stream values within an interval.
        name: name of the stream.
        start: first stream ID. defaults to '-',
               meaning the earliest available.
        finish: last stream ID. defaults to '+',
                meaning the latest available.
        count: if set, only return this many items, beginning with the
               earliest available.
        """
        pieces: List[EncodableT] = [min, max]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XRANGE", name, *pieces)

    def xread(
        self,
        streams: Dict[KeyT, StreamIdT],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> Awaitable:
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        """
        pieces: List[EncodableT] = []
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREAD block must be a non-negative integer")
            pieces.append(b"BLOCK")
            pieces.append(str(block))
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREAD count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError("XREAD streams must be a non empty dict")
        pieces.append(b"STREAMS")
        keys, values = zip(*streams.items())
        pieces.extend(keys)
        pieces.extend(values)
        return self.execute_command("XREAD", *pieces)

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Dict[KeyT, StreamIdT],
        count: Optional[int] = None,
        block: Optional[int] = None,
        noack: bool = False,
    ) -> Awaitable:
        """
        Read from a stream via a consumer group.
        groupname: name of the consumer group.
        consumername: name of the requesting consumer.
        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL
        """
        pieces: List[EncodableT] = [b"GROUP", groupname, consumername]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREADGROUP count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREADGROUP block must be a non-negative " "integer")
            pieces.append(b"BLOCK")
            pieces.append(str(block))
        if noack:
            pieces.append(b"NOACK")
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError("XREADGROUP streams must be a non empty dict")
        pieces.append(b"STREAMS")
        pieces.extend(streams.keys())
        pieces.extend(streams.values())
        return self.execute_command("XREADGROUP", *pieces)

    def xrevrange(
        self,
        name: KeyT,
        max: StreamIdT = "+",
        min: StreamIdT = "-",
        count: Optional[int] = None,
    ) -> Awaitable:
        """
        Read stream values within an interval, in reverse order.
        name: name of the stream
        start: first stream ID. defaults to '+',
               meaning the latest available.
        finish: last stream ID. defaults to '-',
                meaning the earliest available.
        count: if set, only return this many items, beginning with the
               latest available.
        """
        pieces: List[EncodableT] = [max, min]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREVRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XREVRANGE", name, *pieces)

    def xtrim(self, name: KeyT, maxlen: int, approximate: bool = True) -> Awaitable:
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        approximate: actual stream length may be slightly more than maxlen
        """
        pieces: List[EncodableT] = [b"MAXLEN"]
        if approximate:
            pieces.append(b"~")
        pieces.append(maxlen)
        return self.execute_command("XTRIM", name, *pieces)

    # SORTED SET COMMANDS
    def zadd(
        self,
        name: KeyT,
        mapping: Mapping[AnyKeyT, EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> Awaitable:
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.
        """
        if not mapping:
            raise DataError("ZADD requires at least one element/score pair")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise DataError(
                "ZADD option 'incr' only works when passing a "
                "single element/score pair"
            )
        pieces: List[EncodableT] = []
        options = {}
        if nx:
            pieces.append(b"NX")
        if xx:
            pieces.append(b"XX")
        if ch:
            pieces.append(b"CH")
        if incr:
            pieces.append(b"INCR")
            options["as_score"] = True
        for pair in mapping.items():
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command("ZADD", name, *pieces, **options)

    def zcard(self, name: KeyT) -> Awaitable:
        """Return the number of elements in the sorted set ``name``"""
        return self.execute_command("ZCARD", name)

    def zcount(self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT) -> Awaitable:
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.execute_command("ZCOUNT", name, min, max)

    def zincrby(self, name: KeyT, amount: float, value: EncodableT) -> Awaitable:
        """Increment the score of ``value`` in sorted set ``name`` by ``amount``"""
        return self.execute_command("ZINCRBY", name, amount, value)

    def zinterstore(
        self,
        dest: KeyT,
        keys: Union[Sequence[KeyT], Mapping[AnyKeyT, float]],
        aggregate: Optional[str] = None,
    ) -> Awaitable:
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate("ZINTERSTORE", dest, keys, aggregate)

    def zlexcount(self, name: KeyT, min: EncodableT, max: EncodableT) -> Awaitable:
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return self.execute_command("ZLEXCOUNT", name, min, max)

    def zpopmax(self, name: KeyT, count: Optional[int] = None) -> Awaitable:
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMAX", name, *args, **options)

    def zpopmin(self, name: KeyT, count: Optional[int] = None) -> Awaitable:
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMIN", name, *args, **options)

    def bzpopmax(self, keys: KeysT, timeout: int = 0) -> Awaitable:
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        parsed_keys = list_or_args(keys, (timeout,))
        return self.execute_command("BZPOPMAX", *parsed_keys)

    def bzpopmin(self, keys: KeysT, timeout: int = 0) -> Awaitable:
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        klist: List[EncodableT] = list_or_args(keys, None)
        klist.append(timeout)
        return self.execute_command("BZPOPMIN", *klist)

    def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: Union[Type, Callable] = float,
    ) -> Awaitable:
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if desc:
            return self.zrevrange(name, start, end, withscores, score_cast_func)
        pieces: List[EncodableT] = ["ZRANGE", name, start, end]
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: Optional[int] = None,
        num: Optional[int] = None,
    ) -> Awaitable:
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces: List[EncodableT] = ["ZRANGEBYLEX", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        return self.execute_command(*pieces)

    def zrevrangebylex(
        self,
        name: KeyT,
        max: EncodableT,
        min: EncodableT,
        start: Optional[int] = None,
        num: Optional[int] = None,
    ) -> Awaitable:
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces: List[EncodableT] = ["ZREVRANGEBYLEX", name, max, min]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        return self.execute_command(*pieces)

    def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: Optional[int] = None,
        num: Optional[int] = None,
        withscores: bool = False,
        score_cast_func: Union[Type, Callable] = float,
    ) -> Awaitable:
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces: List[EncodableT] = ["ZRANGEBYSCORE", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrank(self, name: KeyT, value: EncodableT) -> Awaitable:
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command("ZRANK", name, value)

    def zrem(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Remove member ``values`` from sorted set ``name``"""
        return self.execute_command("ZREM", name, *values)

    def zremrangebylex(self, name: KeyT, min: EncodableT, max: EncodableT) -> Awaitable:
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        """
        return self.execute_command("ZREMRANGEBYLEX", name, min, max)

    def zremrangebyrank(self, name: KeyT, min: int, max: int) -> Awaitable:
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.execute_command("ZREMRANGEBYRANK", name, min, max)

    def zremrangebyscore(
        self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> Awaitable:
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.execute_command("ZREMRANGEBYSCORE", name, min, max)

    def zrevrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: Union[Type, Callable] = float,
    ) -> Awaitable:
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces: List[EncodableT] = ["ZREVRANGE", name, start, end]
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: Optional[int] = None,
        num: Optional[int] = None,
        withscores: bool = False,
        score_cast_func: Union[Type, Callable] = float,
    ) -> Awaitable:
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces: List[EncodableT] = ["ZREVRANGEBYSCORE", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrevrank(self, name: KeyT, value: EncodableT) -> Awaitable:
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command("ZREVRANK", name, value)

    def zscore(self, name: str, value: EncodableT) -> Awaitable:
        """Return the score of element ``value`` in sorted set ``name``"""
        return self.execute_command("ZSCORE", name, value)

    def zunionstore(
        self,
        dest: KeyT,
        keys: Union[Sequence[KeyT], Mapping[AnyKeyT, float]],
        aggregate: Optional[str] = None,
    ) -> Awaitable:
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate("ZUNIONSTORE", dest, keys, aggregate)

    def _zaggregate(
        self,
        command: str,
        dest: KeyT,
        keys: Union[Sequence[KeyT], Mapping[AnyKeyT, float]],
        aggregate: Optional[str] = None,
    ) -> Awaitable:
        pieces: List[EncodableT] = [command, dest, len(keys)]
        key_names: Union[Sequence[KeyT], AbstractSet[AnyKeyT]]
        weights: Optional[ValuesView[float]]
        if isinstance(keys, Mapping):
            key_names, weights = keys.keys(), keys.values()
        else:
            key_names = keys
            weights = None
        pieces.extend(key_names)
        if weights:
            pieces.append(b"WEIGHTS")
            pieces.extend(weights)
        if aggregate:
            pieces.append(b"AGGREGATE")
            pieces.append(aggregate)
        return self.execute_command(*pieces)

    # HYPERLOGLOG COMMANDS
    def pfadd(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Adds the specified elements to the specified HyperLogLog."""
        return self.execute_command("PFADD", name, *values)

    def pfcount(self, *sources: KeyT) -> Awaitable:
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return self.execute_command("PFCOUNT", *sources)

    def pfmerge(self, dest: KeyT, *sources: KeyT) -> Awaitable:
        """Merge N different HyperLogLogs into a single one."""
        return self.execute_command("PFMERGE", dest, *sources)

    # HASH COMMANDS
    def hdel(self, name: KeyT, *keys: FieldT) -> Awaitable:
        """Delete ``keys`` from hash ``name``"""
        return self.execute_command("HDEL", name, *keys)

    def hexists(self, name: KeyT, key: FieldT) -> Awaitable:
        """Returns a boolean indicating if ``key`` exists within hash ``name``"""
        return self.execute_command("HEXISTS", name, key)

    def hget(self, name: KeyT, key: FieldT) -> Awaitable:
        """Return the value of ``key`` within the hash ``name``"""
        return self.execute_command("HGET", name, key)

    def hgetall(self, name: KeyT) -> Awaitable:
        """Return a Python dict of the hash's name/value pairs"""
        return self.execute_command("HGETALL", name)

    def hincrby(self, name: KeyT, key: FieldT, amount: int = 1) -> Awaitable:
        """Increment the value of ``key`` in hash ``name`` by ``amount``"""
        return self.execute_command("HINCRBY", name, key, amount)

    def hincrbyfloat(self, name: KeyT, key: FieldT, amount: float = 1.0) -> Awaitable:
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        return self.execute_command("HINCRBYFLOAT", name, key, amount)

    def hkeys(self, name: KeyT) -> Awaitable:
        """Return the list of keys within hash ``name``"""
        return self.execute_command("HKEYS", name)

    def hlen(self, name: KeyT) -> Awaitable:
        """Return the number of elements in hash ``name``"""
        return self.execute_command("HLEN", name)

    def hset(
        self,
        name: KeyT,
        key: Optional[FieldT] = None,
        value: Optional[EncodableT] = None,
        mapping: Optional[Mapping[AnyFieldT, EncodableT]] = None,
    ) -> Awaitable:
        """
        Set ``key`` to ``value`` within hash ``name``,
        ``mapping`` accepts a dict of key/value pairs that that will be
        added to hash ``name``.
        Returns the number of fields that were added.
        """
        if key is None and not mapping:
            raise DataError("'hset' with no key value pairs")
        items: List[Union[FieldT, Optional[EncodableT]]] = []
        if key is not None:
            items.extend((key, value))
        if mapping:
            for pair in mapping.items():
                items.extend(pair)

        return self.execute_command("HSET", name, *items)

    def hsetnx(self, name: KeyT, key: FieldT, value: EncodableT) -> Awaitable:
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.execute_command("HSETNX", name, key, value)

    def hmset(self, name: KeyT, mapping: Mapping[AnyFieldT, EncodableT]) -> Awaitable:
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        warnings.warn(
            f"{self.__class__.__name__}.hmset() is deprecated. "
            f"Use {self.__class__.__name__}.hset() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items: List[Union[AnyFieldT, EncodableT]] = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("HMSET", name, *items)

    def hmget(self, name: KeyT, keys: Sequence[KeyT], *args: FieldT) -> Awaitable:
        """Returns a list of values ordered identically to ``keys``"""
        parsed_args = list_or_args(keys, args)
        return self.execute_command("HMGET", name, *parsed_args)

    def hvals(self, name: KeyT) -> Awaitable:
        """Return the list of values within hash ``name``"""
        return self.execute_command("HVALS", name)

    def hstrlen(self, name: KeyT, key: FieldT) -> Awaitable:
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``
        """
        return self.execute_command("HSTRLEN", name, key)

    def publish(self, channel: ChannelT, message: EncodableT) -> Awaitable:
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.execute_command("PUBLISH", channel, message)

    def pubsub_channels(self, pattern: PatternT = "*") -> Awaitable:
        """
        Return a list of channels that have at least one subscriber
        """
        return self.execute_command("PUBSUB CHANNELS", pattern)

    def pubsub_numpat(self) -> Awaitable:
        """
        Returns the number of subscriptions to patterns
        """
        return self.execute_command("PUBSUB NUMPAT")

    def pubsub_numsub(self, *args: ChannelT) -> Awaitable:
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``
        """
        return self.execute_command("PUBSUB NUMSUB", *args)

    def cluster(self, cluster_arg: str, *args: str) -> Awaitable:
        return self.execute_command(f"CLUSTER {cluster_arg.upper()}", *args)

    def eval(
        self, script: ScriptTextT, numkeys: int, *keys_and_args: EncodableT
    ) -> Awaitable:
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command("EVAL", script, numkeys, *keys_and_args)

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: str) -> Awaitable:
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command("EVALSHA", sha, numkeys, *keys_and_args)

    def script_exists(self, *args: str) -> Awaitable:
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        return self.execute_command("SCRIPT EXISTS", *args)

    def script_flush(self) -> Awaitable:
        """Flush all scripts from the script cache"""
        return self.execute_command("SCRIPT FLUSH")

    def script_kill(self) -> Awaitable:
        """Kill the currently executing Lua script"""
        return self.execute_command("SCRIPT KILL")

    def script_load(self, script: ScriptTextT) -> Awaitable:
        """Load a Lua ``script`` into the script cache. Returns the SHA."""
        return self.execute_command("SCRIPT LOAD", script)

    def register_script(self, script: ScriptTextT) -> "Script":
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)

    # GEO COMMANDS
    def geoadd(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.
        """
        if len(values) % 3 != 0:
            raise DataError("GEOADD requires places with lon, lat and name values")
        return self.execute_command("GEOADD", name, *values)

    def geodist(
        self, name: KeyT, place1: FieldT, place2: FieldT, unit: Optional[str] = None
    ) -> Awaitable:
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.
        """
        pieces: List[EncodableT] = [name, place1, place2]
        if unit and unit not in ("m", "km", "mi", "ft"):
            raise DataError("GEODIST invalid unit")
        elif unit:
            pieces.append(unit)
        return self.execute_command("GEODIST", *pieces)

    def geohash(self, name: KeyT, *values: FieldT) -> Awaitable:
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name`` argument.
        """
        return self.execute_command("GEOHASH", name, *values)

    def geopos(self, name: KeyT, *values: FieldT) -> Awaitable:
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name`` argument. Each position
        is represented by the pairs lon and lat.
        """
        return self.execute_command("GEOPOS", name, *values)

    def georadius(
        self,
        name: KeyT,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Optional[str] = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: Optional[int] = None,
        sort: Optional[str] = None,
        store: Optional[KeyT] = None,
        store_dist: Optional[KeyT] = None,
    ) -> Awaitable:
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.
        """
        return self._georadiusgeneric(
            "GEORADIUS",
            name,
            longitude,
            latitude,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            sort=sort,
            store=store,
            store_dist=store_dist,
        )

    def georadiusbymember(
        self,
        name: KeyT,
        member: FieldT,
        radius: float,
        unit: Optional[str] = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: Optional[int] = None,
        sort: Optional[str] = None,
        store: Optional[KeyT] = None,
        store_dist: Optional[KeyT] = None,
    ) -> Awaitable:
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.
        """
        return self._georadiusgeneric(
            "GEORADIUSBYMEMBER",
            name,
            member,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            sort=sort,
            store=store,
            store_dist=store_dist,
        )

    def _georadiusgeneric(
        self, command: str, *args: EncodableT, **kwargs: Optional[EncodableT]
    ) -> Awaitable:
        pieces: List[Optional[EncodableT]] = list(args)
        if kwargs["unit"] and kwargs["unit"] not in ("m", "km", "mi", "ft"):
            raise DataError("GEORADIUS invalid unit")
        elif kwargs["unit"]:
            pieces.append(kwargs["unit"])
        else:
            pieces.append(
                "m",
            )

        for arg_name, byte_repr in (
            ("withdist", b"WITHDIST"),
            ("withcoord", b"WITHCOORD"),
            ("withhash", b"WITHHASH"),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        if kwargs["count"]:
            pieces.extend([b"COUNT", kwargs["count"]])

        if kwargs["sort"]:
            if kwargs["sort"] == "ASC":
                pieces.append(b"ASC")
            elif kwargs["sort"] == "DESC":
                pieces.append(b"DESC")
            else:
                raise DataError("GEORADIUS invalid sort")

        if kwargs["store"] and kwargs["store_dist"]:
            raise DataError("GEORADIUS store and store_dist cant be set" " together")

        if kwargs["store"]:
            pieces.extend([b"STORE", kwargs["store"]])

        if kwargs["store_dist"]:
            pieces.extend([b"STOREDIST", kwargs["store_dist"]])

        return self.execute_command(command, *pieces, **kwargs)

    # MODULE COMMANDS
    def module_load(self, path: str) -> Awaitable:
        """
        Loads the module from ``path``.
        Raises ``ModuleError`` if a module is not found at ``path``.
        """
        return self.execute_command("MODULE LOAD", path)

    def module_unload(self, name: str) -> Awaitable:
        """
        Unloads the module ``name``.
        Raises ``ModuleError`` if ``name`` is not in loaded modules.
        """
        return self.execute_command("MODULE UNLOAD", name)

    def module_list(self) -> Awaitable:
        """
        Returns a list of dictionaries containing the name and version of
        all loaded modules.
        """
        return self.execute_command("MODULE LIST")


StrictRedis = Redis


class MonitorCommandInfo(TypedDict):
    time: float
    db: int
    client_address: str
    client_port: str
    client_type: str
    command: str


class Monitor:
    """
    Monitor is useful for handling the MONITOR command to the redis server.
    next_command() method returns one command from monitor
    listen() method yields commands from monitor.
    """

    monitor_re = re.compile(r"\[(\d+) (.*)\] (.*)")
    command_re = re.compile(r'"(.*?)(?<!\\)"')

    def __init__(self, connection_pool: ConnectionPool):
        self.connection_pool = connection_pool
        self.connection: Optional[Connection] = None

    async def connect(self):
        if self.connection is None:
            self.connection = await self.connection_pool.get_connection("MONITOR")

    async def __aenter__(self):
        await self.connect()
        self.connection = cast(Connection, self.connection)  # Connected above.
        await self.connection.send_command("MONITOR")
        # check that monitor returns 'OK', but don't return it to user
        response = await self.connection.read_response()
        if not bool_ok(response):
            raise RedisError(f"MONITOR failed: {response}")
        return self

    async def __aexit__(self, *args):
        assert self.connection is not None
        await self.connection.disconnect()
        await self.connection_pool.release(self.connection)

    async def next_command(self) -> MonitorCommandInfo:
        """Parse the response from a monitor command"""
        if self.connection is None:
            raise RedisError("Connection already closed.")
        await self.connect()
        response = await self.connection.read_response()
        if isinstance(response, bytes):
            response = self.connection.encoder.decode(response, force=True)
        command_time, command_data = response.split(" ", 1)
        m = self.monitor_re.match(command_data)
        if m is None:
            raise RedisError("Invalid command received.")
        db_id, client_info, command = m.groups()
        command = " ".join(self.command_re.findall(command))
        # Redis escapes double quotes because each piece of the command
        # string is surrounded by double quotes. We don't have that
        # requirement so remove the escaping and leave the quote.
        command = command.replace('\\"', '"')

        if client_info == "lua":
            client_address = "lua"
            client_port = ""
            client_type = "lua"
        elif client_info.startswith("unix"):
            client_address = "unix"
            client_port = client_info[5:]
            client_type = "unix"
        else:
            # use rsplit as ipv6 addresses contain colons
            client_address, client_port = client_info.rsplit(":", 1)
            client_type = "tcp"
        return {
            "time": float(command_time),
            "db": int(db_id),
            "client_address": client_address,
            "client_port": client_port,
            "client_type": client_type,
            "command": command,
        }

    async def listen(self) -> AsyncIterator[MonitorCommandInfo]:
        """Listen for commands coming to the server."""
        while True:
            yield await self.next_command()


class PubSub:
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """

    PUBLISH_MESSAGE_TYPES = ("message", "pmessage")
    UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe")
    HEALTH_CHECK_MESSAGE = "aioredis-py-health-check"

    def __init__(
        self,
        connection_pool: ConnectionPool,
        shard_hint: Optional[str] = None,
        ignore_subscribe_messages: bool = False,
    ):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection: Optional[Connection] = None
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = self.connection_pool.get_encoder()
        if self.encoder.decode_responses:
            self.health_check_response: Iterable[Union[str, bytes]] = [
                "pong",
                self.HEALTH_CHECK_MESSAGE,
            ]
        else:
            self.health_check_response = [
                b"pong",
                self.encoder.encode(self.HEALTH_CHECK_MESSAGE),
            ]
        self.channels: Dict[ChannelT, PubSubHandler] = {}
        self.pending_unsubscribe_channels: Set[ChannelT] = set()
        self.patterns: Dict[ChannelT, PubSubHandler] = {}
        self.pending_unsubscribe_patterns: Set[ChannelT] = set()
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.reset()

    def __del__(self):
        if self.connection:
            self.connection.clear_connect_callbacks()

    async def reset(self):
        async with self._lock:
            if self.connection:
                await self.connection.disconnect()
                self.connection.clear_connect_callbacks()
                await self.connection_pool.release(self.connection)
                self.connection = None
            self.channels = {}
            self.pending_unsubscribe_channels = set()
            self.patterns = {}
            self.pending_unsubscribe_patterns = set()

    def close(self) -> Awaitable[NoReturn]:
        return self.reset()

    async def on_connect(self, connection: Connection):
        """Re-subscribe to any channels and patterns previously subscribed to"""
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        if self.channels:
            channels = {}
            for k, v in self.channels.items():
                channels[self.encoder.decode(k, force=True)] = v
            await self.subscribe(**channels)
        if self.patterns:
            patterns = {}
            for k, v in self.patterns.items():
                patterns[self.encoder.decode(k, force=True)] = v
            await self.psubscribe(**patterns)

    @property
    def subscribed(self):
        """Indicates if there are subscriptions to any channels or patterns"""
        return bool(self.channels or self.patterns)

    async def execute_command(self, *args: EncodableT):
        """Execute a publish/subscribe command"""

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = await self.connection_pool.get_connection(
                "pubsub", self.shard_hint
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        kwargs = {"check_health": not self.subscribed}
        await self._execute(connection, connection.send_command, *args, **kwargs)

    async def _execute(self, connection, command, *args, **kwargs):
        try:
            return await command(*args, **kwargs)
        except (ConnectionError, TimeoutError) as e:
            await connection.disconnect()
            if not (connection.retry_on_timeout and isinstance(e, TimeoutError)):
                raise
            # Connect manually here. If the Redis server is down, this will
            # fail and raise a ConnectionError as desired.
            await connection.connect()
            # the ``on_connect`` callback should haven been called by the
            # connection to resubscribe us to any channels and patterns we were
            # previously listening to
            return await command(*args, **kwargs)

    async def parse_response(self, block: bool = True, timeout: float = 0):
        """Parse the response from a publish/subscribe command"""
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        await self.check_health()

        if not block and not await conn.can_read(timeout=timeout):
            return None
        response = await self._execute(conn, conn.read_response)

        if conn.health_check_interval and response == self.health_check_response:
            # ignore the health check message as user might not expect it
            return None
        return response

    async def check_health(self):
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        if (
            conn.health_check_interval
            and get_event_loop().time() > conn.next_health_check
        ):
            await conn.send_command(
                "PING", self.HEALTH_CHECK_MESSAGE, check_health=False
            )

    def _normalize_keys(self, data: _NormalizeKeysT) -> _NormalizeKeysT:
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in data.items()}  # type: ignore[return-value]

    async def psubscribe(self, *args: ChannelT, **kwargs: PubSubHandler):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        parsed_args = list_or_args((args[0],), args[1:]) if args else args
        new_patterns: Dict[ChannelT, PubSubHandler] = dict.fromkeys(parsed_args)
        # Mypy bug: https://github.com/python/mypy/issues/10970
        new_patterns.update(kwargs)  # type: ignore[arg-type]
        ret_val = await self.execute_command("PSUBSCRIBE", *new_patterns.keys())
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args: ChannelT) -> Awaitable:
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        patterns: Iterable[ChannelT]
        if args:
            parsed_args = list_or_args((args[0],), args[1:])
            patterns = self._normalize_keys(dict.fromkeys(parsed_args)).keys()
        else:
            parsed_args = []
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return self.execute_command("PUNSUBSCRIBE", *parsed_args)

    async def subscribe(self, *args: ChannelT, **kwargs: Callable):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        parsed_args = list_or_args((args[0],), args[1:]) if args else ()
        new_channels = dict.fromkeys(parsed_args)
        # Mypy bug: https://github.com/python/mypy/issues/10970
        new_channels.update(kwargs)  # type: ignore[arg-type]
        ret_val = await self.execute_command("SUBSCRIBE", *new_channels.keys())
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args) -> Awaitable:
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            parsed_args = list_or_args(args[0], args[1:])
            channels = self._normalize_keys(dict.fromkeys(parsed_args))
        else:
            parsed_args = []
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return self.execute_command("UNSUBSCRIBE", *parsed_args)

    async def listen(self) -> AsyncIterator:
        """Listen for messages on channels this client has been subscribed to"""
        while self.subscribed:
            response = self.handle_message(await self.parse_response(block=True))
            if response is not None:
                yield response

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number.
        """
        response = await self.parse_response(block=False, timeout=timeout)
        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    def ping(self, message=None) -> Awaitable:
        """
        Ping the Redis server
        """
        message = "" if message is None else message
        return self.execute_command("PING", message)

    def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        message_type = str_if_bytes(response[0])
        if message_type == "pmessage":
            message = {
                "type": message_type,
                "pattern": response[1],
                "channel": response[2],
                "data": response[3],
            }
        elif message_type == "pong":
            message = {
                "type": message_type,
                "pattern": None,
                "channel": None,
                "data": response[1],
            }
        else:
            message = {
                "type": message_type,
                "pattern": None,
                "channel": response[1],
                "data": response[2],
            }

        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == "punsubscribe":
                pattern = response[1]
                if pattern in self.pending_unsubscribe_patterns:
                    self.pending_unsubscribe_patterns.remove(pattern)
                    self.patterns.pop(pattern, None)
            else:
                channel = response[1]
                if channel in self.pending_unsubscribe_channels:
                    self.pending_unsubscribe_channels.remove(channel)
                    self.channels.pop(channel, None)

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == "pmessage":
                handler = self.patterns.get(message["pattern"], None)
            else:
                handler = self.channels.get(message["channel"], None)
            if handler:
                handler(message)
                return None
        elif message_type != "pong":
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    async def run(
        self,
        *,
        exception_handler: Optional["PSWorkerThreadExcHandlerT"] = None,
        poll_timeout: float = 1.0,
    ) -> None:
        """Process pub/sub messages using registered callbacks.

        This is the equivalent of :py:meth:`redis.PubSub.run_in_thread` in
        redis-py, but it is a coroutine. To launch it as a separate task, use
        ``asyncio.create_task``:

            >>> task = asyncio.create_task(pubsub.run())

        To shut it down, use asyncio cancellation:

            >>> task.cancel()
            >>> await task
        """
        for channel, handler in self.channels.items():
            if handler is None:
                raise PubSubError(f"Channel: '{channel}' has no handler registered")
        for pattern, handler in self.patterns.items():
            if handler is None:
                raise PubSubError(f"Pattern: '{pattern}' has no handler registered")

        while True:
            try:
                await self.get_message(
                    ignore_subscribe_messages=True, timeout=poll_timeout
                )
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                if exception_handler is None:
                    raise
                res = exception_handler(e, self)
                if inspect.isawaitable(res):
                    await res
            # Ensure that other tasks on the event loop get a chance to run
            # if we didn't have to block for I/O anywhere.
            await asyncio.sleep(0)


class PubsubWorkerExceptionHandler(Protocol):
    def __call__(self, e: BaseException, pubsub: PubSub):
        ...


class AsyncPubsubWorkerExceptionHandler(Protocol):
    async def __call__(self, e: BaseException, pubsub: PubSub):
        ...


PSWorkerThreadExcHandlerT = Union[
    PubsubWorkerExceptionHandler, AsyncPubsubWorkerExceptionHandler
]


CommandT = Tuple[Tuple[Union[str, bytes], ...], Mapping[str, Any]]
CommandStackT = List[CommandT]


class Pipeline(Redis):  # lgtm [py/init-calls-subclass]
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = {"DISCARD", "EXEC", "UNWATCH"}

    def __init__(
        self,
        connection_pool: ConnectionPool,
        response_callbacks: MutableMapping[Union[str, bytes], ResponseCallbackT],
        transaction: bool,
        shard_hint: Optional[str],
    ):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.is_transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.command_stack: CommandStackT = []
        self.scripts: Set[Script] = set()
        self.explicit_transaction = False

    async def __aenter__(self: _RedisT) -> _RedisT:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.reset()

    def __await__(self):
        return self._async_self().__await__()

    def __del__(self):
        try:
            create_task_or_run(self.reset())
            super().__del__()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)

    def __bool__(self):
        """Pipeline instances should always evaluate to True"""
        return True

    async def _async_self(self):
        return self

    async def reset(self):
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                await self.connection.send_command("UNWATCH")
                await self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                if self.connection:
                    await self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            await self.connection_pool.release(self.connection)
            self.connection = None

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self.command_stack:
            raise RedisError(
                "Commands without an initial WATCH have already " "been issued"
            )
        self.explicit_transaction = True

    def execute_command(
        self, *args, **kwargs
    ) -> Union["Pipeline", Awaitable["Pipeline"]]:
        if (self.watching or args[0] == "WATCH") and not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    async def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on a
        ConnectionError if we're already WATCHing a variable. Used when
        issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = await self.connection_pool.get_connection(
                command_name, self.shard_hint
            )
            self.connection = conn
        conn = cast(Connection, conn)
        try:
            await conn.send_command(*args)
            return await self.parse_response(conn, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            await conn.disconnect()
            # if we were already watching a variable, the watch is no longer
            # valid since this connection has died. raise a WatchError, which
            # indicates the user should retry this transaction.
            if self.watching:
                await self.reset()
                raise WatchError(
                    "A ConnectionError occurred on while watching one or more keys"
                ) from e
            # if retry_on_timeout is not set, or the error is not
            # a TimeoutError, raise it
            if not (conn.retry_on_timeout and isinstance(e, TimeoutError)):
                await self.reset()
                raise

            # retry_on_timeout is set, this is a TimeoutError and we are not
            # already WATCHing any variables. retry the command.
            try:
                await conn.send_command(*args)
                return self.parse_response(conn, command_name, **options)
            except (ConnectionError, TimeoutError):
                # a subsequent failure should simply be raised
                await self.reset()
                raise
        except asyncio.CancelledError:
            await conn.disconnect()
            raise

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self.command_stack.append((args, options))
        return self

    async def _execute_transaction(  # noqa: C901
        self, connection: Connection, commands: CommandStackT, raise_on_error
    ):
        pre: CommandT = (("MULTI",), {})
        post: CommandT = (("EXEC",), {})
        cmds = (pre, *commands, post)
        all_cmds = connection.pack_commands(
            args for args, options in cmds if EMPTY_RESPONSE not in options
        )
        await connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await self.parse_response(connection, "_")
        except ResponseError as err:
            errors.append((0, err))

        # and all the other commands
        for i, command in enumerate(commands):
            if EMPTY_RESPONSE in command[1]:
                errors.append((i, command[1][EMPTY_RESPONSE]))
            else:
                try:
                    await self.parse_response(connection, "_")
                except ResponseError as err:
                    self.annotate_exception(err, i + 1, command[0])
                    errors.append((i, err))

        # parse the EXEC.
        try:
            response = await self.parse_response(connection, "_")
        except ExecAbortError as err:
            if errors:
                raise errors[0][1] from err
            raise

        # EXEC clears any watched keys
        self.watching = False

        if response is None:
            raise WatchError("Watched variable changed.") from None

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            if self.connection:
                await self.connection.disconnect()
            raise ResponseError(
                "Wrong number of response items from pipeline execution"
            ) from None

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
                    if inspect.isawaitable(r):
                        r = await r
            data.append(r)
        return data

    async def _execute_pipeline(
        self, connection: Connection, commands: CommandStackT, raise_on_error: bool
    ):
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands([args for args, _ in commands])
        await connection.send_packed_command(all_cmds)

        response = []
        for args, options in commands:
            try:
                response.append(
                    await self.parse_response(connection, args[0], **options)
                )
            except ResponseError as e:
                response.append(e)

        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands: CommandStackT, response: Iterable[Any]):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(
        self, exception: Exception, number: int, command: Iterable[object]
    ) -> None:
        cmd = " ".join(map(safe_str, command))
        msg = f"Command # {number} ({cmd}) of pipeline caused error: {exception.args}"
        exception.args = (msg,) + exception.args[1:]

    def parse_response(
        self, connection: Connection, command_name: Union[str, bytes], **options
    ):
        result = super().parse_response(connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == "WATCH":
            self.watching = True
        return result

    async def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = await immediate("SCRIPT EXISTS", *shas)
        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await immediate("SCRIPT LOAD", s.script)

    async def execute(self, raise_on_error: bool = True):
        """Execute all the commands in the current pipeline"""
        stack = self.command_stack
        if not stack and not self.watching:
            return []
        if self.scripts:
            await self.load_scripts()
        if self.is_transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = await self.connection_pool.get_connection("MULTI", self.shard_hint)
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn
        conn = cast(Connection, conn)

        try:
            return await execute(conn, stack, raise_on_error)
        except (ConnectionError, TimeoutError) as e:
            await conn.disconnect()
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry this transaction.
            if self.watching:
                raise WatchError(
                    "A ConnectionError occurred on while " "watching one or more keys"
                ) from e
            # if retry_on_timeout is not set, or the error is not
            # a TimeoutError, raise it
            if not (conn.retry_on_timeout and isinstance(e, TimeoutError)):
                raise
            # retry a TimeoutError when retry_on_timeout is set
            return await execute(conn, stack, raise_on_error)
        finally:
            await self.reset()

    async def watch(self, *names: KeyT):
        """Watches the values at keys ``names``"""
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        return await self.execute_command("WATCH", *names)

    async def unwatch(self):
        """Unwatches all previously specified keys"""
        return self.watching and await self.execute_command("UNWATCH") or True


class Script:
    """An executable Lua script object returned by ``register_script``"""

    def __init__(self, registered_client: Redis, script: ScriptTextT):
        self.registered_client = registered_client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, str):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            encoder = registered_client.connection_pool.get_encoder()
            script_bytes = encoder.encode(script)
        else:
            script_bytes = script
        self.sha = hashlib.sha1(script_bytes).hexdigest()

    async def __call__(
        self,
        keys: Optional[Sequence[KeyT]] = None,
        args: Optional[Iterable[EncodableT]] = None,
        client: Optional[Redis] = None,
    ):
        """Execute the script, passing any required ``args``"""
        keys = keys or []
        args = args or []
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
            return client.evalsha(self.sha, len(keys), *args)
        try:
            return await client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a differnet server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = await client.script_load(self.script)
            return await client.evalsha(self.sha, len(keys), *args)


class BitFieldOperation:
    """
    Command builder for BITFIELD commands.
    """

    def __init__(
        self, client: Redis, key: KeyT, default_overflow: Optional[str] = None
    ):
        self.client = client
        self.key = key
        self._default_overflow = default_overflow
        self.operations: List[Tuple[EncodableT, ...]] = []
        self._last_overflow = "WRAP"
        self.reset()

    def reset(self):
        """
        Reset the state of the instance to when it was constructed
        """
        self.operations = []
        self._last_overflow = "WRAP"
        self.overflow(self._default_overflow or self._last_overflow)

    def overflow(self, overflow: str):
        """
        Update the overflow algorithm of successive INCRBY operations
        :param overflow: Overflow algorithm, one of WRAP, SAT, FAIL. See the
            Redis docs for descriptions of these algorithmsself.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        overflow = overflow.upper()
        if overflow != self._last_overflow:
            self._last_overflow = overflow
            self.operations.append(("OVERFLOW", overflow))
        return self

    def incrby(
        self,
        fmt: str,
        offset: BitfieldOffsetT,
        increment: int,
        overflow: Optional[str] = None,
    ):
        """
        Increment a bitfield by a given amount.
        :param fmt: format-string for the bitfield being updated, e.g. 'u8'
            for an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int increment: value to increment the bitfield by.
        :param str overflow: overflow algorithm. Defaults to WRAP, but other
            acceptable values are SAT and FAIL. See the Redis docs for
            descriptions of these algorithms.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        if overflow is not None:
            self.overflow(overflow)

        self.operations.append(("INCRBY", fmt, offset, increment))
        return self

    def get(self, fmt: str, offset: BitfieldOffsetT):
        """
        Get the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(("GET", fmt, offset))
        return self

    def set(self, fmt: str, offset: BitfieldOffsetT, value: int):
        """
        Set the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int value: value to set at the given position.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(("SET", fmt, offset, value))
        return self

    @property
    def command(self):
        cmd: List[EncodableT] = ["BITFIELD", self.key]
        for ops in self.operations:
            cmd.extend(ops)
        return cmd

    def execute(self):
        """
        Execute the operation(s) in a single BITFIELD command. The return value
        is a list of values corresponding to each operation. If the client
        used to create this instance was a pipeline, the list of values
        will be present within the pipeline's execute.
        """
        command = self.command
        self.reset()
        return self.client.execute_command(*command)
