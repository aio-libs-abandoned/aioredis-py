from __future__ import annotations

import datetime
import hashlib
import time as mod_time
import warnings
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)

from aioredis.compat import Literal
from aioredis.exceptions import ConnectionError, DataError, NoScriptError, RedisError
from aioredis.typing import (
    AbsExpiryT,
    AnyFieldT,
    AnyKeyT,
    BitfieldOffsetT,
    ChannelT,
    CommandsProtocol,
    ConsumerT,
    EncodableT,
    ExpiryT,
    FieldT,
    GroupT,
    KeysT,
    KeyT,
    PatternT,
    ScriptTextT,
    StreamIdT,
    TimeoutSecT,
    ZScoreBoundT,
)
from aioredis.utils import EMPTY_RESPONSE

from .helpers import list_or_args

if TYPE_CHECKING:
    from aioredis.client import Redis


class CoreCommands(CommandsProtocol):
    """
    A class containing all of the implemented redis commands. This class is
    to be used as a mixin.
    """

    # SERVER INFORMATION

    # ACL methods

    def acl_cat(self, category: str | None = None) -> Awaitable:
        """
        Returns a list of categories or commands within a category.

        If ``category`` is not supplied, returns a list of all categories.
        If ``category`` is supplied, returns a list of all commands within
        that category.
        """
        pieces: list[EncodableT] = [category] if category else []
        return self.execute_command("ACL CAT", *pieces)

    def acl_deluser(self, *username: str) -> Awaitable:
        """Delete the ACL for the specified ``username``s"""
        return self.execute_command("ACL DELUSER", *username)

    def acl_genpass(self, bits: int | None = None) -> Awaitable:
        """Generate a random password value.
        If ``bits`` is supplied then use this number of bits, rounded to
        the next multiple of 4.
        See: https://redis.io/commands/acl-genpass
        """
        pieces = []
        if bits is not None:
            try:
                b = int(bits)
                if b < 0 or b > 4096:
                    raise ValueError
            except ValueError:
                raise DataError(
                    "genpass optionally accepts a bits argument, between 0 and 4096."
                )
        return self.execute_command("ACL GENPASS", *pieces)

    def acl_getuser(self, username: str) -> Awaitable:
        """
        Get the ACL details for the specified ``username``.

        If ``username`` does not exist, return None
        """
        return self.execute_command("ACL GETUSER", username)

    def acl_help(self) -> Awaitable:
        """The ACL HELP command returns helpful text describing
        the different subcommands.
        """
        return self.execute_command("ACL HELP")

    def acl_list(self) -> Awaitable:
        """Return a list of all ACLs on the server"""
        return self.execute_command("ACL LIST")

    def acl_log(self, count: int | None = None) -> Awaitable:
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
        passwords: str | Iterable[str] | None = None,
        hashed_passwords: str | Iterable[str] | None = None,
        categories: Iterable[str] | None = None,
        commands: Iterable[str] | None = None,
        keys: Iterable[KeyT] | None = None,
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
        pieces: list[str | bytes] = [username]

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
            passwords = list_or_args(passwords, [])
            for i, password in enumerate(passwords):
                password = encoder.encode(password)
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
            hashed_passwords = list_or_args(hashed_passwords, [])
            for i, hashed_password in enumerate(hashed_passwords):
                hashed_password = encoder.encode(hashed_password)
                if hashed_password.startswith(b"+"):
                    pieces.append(b"#%s" % hashed_password[1:])
                elif hashed_password.startswith(b"-"):
                    pieces.append(b"!%s" % hashed_password[1:])
                else:
                    raise DataError(
                        f"Hashed {i} password must be prefixeed "
                        'with a "+" to add or a "-" to remove'
                    )

        if nopass:
            pieces.append(b"nopass")

        if categories:
            for category in categories:
                category = encoder.encode(category)
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
            for cmd in commands:
                cmd = encoder.encode(cmd)
                if not cmd.startswith(b"+") and not cmd.startswith(b"-"):
                    raise DataError(
                        f'Command "{encoder.decode(cmd, force=True)}" must be '
                        'prefixed with "+" or "-"'
                    )
                pieces.append(cmd)

        if keys:
            for key in keys:
                key = encoder.encode(key)
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

    def bgsave(self, schedule: bool = True) -> Awaitable:
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        pieces = []
        if schedule:
            pieces.append("SCHEDULE")
        return self.execute_command("BGSAVE", *pieces)

    def client_kill(self, address: str) -> Awaitable:
        """Disconnects the client at ``address`` (ip:port)"""
        return self.execute_command("CLIENT KILL", address)

    def client_kill_filter(
        self,
        _id: str | None = None,
        _type: str | None = None,
        addr: str | None = None,
        skipme: bool | None = None,
        laddr: bool | None = None,
        user: str = None,
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
        :param laddr: Kills a client by its 'local (bind)  address:port'
        :param user: Kills a client for a specific user name
        """
        args = []
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
        if laddr is not None:
            args.extend((b"LADDR", laddr))
        if user is not None:
            args.extend((b"USER", user))
        if not args:
            raise DataError(
                "CLIENT KILL <filter> <value> ... ... <filter> "
                "<value> must specify at least one filter"
            )
        return self.execute_command("CLIENT KILL", *args)

    def client_info(self) -> Awaitable:
        """
        Returns information and statistics about the current
        client connection.
        """
        return self.execute_command("CLIENT INFO")

    def client_list(
        self,
        _type: str | None = None,
        client_id: list[EncodableT] | None = None,
    ) -> Awaitable:
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        :param client_id: optional. the client id
        """
        "Returns a list of currently connected clients"
        args = []
        if _type is not None:
            client_types = ("normal", "master", "replica", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT LIST _type must be one of {client_types!r}")
            args.append(b"TYPE")
            args.append(_type)
        if client_id is None:
            client_id = []
        if not isinstance(client_id, list):
            raise DataError("client_id must be a list")
        if client_id:
            args.append(b"ID")
            args.append(" ".join(client_id))
        return self.execute_command("CLIENT LIST", *args)

    def client_getname(self) -> Awaitable:
        """Returns the current connection name"""
        return self.execute_command("CLIENT GETNAME")

    def client_getredir(self):
        """Returns the ID (an integer) of the client to whom we are
        redirecting tracking notifications.

        see: https://redis.io/commands/client-getredir
        """
        return self.execute_command("CLIENT GETREDIR")

    def client_reply(
        self,
        reply: Literal["ON"] | Literal["OFF"] | Literal["SKIP"],
    ) -> Awaitable:
        """Enable and disable redis server replies.
        ``reply`` Must be ON OFF or SKIP,
            ON - The default most with server replies to commands
            OFF - Disable server responses to commands
            SKIP - Skip the response of the immediately following command.

        Note: When setting OFF or SKIP replies, you will need a client object
        with a timeout specified in seconds, and will need to catch the
        TimeoutError.
              The test_client_reply unit test illustrates this, and
              conftest.py has a client with a timeout.
        See https://redis.io/commands/client-reply
        """
        replies = ["ON", "OFF", "SKIP"]
        if reply not in replies:
            raise DataError("CLIENT REPLY must be one of %r" % replies)
        return self.execute_command("CLIENT REPLY", reply)

    def client_id(self) -> Awaitable:
        """Returns the current connection id"""
        return self.execute_command("CLIENT ID")

    def client_trackinginfo(self) -> Awaitable:
        """Returns the information about the current client connection's
        use of the server assisted client side cache.
        See https://redis.io/commands/client-trackinginfo
        """
        return self.execute_command("CLIENT TRACKINGINFO")

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

    def client_unpause(self) -> Awaitable:
        """
        Unpause all redis clients
        """
        return self.execute_command("CLIENT UNPAUSE")

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

    def debug_segfault(self):
        raise NotImplementedError(
            "DEBUG SEGFAULT is intentionally not implemented in the client."
        )

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

    def info(self, section: str | None = None) -> Awaitable:
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

    def lolwut(self, *version_numbers: str | float) -> Awaitable:
        """Get the Redis version and a piece of generative computer art
        See: https://redis.io/commands/lolwut
        """
        if version_numbers:
            return self.execute_command("LOLWUT VERSION", *version_numbers)
        else:
            return self.execute_command("LOLWUT")

    def migrate(
        self,
        host: str,
        port: int,
        keys: KeysT,
        destination_db: int,
        timeout: int,
        copy: bool = False,
        replace: bool = False,
        auth: str | None = None,
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
        pieces: list[EncodableT] = []
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

    def memory_doctor(self):
        raise NotImplementedError(
            "MEMORY DOCTOR is intentionally not implemented in the client."
        )

    def memory_help(self):
        raise NotImplementedError(
            "MEMORY HELP is intentionally not implemented in the client."
        )

    def memory_stats(self):
        """Return a dictionary of memory stats"""
        return self.execute_command("MEMORY STATS")

    def memory_malloc_stats(self):
        """Return an internal statistics report from the memory allocator."""
        return self.execute_command("MEMORY MALLOC-STATS")

    def memory_usage(
        self,
        key: KeyT,
        samples: int | None = None,
    ) -> Awaitable:
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

    def quit(self) -> Awaitable:
        """Ask the server to close the connection.
        https://redis.io/commands/quit
        """
        return self.execute_command("QUIT")

    def save(self) -> Awaitable:
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command("SAVE")

    def shutdown(
        self,
        save: bool = False,
        nosave: bool = False,
    ) -> Awaitable | None:
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
        self,
        host: str | None = None,
        port: int | None = None,
    ) -> Awaitable:
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.
        """
        if host is None and port is None:
            return self.execute_command("SLAVEOF", b"NO", b"ONE")
        return self.execute_command("SLAVEOF", host, port)

    def slowlog_get(self, num: int | None = None) -> Awaitable:
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args: list[EncodableT] = ["SLOWLOG GET"]
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
        self,
        key: KeyT,
        start: int | None = None,
        end: int | None = None,
    ) -> Awaitable:
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` parameters indicate which bytes to consider
        """
        params: list[EncodableT] = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or (end is not None and start is None):
            raise DataError("Both start and end must be specified")
        return self.execute_command("BITCOUNT", *params)

    def bitfield(
        self,
        key: KeyT,
        default_overflow: str | None = None,
    ) -> BitFieldOperation:
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.
        """
        return BitFieldOperation(self, key, default_overflow=default_overflow)

    def bitop(
        self,
        operation: str,
        dest: KeyT,
        *keys: KeyT,
    ) -> Awaitable:
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        return self.execute_command("BITOP", operation, dest, *keys)

    def bitpos(
        self,
        key: KeyT,
        bit: int,
        start: int | None = None,
        end: int | None = None,
    ) -> Awaitable:
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` defines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """
        if bit not in (0, 1):
            raise DataError("bit must be 0 or 1")
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise DataError("start argument is not set, " "when end is specified")
        return self.execute_command("BITPOS", *params)

    def copy(
        self,
        source: str,
        destination: str,
        destination_db: str | None = None,
        replace: bool = False,
    ) -> Awaitable:
        """
        Copy the value stored in the ``source`` key to the ``destination`` key.

        ``destination_db`` an alternative destination database. By default,
        the ``destination`` key is created in the source Redis database.

        ``replace`` whether the ``destination`` key should be removed before
        copying the value to it. By default, the value is not copied if
        the ``destination`` key already exists.
        """
        params = [source, destination]
        if destination_db is not None:
            params.extend(["DB", destination_db])
        if replace:
            params.append("REPLACE")
        return self.execute_command("COPY", *params)

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

    def getdel(self, name: KeyT) -> Awaitable:
        """
        Get the value at key ``name`` and delete the key. This command
        is similar to GET, except for the fact that it also deletes
        the key on success (if and only if the key's value type
        is a string).
        """
        return self.execute_command("GETDEL", name)

    def getex(
        self,
        name: KeyT,
        ex: ExpiryT | None = None,
        px: ExpiryT | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ):
        """
        Get the value of key and optionally set its expiration.
        GETEX is similar to GET, but is a write command with
        additional options.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
        specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
        specified in unix time.

        ``persist`` remove the time to live associated with ``name``.
        """

        opset = {ex, px, exat, pxat}
        if len(opset) > 2 or len(opset) > 1 and persist:
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``persist`` are mutually exclusive."
            )

        pieces = []
        # similar to set command
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
        # similar to pexpireat command
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(mod_time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(mod_time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)
        if persist:
            pieces.append("PERSIST")

        return self.execute_command("GETEX", name, *pieces)

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

        As per Redis 6.2, GETSET is considered deprecated.
        Please use SET with GET parameter in new code.
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

    def lmove(
        self,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> Awaitable:
        """
        Atomically returns and removes the first/last element of a list,
        pushing it as the first/last element on the destination list.
        Returns the element being popped and pushed.

        ``src`` and ``dest`` can be right/left or left/right in any casing.
        """
        params = [first_list, second_list, src, dest]
        return self.execute_command("LMOVE", *params)

    def blmove(
        self,
        first_list: str,
        second_list: str,
        timeout: int,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> Awaitable:
        """
        Blocking version of lmove.
        """
        params = [first_list, second_list, src, dest, timeout]
        return self.execute_command("BLMOVE", *params)

    def mget(self, keys: KeysT, *args: EncodableT) -> Awaitable:
        """
        Returns a list of values ordered identically to ``keys``
        """
        args = list_or_args(keys, args)
        options: dict[str, EncodableT | Iterable[EncodableT]] = {}
        if not args:
            options[EMPTY_RESPONSE] = []
        return self.execute_command("MGET", *args, **options)

    def mset(self, mapping: Mapping[AnyKeyT, EncodableT]) -> Awaitable:
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().
        """
        items: list[EncodableT] = []
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
        items: list[EncodableT] = []
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

    def psetex(
        self,
        name: KeyT,
        time_ms: ExpiryT,
        value: EncodableT,
    ) -> Awaitable:
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

    def hrandfield(
        self,
        key: str,
        count: int = None,
        withvalues: bool = False,
    ) -> Awaitable:
        """
        Return a random field from the hash value stored at key.

        count: if the argument is positive, return an array of distinct fields.
        If called with a negative count, the behavior changes and the command
        is allowed to return the same field multiple times. In this case,
        the number of returned fields is the absolute value of the
        specified count.
        withvalues: The optional WITHVALUES modifier changes the reply so it
        includes the respective values of the randomly selected hash fields.
        """
        params = []
        if count is not None:
            params.append(count)
        if withvalues:
            params.append("WITHVALUES")

        return self.execute_command("HRANDFIELD", key, *params)

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
        idletime: int | None = None,
        frequency: int | None = None,
    ) -> Awaitable:
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.

        ``replace`` allows an existing key on ``name`` to be overridden. If
        it's not specified an error is raised on collision.

        ``absttl`` if True, specified ``ttl`` should represent an absolute Unix
        timestamp in milliseconds in which the key will expire. (Redis 5.0 or
        greater).

        ``idletime`` Used for eviction, this is the number of seconds the
        key must be idle, prior to execution.

        ``frequency`` Used for eviction, this is the frequency counter of
        the object stored at the key, prior to execution.
        """
        params = [name, ttl, value]
        if replace:
            params.append("REPLACE")
        if absttl:
            params.append("ABSTTL")
        if idletime is not None:
            params.append("IDLETIME")
            try:
                params.append(int(idletime))
            except ValueError:
                raise DataError("idletimemust be an integer")

        if frequency is not None:
            params.append("FREQ")
            try:
                params.append(int(frequency))
            except ValueError:
                raise DataError("frequency must be an integer")
        return self.execute_command("RESTORE", *params)

    def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: ExpiryT | None = None,
        px: ExpiryT | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
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

        ``get`` if True, set the value at key ``name`` to ``value`` and return
            the old value stored at key, or None if the key did not exist.
            (Available since Redis 6.2)

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
            specified in unix time.
        """
        pieces: list[EncodableT] = [name, value]
        options = {}
        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            else:
                raise DataError("ex must be datetime.timedelta or int")
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(mod_time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(mod_time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)
        if keepttl:
            pieces.append("KEEPTTL")

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if get:
            pieces.append("GET")
            options["get"] = True

        return self.execute_command("SET", *pieces, **options)

    def setbit(self, name: KeyT, offset: int, value: int) -> Awaitable:
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.execute_command("SETBIT", name, offset, value)

    def setex(
        self,
        name: KeyT,
        time: ExpiryT,
        value: EncodableT,
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

    def setrange(
        self,
        name: KeyT,
        offset: int,
        value: EncodableT,
    ) -> Awaitable:
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

    def stralgo(
        self,
        algo: Literal["LCS"],
        value1: KeyT,
        value2: KeyT,
        specific_argument: Literal["strings"] | Literal["keys"] = "strings",
        len: bool = False,
        idx: bool = False,
        minmatchlen: int | None = None,
        withmatchlen: bool = False,
    ) -> Awaitable:
        """
        Implements complex algorithms that operate on strings.
        Right now the only algorithm implemented is the LCS algorithm
        (longest common substring). However new algorithms could be
        implemented in the future.
        ``algo`` Right now must be LCS
        ``value1`` and ``value2`` Can be two strings or two keys
        ``specific_argument`` Specifying if the arguments to the algorithm
        will be keys or strings. strings is the default.
        ``len`` Returns just the len of the match.
        ``idx`` Returns the match positions in each string.
        ``minmatchlen`` Restrict the list of matches to the ones of a given
        minimal length. Can be provided only when ``idx`` set to True.
        ``withmatchlen`` Returns the matches with the len of the match.
        Can be provided only when ``idx`` set to True.
        """
        # check validity
        supported_algo = ["LCS"]
        if algo not in supported_algo:
            raise DataError(
                "The supported algorithms are: %s" % (", ".join(supported_algo))
            )
        if specific_argument not in ["keys", "strings"]:
            raise DataError("specific_argument can be only" " keys or strings")
        if len and idx:
            raise DataError("len and idx cannot be provided together.")

        pieces = [algo, specific_argument.upper(), value1, value2]
        if len:
            pieces.append(b"LEN")
        if idx:
            pieces.append(b"IDX")
        try:
            int(minmatchlen)
            pieces.extend([b"MINMATCHLEN", minmatchlen])
        except TypeError:
            pass
        if withmatchlen:
            pieces.append(b"WITHMATCHLEN")

        return self.execute_command(
            "STRALGO",
            *pieces,
            len=len,
            idx=idx,
            minmatchlen=minmatchlen,
            withmatchlen=withmatchlen,
        )

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
    def blpop(self, keys: KeysT, timeout: TimeoutSecT = 0) -> Awaitable:
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BLPOP", *keys)

    def brpop(self, keys: KeysT, timeout: TimeoutSecT = 0) -> Awaitable:
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BRPOP", *keys)

    def brpoplpush(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: TimeoutSecT = 0,
    ) -> Awaitable:
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        if timeout is None:
            timeout = 0
        return self.execute_command("BRPOPLPUSH", src, dst, timeout)

    def lindex(self, name: KeyT, index: int) -> Awaitable:
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command("LINDEX", name, index)

    def linsert(
        self,
        name: KeyT,
        where: str,
        refvalue: EncodableT,
        value: EncodableT,
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

    def lpop(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable:
        """
        Removes and returns the first elements of the list ``name``.

        By default, the command pops a single element from the beginning of
        the list. When provided with the optional ``count`` argument, the reply
        will consist of up to count elements, depending on the list's length.
        """
        if count is not None:
            return self.execute_command("LPOP", name, count)
        else:
            return self.execute_command("LPOP", name)

    def lpush(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Push ``values`` onto the head of the list ``name``"""
        return self.execute_command("LPUSH", name, *values)

    def lpushx(self, name: KeyT, *values: EncodableT) -> Awaitable:
        """Push ``value`` onto the head of the list ``name`` if ``name`` exists"""
        return self.execute_command("LPUSHX", name, *values)

    def lrange(self, name: KeyT, start: int, end: int) -> Awaitable:
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command("LRANGE", name, start, end)

    def lrem(
        self,
        name: KeyT,
        count: int,
        value: EncodableT,
    ) -> Awaitable:
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return self.execute_command("LREM", name, count, value)

    def lset(
        self,
        name: KeyT,
        index: int,
        value: EncodableT,
    ) -> Awaitable:
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

    def rpop(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable:
        """
        Removes and returns the last elements of the list ``name``.

        By default, the command pops a single element from the end of the list.
        When provided with the optional ``count`` argument, the reply will
        consist of up to count elements, depending on the list's length.
        """
        if count is not None:
            return self.execute_command("RPOP", name, count)
        else:
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
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
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
        pieces: list[EncodableT] = [name, value]
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
        start: int | None = None,
        num: int | None = None,
        by: KeyT | None = None,
        get: KeysT | None = None,
        desc: bool = False,
        alpha: bool = False,
        store: KeyT | None = None,
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

        pieces: list[EncodableT] = [name]
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
                pieces.extend([b"GET", get])
            else:
                for g in get:
                    pieces.extend([b"GET", g])
        if desc:
            pieces.append(b"DESC")
        if alpha:
            pieces.append(b"ALPHA")
        if store is not None:
            pieces.extend([b"STORE", store])
        if groups:
            if not get or isinstance(get, (bytes, str)) or len(get) < 2:
                raise DataError(
                    'when using "groups" the "get" argument '
                    "must be specified and contain at least "
                    "two keys"
                )

        options = {"groups": len(get) if groups else None}
        return self.execute_command("SORT", *pieces, **options)

    # SCAN COMMANDS
    def scan(
        self,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
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
        pieces: list[EncodableT] = [cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        if _type is not None:
            pieces.extend([b"TYPE", _type])
        return self.execute_command("SCAN", *pieces)

    async def scan_iter(
        self,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
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
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.scan(
                cursor=cursor, match=match, count=count, _type=_type
            )
            for d in data:
                yield d

    def sscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> Awaitable:
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces: list[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        return self.execute_command("SSCAN", *pieces)

    async def sscan_iter(
        self,
        name: KeyT,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> AsyncIterator:
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.sscan(
                name, cursor=cursor, match=match, count=count
            )
            for d in data:
                yield d

    def hscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> Awaitable:
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces: list[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        return self.execute_command("HSCAN", *pieces)

    async def hscan_iter(
        self,
        name: str,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> AsyncIterator:
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.hscan(
                name, cursor=cursor, match=match, count=count
            )
            for it in data.items():
                yield it

    def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        score_cast_func: type | Callable = float,
    ) -> Awaitable:
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces: list[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        options = {"score_cast_func": score_cast_func}
        return self.execute_command("ZSCAN", *pieces, **options)

    async def zscan_iter(
        self,
        name: KeyT,
        match: PatternT | None = None,
        count: int | None = None,
        score_cast_func: type | Callable = float,
    ) -> AsyncIterator:
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.zscan(
                name,
                cursor=cursor,
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
        args = list_or_args(keys, args)
        return self.execute_command("SDIFF", *args)

    def sdiffstore(
        self,
        dest: KeyT,
        keys: KeysT,
        *args: EncodableT,
    ) -> Awaitable:
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command("SDIFFSTORE", dest, *args)

    def sinter(self, keys: KeysT, *args: EncodableT) -> Awaitable:
        """Return the intersection of sets specified by ``keys``"""
        args = list_or_args(keys, args)
        return self.execute_command("SINTER", *args)

    def sinterstore(
        self,
        dest: KeyT,
        keys: KeysT,
        *args: EncodableT,
    ) -> Awaitable:
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command("SINTERSTORE", dest, *args)

    def sismember(self, name: KeyT, value: EncodableT) -> Awaitable:
        """Return a boolean indicating if ``value`` is a member of set ``name``"""
        return self.execute_command("SISMEMBER", name, value)

    def smembers(self, name: KeyT) -> Awaitable:
        """Return all members of the set ``name``"""
        return self.execute_command("SMEMBERS", name)

    def smismember(self, name, values, *args):
        """
        Return whether each value in ``values`` is a member of the set ``name``
        as a list of ``bool`` in the order of ``values``
        """
        args = list_or_args(values, args)
        return self.execute_command("SMISMEMBER", name, *args)

    def smove(
        self,
        src: KeyT,
        dst: KeyT,
        value: EncodableT,
    ) -> Awaitable:
        """Move ``value`` from set ``src`` to set ``dst`` atomically"""
        return self.execute_command("SMOVE", src, dst, value)

    def spop(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable:
        """Remove and return a random member of set ``name``"""
        args = (count is not None) and [count] or []
        return self.execute_command("SPOP", name, *args)

    def srandmember(
        self,
        name: KeyT,
        number: int | None = None,
    ) -> Awaitable:
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
        args = list_or_args(keys, args)
        return self.execute_command("SUNION", *args)

    def sunionstore(
        self,
        dest: KeyT,
        keys: KeysT,
        *args: EncodableT,
    ) -> Awaitable:
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command("SUNIONSTORE", dest, *args)

    # STREAMS COMMANDS
    def xack(
        self,
        name: KeyT,
        groupname: GroupT,
        *ids: StreamIdT,
    ) -> Awaitable:
        """
        Acknowledges the successful processing of one or more messages.
        name: name of the stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowledge.
        """
        return self.execute_command("XACK", name, groupname, *ids)

    def xadd(
        self,
        name: KeyT,
        fields: dict[FieldT, EncodableT],
        id: StreamIdT = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: StreamIdT | None = None,
        limit: int | None = None,
    ) -> Awaitable:
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size.
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        nomkstream: When set to true, do not make a stream
        minid: the minimum id in the stream to query.
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve
        """
        pieces: list[EncodableT] = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ```maxlen``` or ```minid``` may be specified")
        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 1:
                raise DataError("XADD maxlen must be a positive integer")
            pieces.append(b"MAXLEN")
            if approximate:
                pieces.append(b"~")
            pieces.append(str(maxlen))
        if minid is not None:
            pieces.append(b"MINID")
            if approximate:
                pieces.append(b"~")
            pieces.append(minid)
        if limit is not None:
            pieces.append(b"LIMIT")
            pieces.append(limit)
        if nomkstream:
            pieces.append(b"NOMKSTREAM")
        pieces.append(id)
        if not isinstance(fields, dict) or len(fields) == 0:
            raise DataError("XADD fields must be a non-empty dict")
        for pair in fields.items():
            pieces.extend(pair)
        return self.execute_command("XADD", name, *pieces)

    def xautoclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        start_id: int = 0,
        count: int | None = None,
        justid: bool = False,
    ) -> Awaitable:
        """
        Transfers ownership of pending stream entries that match the specified
        criteria. Conceptually, equivalent to calling XPENDING and then XCLAIM,
        but provides a more straightforward way to deal with message delivery
        failures via SCAN-like semantics.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds.
        start_id: filter messages with equal or greater ID.
        count: optional integer, upper limit of the number of entries that the
        command attempts to claim. Set to 100 by default.
        justid: optional boolean, false by default. Return just an array of IDs
        of messages successfully claimed, without returning the actual message
        """
        try:
            if int(min_idle_time) < 0:
                raise DataError(
                    "XAUTOCLAIM min_idle_time must be a nonnegative integer"
                )
        except TypeError:
            pass

        kwargs = {}
        pieces = [name, groupname, consumername, min_idle_time, start_id]

        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be an integer >= 0")
            pieces.extend([b"COUNT", count])
        except TypeError:
            pass
        if justid:
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True

        return self.execute_command("XAUTOCLAIM", *pieces, **kwargs)

    def xclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        message_ids: list[StreamIdT] | tuple[StreamIdT],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
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
        pieces: list[EncodableT] = [name, groupname, consumername, str(min_idle_time)]
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
        self,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT = "$",
        mkstream: bool = False,
    ) -> Awaitable:
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        pieces: list[EncodableT] = ["XGROUP CREATE", name, groupname, id]
        if mkstream:
            pieces.append(b"MKSTREAM")
        return self.execute_command(*pieces)

    def xgroup_delconsumer(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
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

    def xgroup_createconsumer(
        self, name: KeyT, groupname: GroupT, consumername: ConsumerT
    ) -> Awaitable:
        """
        Consumers in a consumer group are auto-created every time a new
        consumer name is mentioned by some command.
        They can be explicitly created by using this command.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to create.
        """
        return self.execute_command(
            "XGROUP CREATECONSUMER", name, groupname, consumername
        )

    def xgroup_setid(
        self,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT,
    ) -> Awaitable:
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

    def xinfo_stream(self, name: KeyT, full: bool = False) -> Awaitable:
        """
        Returns general information about the stream.
        name: name of the stream.
        full: optional boolean, false by default. Return full summary
        """
        pieces = [name]
        options = {}
        if full:
            pieces.append(b"FULL")
            options = {"full": full}
        return self.execute_command("XINFO STREAM", *pieces, **options)

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
        min: StreamIdT,
        max: StreamIdT,
        count: int,
        consumername: ConsumerT | None = None,
        idle: int | None = None,
    ) -> Awaitable:
        """
        Returns information about pending messages, in a range.

        name: name of the stream.
        groupname: name of the consumer group.
        idle: available from  version 6.2. filter entries by their
        idle-time, given in milliseconds (optional).
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).

        """
        if {min, max, count} == {None}:
            if idle is not None or consumername is not None:
                raise DataError(
                    "if XPENDING is provided with idle time"
                    " or consumername, it must be provided"
                    " with min, max and count parameters"
                )
            return self.xpending(name, groupname)

        pieces: list[EncodableT] = [name, groupname]
        if min is None or max is None or count is None:
            raise DataError(
                "XPENDING must be provided with min, max "
                "and count parameters, or none of them."
            )
        # idle
        try:
            if int(idle) < 0:
                raise DataError("XPENDING idle must be a integer >= 0")
            pieces.extend(["IDLE", idle])
        except TypeError:
            pass
        # count
        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([min, max, count])
        except TypeError:
            pass
        # consumername
        if consumername:
            pieces.append(consumername)

        return self.execute_command("XPENDING", *pieces, parse_detail=True)

    def xrange(
        self,
        name: KeyT,
        min: StreamIdT = "-",
        max: StreamIdT = "+",
        count: int | None = None,
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
        pieces: list[EncodableT] = [min, max]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XRANGE", name, *pieces)

    def xread(
        self,
        streams: dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
    ) -> Awaitable:
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        """
        pieces: list[EncodableT] = []
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
        streams: dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
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
        pieces: list[EncodableT] = [b"GROUP", groupname, consumername]
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
        count: int | None = None,
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
        pieces: list[EncodableT] = [max, min]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREVRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XREVRANGE", name, *pieces)

    def xtrim(
        self,
        name: KeyT,
        maxlen: int,
        approximate: bool = True,
        minid: StreamIdT | None = None,
        limit: int | None = None,
    ) -> Awaitable:
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        minid: the minimum id in the stream to query
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve
        """
        pieces: list[EncodableT] = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` may be specified")

        if maxlen is not None:
            pieces.append(b"MAXLEN")
        if minid is not None:
            pieces.append(b"MINID")
        if approximate:
            pieces.append(b"~")
        if maxlen is not None:
            pieces.append(maxlen)
        if minid is not None:
            pieces.append(minid)
        if limit is not None:
            pieces.append(b"LIMIT")
            pieces.append(limit)

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
        gt: bool = None,
        lt: bool = None,
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

        ``LT`` Only update existing elements if the new score is less than
        the current score. This flag doesn't prevent adding new elements.

        ``GT`` Only update existing elements if the new score is greater than
        the current score. This flag doesn't prevent adding new elements.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.

        ``NX``, ``LT``, and ``GT`` are mutually exclusive options.
        See: https://redis.io/commands/ZADD
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
        if nx is True and (gt is not None or lt is not None):
            raise DataError("Only one of 'gt' or 'lt' can be set.")

        pieces: list[EncodableT] = []
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
        if gt:
            pieces.append(b"GT")
        if lt:
            pieces.append(b"LT")
        for pair in mapping.items():
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command("ZADD", name, *pieces, **options)

    def zcard(self, name: KeyT) -> Awaitable:
        """Return the number of elements in the sorted set ``name``"""
        return self.execute_command("ZCARD", name)

    def zcount(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
    ) -> Awaitable:
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.execute_command("ZCOUNT", name, min, max)

    def zdiff(self, keys: KeysT, withscores: bool = False) -> Awaitable:
        """
        Returns the difference between the first and all successive input
        sorted sets provided in ``keys``.
        """
        pieces = [len(keys), *keys]
        if withscores:
            pieces.append("WITHSCORES")
        return self.execute_command("ZDIFF", *pieces)

    def zdiffstore(self, dest: KeyT, keys: KeysT) -> Awaitable:
        """
        Computes the difference between the first and all successive input
        sorted sets provided in ``keys`` and stores the result in ``dest``.
        """
        pieces = [len(keys), *keys]
        return self.execute_command("ZDIFFSTORE", dest, *pieces)

    def zincrby(
        self,
        name: KeyT,
        amount: float,
        value: EncodableT,
    ) -> Awaitable:
        """Increment the score of ``value`` in sorted set ``name`` by ``amount``"""
        return self.execute_command("ZINCRBY", name, amount, value)

    def zinter(
        self,
        keys: KeysT,
        aggregate: str | None = None,
        withscores: bool = False,
    ) -> Awaitable:
        """
        Return the intersect of multiple sorted sets specified by ``keys``.
        With the ``aggregate`` option, it is possible to specify how the
        results of the union are aggregated. This option defaults to SUM,
        where the score of an element is summed across the inputs where it
        exists. When this option is set to either MIN or MAX, the resulting
        set will contain the minimum or maximum score of an element across
        the inputs where it exists.
        """
        return self._zaggregate("ZINTER", None, keys, aggregate, withscores=withscores)

    def zinterstore(
        self,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> Awaitable:
        """
        Intersect multiple sorted sets specified by ``keys`` into a new
        sorted set, ``dest``. Scores in the destination will be aggregated
        based on the ``aggregate``. This option defaults to SUM, where the
        score of an element is summed across the inputs where it exists.
        When this option is set to either MIN or MAX, the resulting set will
        contain the minimum or maximum score of an element across the inputs
        where it exists.
        """
        return self._zaggregate("ZINTERSTORE", dest, keys, aggregate)

    def zlexcount(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
    ) -> Awaitable:
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return self.execute_command("ZLEXCOUNT", name, min, max)

    def zpopmax(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable:
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMAX", name, *args, **options)

    def zpopmin(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable:
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMIN", name, *args, **options)

    def zrandmember(
        self,
        key: KeyT,
        count: int = None,
        withscores: bool = False,
    ) -> Awaitable:
        """
        Return a random element from the sorted set value stored at key.

        ``count`` if the argument is positive, return an array of distinct
        fields. If called with a negative count, the behavior changes and
        the command is allowed to return the same field multiple times.
        In this case, the number of returned fields is the absolute value
        of the specified count.

        ``withscores`` The optional WITHSCORES modifier changes the reply so it
        includes the respective scores of the randomly selected elements from
        the sorted set.
        """
        params = []
        if count is not None:
            params.append(count)
        if withscores:
            params.append("WITHSCORES")

        return self.execute_command("ZRANDMEMBER", key, *params)

    def bzpopmax(self, keys: KeysT, timeout: TimeoutSecT = 0) -> Awaitable:
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BZPOPMAX", *keys)

    def bzpopmin(self, keys: KeysT, timeout: TimeoutSecT = 0) -> Awaitable:
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        klist: list[EncodableT] = list_or_args(keys, None)
        klist.append(timeout)
        return self.execute_command("BZPOPMIN", *klist)

    def _zrange(
        self,
        command,
        dest,
        name,
        start,
        end,
        desc=False,
        byscore=False,
        bylex=False,
        withscores=False,
        score_cast_func=float,
        offset=None,
        num=None,
    ):
        if byscore and bylex:
            raise DataError(
                "``byscore`` and ``bylex`` can not be " "specified together."
            )
        if (offset is not None and num is None) or (num is not None and offset is None):
            raise DataError("``offset`` and ``num`` must both be specified.")
        if bylex and withscores:
            raise DataError(
                "``withscores`` not supported in combination " "with ``bylex``."
            )
        pieces = [command]
        if dest:
            pieces.append(dest)
        pieces.extend([name, start, end])
        if byscore:
            pieces.append("BYSCORE")
        if bylex:
            pieces.append("BYLEX")
        if desc:
            pieces.append("REV")
        if offset is not None and num is not None:
            pieces.extend(["LIMIT", offset, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: int = None,
        num: int = None,
    ) -> Awaitable:
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.
        """
        # Need to support ``desc`` also when using old redis version
        # because it was supported in 3.5.3 (of redis-py)
        if not byscore and not bylex and (offset is None and num is None) and desc:
            return self.zrevrange(name, start, end, withscores, score_cast_func)

        return self._zrange(
            "ZRANGE",
            None,
            name,
            start,
            end,
            desc,
            byscore,
            bylex,
            withscores,
            score_cast_func,
            offset,
            num,
        )

    def zrevrange(self, name, start, end, withscores=False, score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ["ZREVRANGE", name, start, end]
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrangestore(
        self,
        dest: KeyT,
        name: KeyT,
        start: int,
        end: int,
        byscore: bool = False,
        bylex: bool = False,
        desc: bool = False,
        offset: int = None,
        num: int = None,
    ) -> Awaitable:
        """
        Stores in ``dest`` the result of a range of values from sorted set
        ``name`` between ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.
        """
        return self._zrange(
            "ZRANGESTORE",
            dest,
            name,
            start,
            end,
            desc,
            byscore,
            bylex,
            False,
            None,
            offset,
            num,
        )

    def zrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable:
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces: list[EncodableT] = ["ZRANGEBYLEX", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        return self.execute_command(*pieces)

    def zrevrangebylex(
        self,
        name: KeyT,
        max: EncodableT,
        min: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable:
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces: list[EncodableT] = ["ZREVRANGEBYLEX", name, max, min]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        return self.execute_command(*pieces)

    def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
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
        pieces: list[EncodableT] = ["ZRANGEBYSCORE", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(
        self,
        name,
        max,
        min,
        start=None,
        num=None,
        withscores=False,
        score_cast_func=float,
    ):
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
        pieces = ["ZREVRANGEBYSCORE", name, max, min]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        if withscores:
            pieces.append("WITHSCORES")
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

    def zremrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
    ) -> Awaitable:
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
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
    ) -> Awaitable:
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.execute_command("ZREMRANGEBYSCORE", name, min, max)

    def zrevrank(self, name: KeyT, value: EncodableT) -> Awaitable:
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command("ZREVRANK", name, value)

    def zscore(self, name: str, value: EncodableT) -> Awaitable:
        """Return the score of element ``value`` in sorted set ``name``"""
        return self.execute_command("ZSCORE", name, value)

    def zunion(
        self,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
        withscores: bool = False,
    ) -> Awaitable:
        """
        Return the union of multiple sorted sets specified by ``keys``.
        ``keys`` can be provided as dictionary of keys and their weights.
        Scores will be aggregated based on the ``aggregate``, or SUM if
        none is provided.
        """
        return self._zaggregate("ZUNION", None, keys, aggregate, withscores=withscores)

    def zunionstore(
        self,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> Awaitable:
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate("ZUNIONSTORE", dest, keys, aggregate)

    def zmscore(self, key: KeyT, members: list[str]) -> Awaitable:
        """
        Returns the scores associated with the specified members
        in the sorted set stored at key.
        ``members`` should be a list of the member name.
        Return type is a list of score.
        If the member does not exist, a None will be returned
        in corresponding position.
        """
        if not members:
            raise DataError("ZMSCORE members must be a non-empty list")
        pieces = [key] + members
        return self.execute_command("ZMSCORE", *pieces)

    def _zaggregate(
        self,
        command: str,
        dest: KeyT | None,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
        **options,
    ) -> Awaitable:
        pieces: list[EncodableT] = [command]
        if dest is not None:
            pieces.append(dest)
        pieces.append(len(keys))
        if isinstance(keys, dict):
            keys, weights = keys.keys(), keys.values()
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(b"WEIGHTS")
            pieces.extend(weights)
        if aggregate:
            if aggregate.upper() in ["SUM", "MIN", "MAX"]:
                pieces.append(b"AGGREGATE")
                pieces.append(aggregate)
            else:
                raise DataError("aggregate can be sum, min, or max")
        if options.get("withscores", False):
            pieces.append(b"WITHSCORES")
        return self.execute_command(*pieces, **options)

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

    def hincrby(
        self,
        name: KeyT,
        key: FieldT,
        amount: int = 1,
    ) -> Awaitable:
        """Increment the value of ``key`` in hash ``name`` by ``amount``"""
        return self.execute_command("HINCRBY", name, key, amount)

    def hincrbyfloat(
        self,
        name: KeyT,
        key: FieldT,
        amount: float = 1.0,
    ) -> Awaitable:
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
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[AnyFieldT, EncodableT] | None = None,
    ) -> Awaitable:
        """
        Set ``key`` to ``value`` within hash ``name``,
        ``mapping`` accepts a dict of key/value pairs that will be
        added to hash ``name``.
        Returns the number of fields that were added.
        """
        if key is None and not mapping:
            raise DataError("'hset' with no key value pairs")
        items: list[EncodableT] = []
        if key is not None:
            items.extend((key, value))
        if mapping:
            for pair in mapping.items():
                items.extend(pair)

        return self.execute_command("HSET", name, *items)

    def hsetnx(
        self,
        name: KeyT,
        key: FieldT,
        value: EncodableT,
    ) -> Awaitable:
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.execute_command("HSETNX", name, key, value)

    def hmset(
        self,
        name: KeyT,
        mapping: Mapping[AnyFieldT, EncodableT],
    ) -> Awaitable:
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
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("HMSET", name, *items)

    def hmget(
        self,
        name: KeyT,
        keys: Sequence[FieldT],
        *args: FieldT,
    ) -> Awaitable:
        """Returns a list of values ordered identically to ``keys``"""
        args = list_or_args(keys, args)
        return self.execute_command("HMGET", name, *args)

    def hvals(self, name: KeyT) -> Awaitable:
        """Return the list of values within hash ``name``"""
        return self.execute_command("HVALS", name)

    def hstrlen(self, name: KeyT, key: FieldT) -> Awaitable:
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``
        """
        return self.execute_command("HSTRLEN", name, key)

    def publish(
        self,
        channel: ChannelT,
        message: EncodableT,
    ) -> Awaitable:
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

    def replicaof(self, *args):
        """
        Update the replication settings of a redis replica, on the fly.
        Examples of valid arguments include:
            NO ONE (set no replication)
            host port (set to the host and port of a redis server)
        see: https://redis.io/commands/replicaof
        """
        return self.execute_command("REPLICAOF", *args)

    def eval(
        self,
        script: ScriptTextT,
        numkeys: int,
        *keys_and_args: EncodableT,
    ) -> Awaitable:
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command("EVAL", script, numkeys, *keys_and_args)

    def evalsha(
        self,
        sha: str,
        numkeys: int,
        *keys_and_args: str,
    ) -> Awaitable:
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

    def script_debug(self, *args):
        raise NotImplementedError(
            "SCRIPT DEBUG is intentionally not implemented in the client."
        )

    def script_flush(
        self,
        sync_type: Literal["SYNC"] | Literal["ASYNC"] = "SYNC",
    ) -> Awaitable:
        """Flush all scripts from the script cache.
        ``sync_type`` is by default SYNC (synchronous) but it can also be
                      ASYNC.
        See: https://redis.io/commands/script-flush
        """

        # Redis pre 6 had no sync_type.
        if sync_type not in ["SYNC", "ASYNC", None]:
            raise DataError(
                "SCRIPT FLUSH defaults to SYNC in redis > 6.2, or "
                "accepts SYNC/ASYNC. For older versions, "
                "of redis leave as None."
            )
        if sync_type is None:
            pieces = []
        else:
            pieces = [sync_type]
        return self.execute_command("SCRIPT FLUSH", *pieces)

    def script_kill(self) -> Awaitable:
        """Kill the currently executing Lua script"""
        return self.execute_command("SCRIPT KILL")

    def script_load(self, script: ScriptTextT) -> Awaitable:
        """Load a Lua ``script`` into the script cache. Returns the SHA."""
        return self.execute_command("SCRIPT LOAD", script)

    def register_script(self, script: ScriptTextT) -> Script:
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)

    # GEO COMMANDS
    def geoadd(
        self,
        name: KeyT,
        values: Sequence[EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> Awaitable:
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.

        Note: You can use ZREM to remove elements.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.
        """
        if nx and xx:
            raise DataError("GEOADD allows either 'nx' or 'xx', not both")
        if len(values) % 3 != 0:
            raise DataError("GEOADD requires places with lon, lat and name values")
        pieces = [name]
        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")
        if ch:
            pieces.append("CH")
        pieces.extend(values)
        return self.execute_command("GEOADD", *pieces)

    def geodist(
        self,
        name: KeyT,
        place1: FieldT,
        place2: FieldT,
        unit: str | None = None,
    ) -> Awaitable:
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.
        """
        pieces: list[EncodableT] = [name, place1, place2]
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
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
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
            any=any,
        )

    def georadiusbymember(
        self,
        name: KeyT,
        member: FieldT,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
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
            any=any,
        )

    def _georadiusgeneric(
        self,
        command: str,
        *args: EncodableT,
        **kwargs: EncodableT | None,
    ) -> Awaitable:
        pieces: list[EncodableT] = list(args)
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
            if kwargs["any"]:
                pieces.append("ANY")

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

    def geosearch(
        self,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
    ) -> Awaitable:
        """
        Return the members of specified key identified by the
        ``name`` argument, which are within the borders of the
        area specified by a given shape. This command extends the
        GEORADIUS command, so in addition to searching within circular
        areas, it supports searching within rectangular areas.
        This command should be used in place of the deprecated
        GEORADIUS and GEORADIUSBYMEMBER commands.
        ``member`` Use the position of the given existing
         member in the sorted set. Can't be given with ``longitude``
         and ``latitude``.
        ``longitude`` and ``latitude`` Use the position given by
        this coordinates. Can't be given with ``member``
        ``radius`` Similar to GEORADIUS, search inside circular
        area according the given radius. Can't be given with
        ``height`` and ``width``.
        ``height`` and ``width`` Search inside an axis-aligned
        rectangle, determined by the given height and width.
        Can't be given with ``radius``
        ``unit`` must be one of the following : m, km, mi, ft.
        `m` for meters (the default value), `km` for kilometers,
        `mi` for miles and `ft` for feet.
        ``sort`` indicates to return the places in a sorted way,
        ASC for nearest to farest and DESC for farest to nearest.
        ``count`` limit the results to the first count matching items.
        ``any`` is set to True, the command will return as soon as
        enough matches are found. Can't be provided without ``count``
        ``withdist`` indicates to return the distances of each place.
        ``withcoord`` indicates to return the latitude and longitude of
        each place.
        ``withhash`` indicates to return the geohash string of each place.
        """

        return self._geosearchgeneric(
            "GEOSEARCH",
            name,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=unit,
            radius=radius,
            width=width,
            height=height,
            sort=sort,
            count=count,
            any=any,
            withcoord=withcoord,
            withdist=withdist,
            withhash=withhash,
            store=None,
            store_dist=None,
        )

    def geosearchstore(
        self,
        dest: KeyT,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        storedist: bool = False,
    ) -> Awaitable:
        """
        This command is like GEOSEARCH, but stores the result in
        ``dest``. By default, it stores the results in the destination
        sorted set with their geospatial information.
        if ``store_dist`` set to True, the command will stores the
        items in a sorted set populated with their distance from the
        center of the circle or box, as a floating-point number.
        """
        return self._geosearchgeneric(
            "GEOSEARCHSTORE",
            dest,
            name,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=unit,
            radius=radius,
            width=width,
            height=height,
            sort=sort,
            count=count,
            any=any,
            withcoord=None,
            withdist=None,
            withhash=None,
            store=None,
            store_dist=storedist,
        )

    def _geosearchgeneric(  # noqa C901
        self,
        command: str,
        *args: EncodableT,
        **kwargs: EncodableT | None,
    ) -> Awaitable:
        pieces = list(args)

        # FROMMEMBER or FROMLONLAT
        if kwargs["member"] is None:
            if kwargs["longitude"] is None or kwargs["latitude"] is None:
                raise DataError("GEOSEARCH must have member or longitude and latitude")
        if kwargs["member"]:
            if kwargs["longitude"] or kwargs["latitude"]:
                raise DataError(
                    "GEOSEARCH member and longitude or latitude cant be set together"
                )
            pieces.extend([b"FROMMEMBER", kwargs["member"]])
        if kwargs["longitude"] and kwargs["latitude"]:
            pieces.extend([b"FROMLONLAT", kwargs["longitude"], kwargs["latitude"]])

        # BYRADIUS or BYBOX
        if kwargs["radius"] is None:
            if kwargs["width"] is None or kwargs["height"] is None:
                raise DataError("GEOSEARCH must have radius or width and height")
        if kwargs["unit"] is None:
            raise DataError("GEOSEARCH must have unit")
        if kwargs["unit"].lower() not in ("m", "km", "mi", "ft"):
            raise DataError("GEOSEARCH invalid unit")
        if kwargs["radius"]:
            if kwargs["width"] or kwargs["height"]:
                raise DataError(
                    "GEOSEARCH radius and width or height" " cant be set together"
                )
            pieces.extend([b"BYRADIUS", kwargs["radius"], kwargs["unit"]])
        if kwargs["width"] and kwargs["height"]:
            pieces.extend([b"BYBOX", kwargs["width"], kwargs["height"], kwargs["unit"]])

        # sort
        if kwargs["sort"]:
            if kwargs["sort"].upper() == "ASC":
                pieces.append(b"ASC")
            elif kwargs["sort"].upper() == "DESC":
                pieces.append(b"DESC")
            else:
                raise DataError("GEOSEARCH invalid sort")

        # count any
        if kwargs["count"]:
            pieces.extend([b"COUNT", kwargs["count"]])
            if kwargs["any"]:
                pieces.append(b"ANY")
        elif kwargs["any"]:
            raise DataError("GEOSEARCH any can't be provided without count")

        # other properties
        for arg_name, byte_repr in (
            ("withdist", b"WITHDIST"),
            ("withcoord", b"WITHCOORD"),
            ("withhash", b"WITHHASH"),
            ("store_dist", b"STOREDIST"),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        return self.execute_command(command, *pieces, **kwargs)

    # MODULE COMMANDS
    def module_load(self, path: str, *args: EncodableT) -> Awaitable:
        """
        Loads the module from ``path``.
        Passes all ``*args`` to the module, during loading.
        Raises ``ModuleError`` if a module is not found at ``path``.
        """
        return self.execute_command("MODULE LOAD", path, *args)

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

    def command_info(self):
        raise NotImplementedError(
            "COMMAND INFO is intentionally not implemented in the client."
        )

    def command_count(self):
        return self.execute_command("COMMAND COUNT")

    def command(self):
        return self.execute_command("COMMAND")


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
            script = encoder.encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    async def __call__(
        self,
        keys: Sequence[KeyT] | None = None,
        args: Iterable[EncodableT] | None = None,
        client: "Redis" | None = None,
    ):
        """Execute the script, passing any required ``args``"""
        from aioredis.client import Pipeline

        keys = keys or []
        args = args or []
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
        try:
            return await client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a different server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = await client.script_load(self.script)
            return await client.evalsha(self.sha, len(keys), *args)


class BitFieldOperation:
    """
    Command builder for BITFIELD commands.
    """

    def __init__(self, client: Redis, key: str, default_overflow: str | None = None):
        self.client = client
        self.key = key
        self._default_overflow = default_overflow
        self.operations: list[tuple[EncodableT, ...]] = []
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
        overflow: str | None = None,
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
        cmd = ["BITFIELD", self.key]
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
