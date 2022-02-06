from __future__ import annotations

from typing import TYPE_CHECKING, AnyStr, Callable, Generator, Iterator, Optional

from aioredis.exceptions import InvalidResponse, ReplyError

if TYPE_CHECKING:
    from aioredis.connection import EncodableT


class PythonReader:
    """Pure-Python RESP2/3 parser following :py:class:`hiredis.Reader` interface."""

    __slots__ = ("_parser",)

    def __init__(
        self,
        protocolError: Callable = InvalidResponse,
        replyError: Callable = ReplyError,
        *,
        encoding: str | None = None,
        errors: str | None = None,
    ):
        self._parser = PythonParser(protocolError, replyError, encoding, errors)

    def feed(self, data, o: int = 0, l: int = -1):
        """Feed data to parser."""
        if l == -1:
            l = len(data) - o
        if o < 0 or l < 0:
            raise ValueError("negative input")
        if o + l > len(data):
            raise ValueError("input is larger than buffer size")
        self._parser.buf.extend(data[o : o + l])

    def gets(self) -> EncodableT | bool | BaseException:
        """Get parsed value or False otherwise.
        Error replies are return as replyError exceptions (not raised).
        Protocol errors are raised.
        """
        return self._parser.parse_one()

    def has_data(self) -> bool:
        """Whether the"""
        return len(self._parser.buf) > self._parser.pos

    def setmaxbuf(self, size: int | None) -> None:
        """No-op."""
        pass

    def getmaxbuf(self) -> int:
        """No-op."""
        return 0

    def set_encoding(
        self, encoding: str | None | ... = ..., errors: str | None | ... = ...
    ):
        if encoding is not ...:
            self._parser.encoding = encoding
        if errors is not ...:
            self._parser.encoding_errors = errors or "strict"


class PythonParser:
    """A pure-Python parser for the RESP2/3.

    Note:
        This class does NOT implement protocols:

            - Push
            - Streamed (strings and aggregates)
            - Attributes

        This implementation was based upon the Hiredis parser and its Python bindings.
        Since the Python bindings do not implement these protocols, neither do we.
        As these features are added and tests are added to the hiredis suite, we will
        implement them here.

    See Also:
        https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md


    """

    __slots__ = (
        "buf",
        "pos",
        "protocolError",
        "replyError",
        "_encoding",
        "encoding_errors",
        "_err",
        "_gen",
        "_protocols",
    )

    def __init__(
        self,
        protocolError: Callable,
        replyError: Callable,
        encoding: str | None = None,
        errors: str | None = None,
    ):

        self.buf: bytearray = bytearray()
        self.pos: int = 0
        self.protocolError: Callable = protocolError
        self.replyError: Callable = replyError
        self.encoding_errors: str = errors or "strict"
        self._encoding: str | None = encoding
        self._err = None
        self._gen: Generator | None = None
        self._protocols: dict[bytes, Callable[[PythonParser], EncodableT]] = (
            self._PROTOCOLS_DECODE if self.encoding else self._PROTOCOLS
        )

    @property
    def encoding(self) -> str:
        return self._encoding

    @encoding.setter
    def encoding(self, val: str):
        self._encoding = val
        self._protocols = self._PROTOCOLS_DECODE

    def waitsome(self, size: int) -> Iterator[bool]:
        # keep yielding false until at least `size` bytes added to buf.
        while len(self.buf) < self.pos + size:
            yield False

    def waitany(self) -> Iterator[bool]:
        yield from self.waitsome(len(self.buf) + 1)

    def readone(self) -> bytes:
        if not self.buf[self.pos : self.pos + 1]:
            yield from self.waitany()
        val = self.buf[self.pos : self.pos + 1]
        self.pos += 1
        return bytes(val)

    def readline(self, size: int | None = None) -> bytes:
        if size is not None:
            if len(self.buf) < size + 2 + self.pos:
                yield from self.waitsome(size + 2)
            offset = self.pos + size
            if self.buf[offset : offset + 2] != b"\r\n":
                raise self.error("Expected b'\r\n'")
        else:
            offset = self.buf.find(b"\r\n", self.pos)
            while offset < 0:
                yield from self.waitany()
                offset = self.buf.find(b"\r\n", self.pos)
        val = self.buf[self.pos : offset]
        self.pos = 0
        del self.buf[: offset + 2]
        return bytes(val)

    def readint(self):
        try:
            return int((yield from self.readline()))
        except ValueError as exc:
            raise self.error(exc)

    def readfloat(self):
        try:
            return float((yield from self.readline()))
        except ValueError as exc:
            raise self.error(exc)

    def readbool(self):
        try:
            val = yield from self.readline()
            return True if val == b"t" else False
        except ValueError as exc:
            raise self.error(exc)

    def error(self, msg):
        self._err = self.protocolError(msg)
        return self._err

    def parse(self) -> EncodableT:
        if self._err is not None:
            raise self._err
        ctl = yield from self.readone()
        if ctl not in self._protocols:
            msg = ctl if ctl is None else ctl.decode(encoding="utf8", errors="replace")
            raise self.error(f"Protocol Error: {msg!r}")
        return (yield from self._protocols[ctl](self))

    def parse_one(self):
        if self._gen is None:
            self._gen = self.parse()
        try:
            self._gen.send(None)
        except StopIteration as exc:
            self._gen = None
            return exc.value
        except Exception:
            self._gen = None
            raise
        else:
            return False

    def _parse_error(self) -> Exception:
        val = yield from self.readline()
        return self.replyError(val.decode(self.encoding or "utf8", errors="replace"))

    def _parse_single(self) -> bytes:
        val = yield from self.readline()
        return val

    def _parse_single_decode(self) -> str:
        val = yield from self._parse_single()
        return self._maybe_decode(val)

    def _parse_verbatim(self) -> bytes:
        length = yield from self.readint()
        if length == -1:
            return None
        vbt = yield from self.readone()
        typ, val = vbt.rsplit(b":", maxsplit=1)
        return val

    def _parse_verbatim_decode(self) -> str:
        val = self._parse_verbatim()
        return self._maybe_decode(val=val)

    def _parse_int(self) -> int:
        return (yield from self.readint())

    def _parse_float(self) -> float:
        return (yield from self.readfloat())

    def _parse_bulk(self) -> bytes | None:
        length = yield from self.readint()
        if length == -1:
            return None
        val = yield from self.readline(length)
        return val

    def _parse_bulk_decode(self) -> bytes | None:
        val = yield from self._parse_bulk()
        return self._maybe_decode(val=val)

    def _parse_mutibulk(self) -> list[AnyStr] | None:
        length = yield from self.readint()
        if length == -1:
            return None
        val = []
        append = val.append
        parse = self.parse
        for _ in range(length):
            append((yield from parse()))
        return val

    def _parse_null(self) -> None:
        yield from self.readline()
        return None

    def _parse_bool(self) -> bool:
        return (yield from self.readbool())

    def _parse_dict(self) -> dict[AnyStr, EncodableT] | None:
        keynum = yield from self.readint()
        if keynum == -1:
            return None
        val = {}
        parse = self.parse
        for _ in range(keynum):
            val[(yield from parse())] = yield from parse()
        return val

    def _parse_set(self) -> set[EncodableT] | None:
        length = yield from self.readint()
        if length == -1:
            return None
        val = set()
        add = val.add
        parse = self.parse
        for _ in range(length):
            add((yield from parse))
        return val

    def _parse_vector(self) -> list[AnyStr] | None:
        return self._parse_mutibulk()

    def _maybe_decode(self, val: bytes | None) -> str:
        return (
            val
            if val is None
            else val.decode(self.encoding, errors=self.encoding_errors)
        )

    _PROTOCOLS: dict[bytes, Callable[[PythonParser], EncodableT]] = {
        b"-": _parse_error,
        b"+": _parse_single,
        b":": _parse_int,
        b"(": _parse_int,
        b"#": _parse_bool,
        b"_": _parse_null,
        b"$": _parse_bulk,
        b"=": _parse_verbatim,
        b"*": _parse_mutibulk,
        b"~": _parse_set,
        b"%": _parse_dict,
        b">": _parse_vector,
    }

    _PROTOCOLS_DECODE: dict[bytes, Callable[[PythonParser], EncodableT]] = {
        b"-": _parse_error,
        b"+": _parse_single_decode,
        b":": _parse_int,
        b"(": _parse_int,
        b"#": _parse_bool,
        b"_": _parse_null,
        b"$": _parse_bulk_decode,
        b"=": _parse_verbatim,
        b"*": _parse_mutibulk,
        b"~": _parse_set,
        b"%": _parse_dict,
        b">": _parse_vector,
    }
