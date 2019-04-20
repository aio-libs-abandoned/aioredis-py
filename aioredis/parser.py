from .errors import ProtocolError, ReplyError
from typing import Optional, Generator, Callable, Iterator  # noqa

__all__ = [
    'Reader', 'PyReader',
]


class PyReader:
    """Pure-Python Redis protocol parser that follows hiredis.Reader
    interface (except setmaxbuf/getmaxbuf).
    """
    def __init__(self, protocolError: Callable = ProtocolError,
                 replyError: Callable = ReplyError,
                 encoding: Optional[str] = None):
        if not callable(protocolError):
            raise TypeError("Expected a callable")
        if not callable(replyError):
            raise TypeError("Expected a callable")
        self._parser = Parser(protocolError, replyError, encoding)

    def feed(self, data, o: int = 0, l: int = -1):
        """Feed data to parser."""
        if l == -1:
            l = len(data) - o
        if o < 0 or l < 0:
            raise ValueError("negative input")
        if o + l > len(data):
            raise ValueError("input is larger than buffer size")
        self._parser.buf.extend(data[o:o+l])

    def gets(self):
        """Get parsed value or False otherwise.

        Error replies are return as replyError exceptions (not raised).
        Protocol errors are raised.
        """
        return self._parser.parse_one()

    def setmaxbuf(self, size: Optional[int]) -> None:
        """No-op."""
        pass

    def getmaxbuf(self) -> int:
        """No-op."""
        return 0


class Parser:
    def __init__(self, protocolError: Callable,
                 replyError: Callable, encoding: Optional[str]):

        self.buf = bytearray()  # type: bytearray
        self.pos = 0  # type: int
        self.protocolError = protocolError  # type: Callable
        self.replyError = replyError  # type: Callable
        self.encoding = encoding  # type: Optional[str]
        self._err = None
        self._gen = None  # type: Optional[Generator]

    def waitsome(self, size: int) -> Iterator[bool]:
        # keep yielding false until at least `size` bytes added to buf.
        while len(self.buf) < self.pos+size:
            yield False

    def waitany(self) -> Iterator[bool]:
        yield from self.waitsome(len(self.buf) + 1)

    def readone(self):
        if not self.buf[self.pos:self.pos + 1]:
            yield from self.waitany()
        val = self.buf[self.pos:self.pos + 1]
        self.pos += 1
        return val

    def readline(self, size: Optional[int] = None):
        if size is not None:
            if len(self.buf) < size + 2 + self.pos:
                yield from self.waitsome(size + 2)
            offset = self.pos + size
            if self.buf[offset:offset+2] != b'\r\n':
                raise self.error("Expected b'\r\n'")
        else:
            offset = self.buf.find(b'\r\n', self.pos)
            while offset < 0:
                yield from self.waitany()
                offset = self.buf.find(b'\r\n', self.pos)
        val = self.buf[self.pos:offset]
        self.pos = 0
        del self.buf[:offset + 2]
        return val

    def readint(self):
        try:
            return int((yield from self.readline()))
        except ValueError as exc:
            raise self.error(exc)

    def error(self, msg):
        self._err = self.protocolError(msg)
        return self._err

    def parse(self, is_bulk: bool = False):
        if self._err is not None:
            raise self._err
        ctl = yield from self.readone()
        if ctl == b'+':
            val = yield from self.readline()
            if self.encoding is not None:
                try:
                    return val.decode(self.encoding)
                except UnicodeDecodeError:
                    pass
            return bytes(val)
        elif ctl == b'-':
            val = yield from self.readline()
            return self.replyError(val.decode('utf-8'))
        elif ctl == b':':
            return (yield from self.readint())
        elif ctl == b'$':
            val = yield from self.readint()
            if val == -1:
                return None
            val = yield from self.readline(val)
            if self.encoding:
                try:
                    return val.decode(self.encoding)
                except UnicodeDecodeError:
                    pass
            return bytes(val)
        elif ctl == b'*':
            val = yield from self.readint()
            if val == -1:
                return None
            bulk_array = []
            error = None
            for _ in range(val):
                try:
                    bulk_array.append((yield from self.parse(is_bulk=True)))
                except LookupError as err:
                    if error is None:
                        error = err
            if error is not None:
                raise error
            return bulk_array
        else:
            raise self.error("Invalid first byte: {!r}".format(ctl))

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


try:
    import hiredis
    Reader = hiredis.Reader
except ImportError:
    Reader = PyReader
