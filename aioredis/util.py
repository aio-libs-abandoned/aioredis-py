
# NOTE: never put here anything else;
#       just this basic types
_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
    }


def _bytes_len(sized):
    return str(len(sized)).encode('utf-8')


def encode_command(*args):
    """Encodes arguments into redis bulk-strings array.

    Raises TypeError if any of args not of bytes, str, int or float type.
    """
    buf = bytearray()
    add = lambda data: buf.extend(data + b'\r\n')

    add(b'*' + _bytes_len(args))
    for arg in args:
        if arg is None:
            add(b'$-1')
        elif type(arg) in _converters:
            barg = _converters[type(arg)](arg)
            add(b'$' + _bytes_len(barg))
            add(barg)
        else:
            raise TypeError("Argument {!r} expected to be of bytes,"
                            " str, int or float type".format(arg))
    return buf


def convert_to_int_or_float(raw_value):
    """Convert redis response ``bytes`` to ``int`` or ``float``"""
    assert isinstance(raw_value, bytes), 'raw_value must be bytes'
    value_str = raw_value.decode('utf-8')
    try:
        value = int(value_str)
    except ValueError:
        value = float(value_str)
    return value
