import pytest

from aioredis.util import (Channel, extract_names)


def test_extract_names():
    ch = Channel('ch:1', is_pattern=False)
    res = list(extract_names([ch]))
    assert res == ['ch:1']

    res = list(extract_names(['ch:1']))
    assert res == ['ch:1']


def test_extract_names__error():
    ch = Channel('ch:1', is_pattern=False)
    pat = Channel('ch:*', is_pattern=True)

    with pytest.raises_regex(
            ValueError, "p\(un\)subscribe command expects pattern channel"):
        list(extract_names([ch], is_pattern_command=True))

    with pytest.raises_regex(
            ValueError, "\(un\)subscribe command expects exact channel"):
        list(extract_names([pat], is_pattern_command=False))
