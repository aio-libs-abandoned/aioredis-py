import pytest

import aioredis
from aioredis import exceptions
from aioredis.commands.json.decoders import decode_list, unstring
from aioredis.commands.json.path import Path

from .conftest import skip_ifmodversion_lt

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def client(modclient: aioredis.Redis):
    await modclient.flushdb()
    return modclient


@pytest.mark.redismod
async def test_json_setbinarykey(client: aioredis.Redis):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        await client.json().set("somekey", Path.rootPath(), d)
    assert await client.json().set("somekey", Path.rootPath(), d, decode_keys=True)


@pytest.mark.redismod
async def test_json_setgetdeleteforget(client: aioredis.Redis):
    assert await client.json().set("foo", Path.rootPath(), "bar")
    assert await client.json().get("foo") == "bar"
    assert await client.json().get("baz") is None
    assert await client.json().delete("foo") == 1
    assert await client.json().forget("foo") == 0  # second delete
    assert await client.exists("foo") == 0


@pytest.mark.redismod
async def test_jsonget(client: aioredis.Redis):
    await client.json().set("foo", Path.rootPath(), "bar")
    assert await client.json().get("foo") == "bar"


@pytest.mark.redismod
async def test_json_get_jset(client: aioredis.Redis):
    assert await client.json().set("foo", Path.rootPath(), "bar")
    assert "bar" == await client.json().get("foo")
    assert await client.json().get("baz") is None
    assert 1 == await client.json().delete("foo")
    assert await client.exists("foo") == 0


@pytest.mark.redismod
async def test_nonascii_setgetdelete(client: aioredis.Redis):
    assert await client.json().set("notascii", Path.rootPath(), "hyvää-élève")
    assert "hyvää-élève" == await client.json().get("notascii", no_escape=True)
    assert 1 == await client.json().delete("notascii")
    assert await client.exists("notascii") == 0


@pytest.mark.redismod
async def test_jsonsetexistentialmodifiersshouldsucceed(client: aioredis.Redis):
    obj = {"foo": "bar"}
    assert await client.json().set("obj", Path.rootPath(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert await client.json().set("obj", Path("foo"), "baz", nx=True) is None
    assert await client.json().set("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert await client.json().set("obj", Path("foo"), "baz", xx=True)
    assert await client.json().set("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        await client.json().set("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.redismod
async def test_mgetshouldsucceed(client: aioredis.Redis):
    await client.json().set("1", Path.rootPath(), 1)
    await client.json().set("2", Path.rootPath(), 2)
    assert await client.json().mget(["1"], Path.rootPath()) == [1]

    assert await client.json().mget([1, 2], Path.rootPath()) == [1, 2]


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_clear(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == await client.json().clear("arr", Path.rootPath())
    assert [] == await client.json().get("arr")


@pytest.mark.redismod
async def test_type(client: aioredis.Redis):
    await client.json().set("1", Path.rootPath(), 1)
    assert "integer" == await client.json().type("1", Path.rootPath())
    assert "integer" == await client.json().type("1")


@pytest.mark.redismod
async def test_numincrby(client: aioredis.Redis):
    await client.json().set("num", Path.rootPath(), 1)
    assert 2 == await client.json().numincrby("num", Path.rootPath(), 1)
    assert 2.5 == await client.json().numincrby("num", Path.rootPath(), 0.5)
    assert 1.25 == await client.json().numincrby("num", Path.rootPath(), -1.25)


@pytest.mark.redismod
async def test_nummultby(client: aioredis.Redis):
    await client.json().set("num", Path.rootPath(), 1)

    with pytest.deprecated_call():
        assert 2 == await client.json().nummultby("num", Path.rootPath(), 2)
        assert 5 == await client.json().nummultby("num", Path.rootPath(), 2.5)
        assert 2.5 == await client.json().nummultby("num", Path.rootPath(), 0.5)


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_toggle(client: aioredis.Redis):
    await client.json().set("bool", Path.rootPath(), False)
    assert await client.json().toggle("bool", Path.rootPath())
    assert await client.json().toggle("bool", Path.rootPath()) is False
    # check non-boolean value
    await client.json().set("num", Path.rootPath(), 1)
    with pytest.raises(aioredis.exceptions.ResponseError):
        await client.json().toggle("num", Path.rootPath())


@pytest.mark.redismod
async def test_strappend(client: aioredis.Redis):
    await client.json().set("jsonkey", Path.rootPath(), "foo")
    assert 6 == await client.json().strappend("jsonkey", "bar")
    assert "foobar" == await client.json().get("jsonkey", Path.rootPath())


@pytest.mark.redismod
async def test_debug(client: aioredis.Redis):
    await client.json().set("str", Path.rootPath(), "foo")
    assert 24 == await client.json().debug("MEMORY", "str", Path.rootPath())
    assert 24 == await client.json().debug("MEMORY", "str")

    # technically help is valid
    assert isinstance(await client.json().debug("HELP"), list)


@pytest.mark.redismod
async def test_strlen(client: aioredis.Redis):
    await client.json().set("str", Path.rootPath(), "foo")
    assert 3 == await client.json().strlen("str", Path.rootPath())
    await client.json().strappend("str", "bar", Path.rootPath())
    assert 6 == await client.json().strlen("str", Path.rootPath())
    assert 6 == await client.json().strlen("str")


@pytest.mark.redismod
async def test_arrappend(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [1])
    assert 2 == await client.json().arrappend("arr", Path.rootPath(), 2)
    assert 4 == await client.json().arrappend("arr", Path.rootPath(), 3, 4)
    assert 7 == await client.json().arrappend("arr", Path.rootPath(), *[5, 6, 7])


@pytest.mark.redismod
async def test_arrindex(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == await client.json().arrindex("arr", Path.rootPath(), 1)
    assert -1 == await client.json().arrindex("arr", Path.rootPath(), 1, 2)


@pytest.mark.redismod
async def test_arrinsert(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [0, 4])
    assert 5 - -await client.json().arrinsert(
        "arr",
        Path.rootPath(),
        1,
        *[
            1,
            2,
            3,
        ]
    )
    assert [0, 1, 2, 3, 4] == await client.json().get("arr")

    # test prepends
    await client.json().set("val2", Path.rootPath(), [5, 6, 7, 8, 9])
    await client.json().arrinsert("val2", Path.rootPath(), 0, ["some", "thing"])
    assert await client.json().get("val2") == [["some", "thing"], 5, 6, 7, 8, 9]


@pytest.mark.redismod
async def test_arrlen(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 5 == await client.json().arrlen("arr", Path.rootPath())
    assert 5 == await client.json().arrlen("arr")
    assert await client.json().arrlen("fakekey") is None


@pytest.mark.redismod
async def test_arrpop(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 4 == await client.json().arrpop("arr", Path.rootPath(), 4)
    assert 3 == await client.json().arrpop("arr", Path.rootPath(), -1)
    assert 2 == await client.json().arrpop("arr", Path.rootPath())
    assert 0 == await client.json().arrpop("arr", Path.rootPath(), 0)
    assert [1] == await client.json().get("arr")

    # test out of bounds
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 4 == await client.json().arrpop("arr", Path.rootPath(), 99)

    # none test
    await client.json().set("arr", Path.rootPath(), [])
    assert await client.json().arrpop("arr") is None


@pytest.mark.redismod
async def test_arrtrim(client: aioredis.Redis):
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 3 == await client.json().arrtrim("arr", Path.rootPath(), 1, 3)
    assert [1, 2, 3] == await client.json().get("arr")

    # <0 test, should be 0 equivalent
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 0 == await client.json().arrtrim("arr", Path.rootPath(), -1, 3)

    # testing stop > end
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 2 == await client.json().arrtrim("arr", Path.rootPath(), 3, 99)

    # start > array size and stop
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 0 == await client.json().arrtrim("arr", Path.rootPath(), 9, 1)

    # all larger
    await client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 0 == await client.json().arrtrim("arr", Path.rootPath(), 9, 11)


@pytest.mark.redismod
async def test_resp(client: aioredis.Redis):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    await client.json().set("obj", Path.rootPath(), obj)
    assert "bar" == await client.json().resp("obj", Path("foo"))
    assert 1 == await client.json().resp("obj", Path("baz"))
    assert await client.json().resp("obj", Path("qaz"))
    assert isinstance(await client.json().resp("obj"), list)


@pytest.mark.redismod
async def test_objkeys(client: aioredis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await client.json().set("obj", Path.rootPath(), obj)
    keys = await client.json().objkeys("obj", Path.rootPath())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys

    await client.json().set("obj", Path.rootPath(), obj)
    keys = await client.json().objkeys("obj")
    assert keys == list(obj.keys())

    assert await client.json().objkeys("fakekey") is None


@pytest.mark.redismod
async def test_objlen(client: aioredis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await client.json().set("obj", Path.rootPath(), obj)
    assert len(obj) == await client.json().objlen("obj", Path.rootPath())

    await client.json().set("obj", Path.rootPath(), obj)
    assert len(obj) == await client.json().objlen("obj")


@pytest.mark.pipeline
@pytest.mark.redismod
async def test_json_commands_in_pipeline(client: aioredis.Redis):
    p = await client.json().pipeline()
    p.set("foo", Path.rootPath(), "bar")
    p.get("foo")
    p.delete("foo")
    assert [True, "bar", 1] == await p.execute()
    assert await client.keys() == []
    assert await client.get("foo") is None

    # now with a true, json object
    await client.flushdb()
    p = await client.json().pipeline()
    d = {"hello": "world", "oh": "snap"}
    p.jsonset("foo", Path.rootPath(), d)
    p.jsonget("foo")
    p.exists("notarealkey")
    p.delete("foo")
    assert [True, d, 0, 1] == await p.execute()
    assert await client.keys() == []
    assert await client.get("foo") is None


@pytest.mark.redismod
async def test_json_delete_with_dollar(client: aioredis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await client.json().set("doc1", "$", doc1)
    assert await client.json().delete("doc1", "$..a") == 2
    r = await client.json().get("doc1", "$")
    assert r == [{"nested": {"b": 3}}]

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await client.json().set("doc2", "$", doc2)
    assert await client.json().delete("doc2", "$..a") == 1
    res = await client.json().get("doc2", "$")
    assert res == [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]

    doc3 = [
        {
            "ciao": ["non ancora"],
            "nested": [
                {"ciao": [1, "a"]},
                {"ciao": [2, "a"]},
                {"ciaoc": [3, "non", "ciao"]},
                {"ciao": [4, "a"]},
                {"e": [5, "non", "ciao"]},
            ],
        }
    ]
    assert await client.json().set("doc3", "$", doc3)
    assert await client.json().delete("doc3", '$.[0]["nested"]..ciao') == 3

    doc3val = [
        [
            {
                "ciao": ["non ancora"],
                "nested": [
                    {},
                    {},
                    {"ciaoc": [3, "non", "ciao"]},
                    {},
                    {"e": [5, "non", "ciao"]},
                ],
            }
        ]
    ]
    res = await client.json().get("doc3", "$")
    assert res == doc3val

    # Test default path
    assert await client.json().delete("doc3") == 1
    assert await client.json().get("doc3", "$") is None

    await client.json().delete("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_forget_with_dollar(client: aioredis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await client.json().set("doc1", "$", doc1)
    assert await client.json().forget("doc1", "$..a") == 2
    r = await client.json().get("doc1", "$")
    assert r == [{"nested": {"b": 3}}]

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await client.json().set("doc2", "$", doc2)
    assert await client.json().forget("doc2", "$..a") == 1
    res = await client.json().get("doc2", "$")
    assert res == [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]

    doc3 = [
        {
            "ciao": ["non ancora"],
            "nested": [
                {"ciao": [1, "a"]},
                {"ciao": [2, "a"]},
                {"ciaoc": [3, "non", "ciao"]},
                {"ciao": [4, "a"]},
                {"e": [5, "non", "ciao"]},
            ],
        }
    ]
    assert await client.json().set("doc3", "$", doc3)
    assert await client.json().forget("doc3", '$.[0]["nested"]..ciao') == 3

    doc3val = [
        [
            {
                "ciao": ["non ancora"],
                "nested": [
                    {},
                    {},
                    {"ciaoc": [3, "non", "ciao"]},
                    {},
                    {"e": [5, "non", "ciao"]},
                ],
            }
        ]
    ]
    res = await client.json().get("doc3", "$")
    assert res == doc3val

    # Test default path
    assert await client.json().forget("doc3") == 1
    assert await client.json().get("doc3", "$") is None

    await client.json().forget("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_mget_dollar(client: aioredis.Redis):
    # Test mget with multi paths
    await client.json().set(
        "doc1",
        "$",
        {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
    )
    await client.json().set(
        "doc2",
        "$",
        {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
    )
    # Compare also to single JSON.GET
    assert await client.json().get("doc1", "$..a") == [1, 3, None]
    assert await client.json().get("doc2", "$..a") == [4, 6, [None]]

    # Test mget with single path
    assert await client.json().mget("doc1", "$..a") == [1, 3, None]
    # Test mget with multi path
    assert await client.json().mget(["doc1", "doc2"], "$..a") == [
        [1, 3, None],
        [4, 6, [None]],
    ]

    # Test missing key
    assert await client.json().mget(["doc1", "missing_doc"], "$..a") == [
        [1, 3, None],
        None,
    ]
    res = await client.json().mget(["missing_doc1", "missing_doc2"], "$..a")
    assert res == [None, None]


@pytest.mark.redismod
async def test_numby_commands_dollar(client: aioredis.Redis):

    # Test NUMINCRBY
    await client.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    # Test multi
    assert await client.json().numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

    assert await client.json().numincrby("doc1", "$..a", 2.5) == [None, 6.5, 9.5, None]
    # Test single
    assert await client.json().numincrby("doc1", "$.b[1].a", 2) == [11.5]

    assert await client.json().numincrby("doc1", "$.b[2].a", 2) == [None]
    assert await client.json().numincrby("doc1", "$.b[1].a", 3.5) == [15.0]

    # Test NUMMULTBY
    await client.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )

    assert await client.json().nummultby("doc1", "$..a", 2) == [None, 4, 10, None]
    assert await client.json().nummultby("doc1", "$..a", 2.5) == [
        None,
        10.0,
        25.0,
        None,
    ]
    # Test single
    assert await client.json().nummultby("doc1", "$.b[1].a", 2) == [50.0]
    assert await client.json().nummultby("doc1", "$.b[2].a", 2) == [None]
    assert await client.json().nummultby("doc1", "$.b[1].a", 3) == [150.0]

    # test missing keys
    with pytest.raises(exceptions.ResponseError):
        await client.json().numincrby("non_existing_doc", "$..a", 2)
        await client.json().nummultby("non_existing_doc", "$..a", 2)

    # Test legacy NUMINCRBY
    await client.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    assert await client.json().numincrby("doc1", ".b[0].a", 3) == 5

    # Test legacy NUMMULTBY
    await client.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    assert await client.json().nummultby("doc1", ".b[0].a", 3) == 6


@pytest.mark.redismod
async def test_strappend_dollar(client: aioredis.Redis):

    await client.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    # Test multi
    assert await client.json().strappend("doc1", "bar", "$..a") == [6, 8, None]

    assert await client.json().get("doc1", "$") == [
        {"a": "foobar", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
    ]
    # Test single
    assert await client.json().strappend("doc1", "baz", "$.nested1.a") == [11]

    assert await client.json().get("doc1", "$") == [
        {"a": "foobar", "nested1": {"a": "hellobarbaz"}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().strappend("non_existing_doc", "$..a", "err")

    # Test multi
    assert await client.json().strappend("doc1", "bar", ".*.a") == 8
    assert await client.json().get("doc1", "$") == [
        {"a": "foo", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
    ]

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await client.json().strappend("doc1", "piu")


@pytest.mark.redismod
async def test_strlen_dollar(client: aioredis.Redis):

    # Test multi
    await client.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    assert await client.json().strlen("doc1", "$..a") == [3, 5, None]

    res2 = await client.json().strappend("doc1", "bar", "$..a")
    res1 = await client.json().strlen("doc1", "$..a")
    assert res1 == res2

    # Test single
    assert await client.json().strlen("doc1", "$.nested1.a") == [8]
    assert await client.json().strlen("doc1", "$.nested2.a") == [None]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().strlen("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrappend_dollar(client: aioredis.Redis):
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert await client.json().arrappend("doc1", "$..a", "bar", "racuda") == [
        3,
        5,
        None,
    ]
    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]

    # Test single
    assert await client.json().arrappend("doc1", "$.nested1.a", "baz") == [6]
    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrappend("non_existing_doc", "$..a")

    # Test legacy
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    assert await client.json().arrappend("doc1", "..a", "bar", "racuda") == 5

    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    # Test single
    assert await client.json().arrappend("doc1", ".nested1.a", "baz") == 6
    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrinsert_dollar(client: aioredis.Redis):
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert await client.json().arrinsert("doc1", "$..a", "1", "bar", "racuda") == [
        3,
        5,
        None,
    ]

    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    # Test single
    assert await client.json().arrinsert("doc1", "$.nested1.a", -2, "baz") == [6]
    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrlen_dollar(client: aioredis.Redis):

    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi
    assert await client.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert await client.json().arrappend("doc1", "$..a", "non", "abba", "stanza") == [
        4,
        6,
        None,
    ]

    await client.json().clear("doc1", "$.a")
    assert await client.json().arrlen("doc1", "$..a") == [0, 6, None]
    # Test single
    assert await client.json().arrlen("doc1", "$.nested1.a") == [6]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrappend("non_existing_doc", "$..a")

    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (return result of last path)
    assert await client.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert await client.json().arrappend("doc1", "..a", "non", "abba", "stanza") == 6

    # Test single
    assert await client.json().arrlen("doc1", ".nested1.a") == 6

    # Test missing key
    assert await client.json().arrlen("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_arrpop_dollar(client: aioredis.Redis):
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # # # Test multi
    assert await client.json().arrpop("doc1", "$..a", 1) == ['"foo"', None, None]

    assert await client.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrpop("non_existing_doc", "..a")

    # # Test legacy
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    assert await client.json().arrpop("doc1", "..a", "1") is None
    assert await client.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
    ]

    # # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrpop("non_existing_doc", "..a")


@pytest.mark.redismod
async def test_arrtrim_dollar(client: aioredis.Redis):

    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert await client.json().arrtrim("doc1", "$..a", "1", -1) == [0, 2, None]
    assert await client.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}
    ]

    assert await client.json().arrtrim("doc1", "$..a", "1", "1") == [0, 1, None]
    assert await client.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
    ]
    # Test single
    assert await client.json().arrtrim("doc1", "$.nested1.a", 1, 0) == [0]
    assert await client.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrtrim("non_existing_doc", "..a", "0", 1)

    # Test legacy
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi (all paths are updated, but return result of last path)
    assert await client.json().arrtrim("doc1", "..a", "1", "-1") == 2

    # Test single
    assert await client.json().arrtrim("doc1", ".nested1.a", "1", "1") == 1
    assert await client.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrtrim("non_existing_doc", "..a", 1, 1)


@pytest.mark.redismod
async def test_objkeys_dollar(client: aioredis.Redis):
    await client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )

    # Test single
    assert await client.json().objkeys("doc1", "$.nested1.a") == [["foo", "bar"]]

    # Test legacy
    assert await client.json().objkeys("doc1", ".*.a") == ["foo", "bar"]
    # Test single
    assert await client.json().objkeys("doc1", ".nested2.a") == ["baz"]

    # Test missing key
    assert await client.json().objkeys("non_existing_doc", "..a") is None

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().objkeys("doc1", "$.nowhere")


@pytest.mark.redismod
async def test_objlen_dollar(client: aioredis.Redis):
    await client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert await client.json().objlen("doc1", "$..a") == [2, None, 1]
    # Test single
    assert await client.json().objlen("doc1", "$.nested1.a") == [2]

    # Test missing key
    assert await client.json().objlen("non_existing_doc", "$..a") is None

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await client.json().objlen("doc1", "$.nowhere")

    # Test legacy
    assert await client.json().objlen("doc1", ".*.a") == 2

    # Test single
    assert await client.json().objlen("doc1", ".nested2.a") == 1

    # Test missing key
    assert await client.json().objlen("non_existing_doc", "..a") is None

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await client.json().objlen("doc1", ".nowhere")


@pytest.mark.redismod
async def load_types_data(nested_key_name):
    td = {
        "object": {},
        "array": [],
        "string": "str",
        "integer": 42,
        "number": 1.2,
        "boolean": False,
        "null": None,
    }
    jdata = {}
    types = []
    for i, (k, v) in zip(range(1, len(td) + 1), iter(td.items())):
        jdata["nested" + str(i)] = {nested_key_name: v}
        types.append(k)

    return jdata, types


@pytest.mark.redismod
async def test_type_dollar(client: aioredis.Redis):
    jdata, jtypes = await load_types_data("a")
    await client.json().set("doc1", "$", jdata)
    # Test multi
    assert await client.json().type("doc1", "$..a") == jtypes

    # Test single
    assert await client.json().type("doc1", "$.nested2.a") == [jtypes[1]]

    # Test missing key
    assert await client.json().type("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_clear_dollar(client: aioredis.Redis):

    await client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert await client.json().clear("doc1", "$..a") == 3

    assert await client.json().get("doc1", "$") == [
        {"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}
    ]

    # Test single
    await client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    assert await client.json().clear("doc1", "$.nested1.a") == 1
    assert await client.json().get("doc1", "$") == [
        {
            "nested1": {"a": {}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        }
    ]

    # Test missing path (defaults to root)
    assert await client.json().clear("doc1") == 1
    assert await client.json().get("doc1", "$") == [{}]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().clear("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_toggle_dollar(client: aioredis.Redis):
    await client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": False},
            "nested2": {"a": 31},
            "nested3": {"a": True},
        },
    )
    # Test multi
    assert await client.json().toggle("doc1", "$..a") == [None, 1, None, 0]
    assert await client.json().get("doc1", "$") == [
        {
            "a": ["foo"],
            "nested1": {"a": True},
            "nested2": {"a": 31},
            "nested3": {"a": False},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await client.json().toggle("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_debug_dollar(client: aioredis.Redis):

    jdata, jtypes = await load_types_data("a")

    await client.json().set("doc1", "$", jdata)

    # Test multi
    assert await client.json().debug("MEMORY", "doc1", "$..a") == [
        72,
        24,
        24,
        16,
        16,
        1,
        0,
    ]

    # Test single
    assert await client.json().debug("MEMORY", "doc1", "$.nested2.a") == [24]

    # Test legacy
    assert await client.json().debug("MEMORY", "doc1", "..a") == 72

    # Test missing path (defaults to root)
    assert await client.json().debug("MEMORY", "doc1") == 72

    # Test missing key
    assert await client.json().debug("MEMORY", "non_existing_doc", "$..a") == []


@pytest.mark.redismod
async def test_resp_dollar(client: aioredis.Redis):

    data = {
        "L1": {
            "a": {
                "A1_B1": 10,
                "A1_B2": False,
                "A1_B3": {
                    "A1_B3_C1": None,
                    "A1_B3_C2": [
                        "A1_B3_C2_D1_1",
                        "A1_B3_C2_D1_2",
                        -19.5,
                        "A1_B3_C2_D1_4",
                        "A1_B3_C2_D1_5",
                        {"A1_B3_C2_D1_6_E1": True},
                    ],
                    "A1_B3_C3": [1],
                },
                "A1_B4": {
                    "A1_B4_C1": "foo",
                },
            },
        },
        "L2": {
            "a": {
                "A2_B1": 20,
                "A2_B2": False,
                "A2_B3": {
                    "A2_B3_C1": None,
                    "A2_B3_C2": [
                        "A2_B3_C2_D1_1",
                        "A2_B3_C2_D1_2",
                        -37.5,
                        "A2_B3_C2_D1_4",
                        "A2_B3_C2_D1_5",
                        {"A2_B3_C2_D1_6_E1": False},
                    ],
                    "A2_B3_C3": [2],
                },
                "A2_B4": {
                    "A2_B4_C1": "bar",
                },
            },
        },
    }
    await client.json().set("doc1", "$", data)
    # Test multi
    res = await client.json().resp("doc1", "$..a")
    assert res == [
        [
            "{",
            "A1_B1",
            10,
            "A1_B2",
            "false",
            "A1_B3",
            [
                "{",
                "A1_B3_C1",
                None,
                "A1_B3_C2",
                [
                    "[",
                    "A1_B3_C2_D1_1",
                    "A1_B3_C2_D1_2",
                    "-19.5",
                    "A1_B3_C2_D1_4",
                    "A1_B3_C2_D1_5",
                    ["{", "A1_B3_C2_D1_6_E1", "true"],
                ],
                "A1_B3_C3",
                ["[", 1],
            ],
            "A1_B4",
            ["{", "A1_B4_C1", "foo"],
        ],
        [
            "{",
            "A2_B1",
            20,
            "A2_B2",
            "false",
            "A2_B3",
            [
                "{",
                "A2_B3_C1",
                None,
                "A2_B3_C2",
                [
                    "[",
                    "A2_B3_C2_D1_1",
                    "A2_B3_C2_D1_2",
                    "-37.5",
                    "A2_B3_C2_D1_4",
                    "A2_B3_C2_D1_5",
                    ["{", "A2_B3_C2_D1_6_E1", "false"],
                ],
                "A2_B3_C3",
                ["[", 2],
            ],
            "A2_B4",
            ["{", "A2_B4_C1", "bar"],
        ],
    ]

    # Test single
    resSingle = await client.json().resp("doc1", "$.L1.a")
    assert resSingle == [
        [
            "{",
            "A1_B1",
            10,
            "A1_B2",
            "false",
            "A1_B3",
            [
                "{",
                "A1_B3_C1",
                None,
                "A1_B3_C2",
                [
                    "[",
                    "A1_B3_C2_D1_1",
                    "A1_B3_C2_D1_2",
                    "-19.5",
                    "A1_B3_C2_D1_4",
                    "A1_B3_C2_D1_5",
                    ["{", "A1_B3_C2_D1_6_E1", "true"],
                ],
                "A1_B3_C3",
                ["[", 1],
            ],
            "A1_B4",
            ["{", "A1_B4_C1", "foo"],
        ]
    ]

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await client.json().resp("doc1", "$.nowhere")

    # Test missing key
    assert await client.json().resp("non_existing_doc", "$..a") is None


@pytest.mark.redismod
async def test_arrindex_dollar(client: aioredis.Redis):

    await client.json().set(
        "store",
        "$",
        {
            "store": {
                "book": [
                    {
                        "category": "reference",
                        "author": "Nigel Rees",
                        "title": "Sayings of the Century",
                        "price": 8.95,
                        "size": [10, 20, 30, 40],
                    },
                    {
                        "category": "fiction",
                        "author": "Evelyn Waugh",
                        "title": "Sword of Honour",
                        "price": 12.99,
                        "size": [50, 60, 70, 80],
                    },
                    {
                        "category": "fiction",
                        "author": "Herman Melville",
                        "title": "Moby Dick",
                        "isbn": "0-553-21311-3",
                        "price": 8.99,
                        "size": [5, 10, 20, 30],
                    },
                    {
                        "category": "fiction",
                        "author": "J. R. R. Tolkien",
                        "title": "The Lord of the Rings",
                        "isbn": "0-395-19395-8",
                        "price": 22.99,
                        "size": [5, 6, 7, 8],
                    },
                ],
                "bicycle": {"color": "red", "price": 19.95},
            }
        },
    )

    assert await client.json().get("store", "$.store.book[?(@.price<10)].size") == [
        [10, 20, 30, 40],
        [5, 10, 20, 30],
    ]
    assert await client.json().arrindex(
        "store", "$.store.book[?(@.price<10)].size", "20"
    ) == [-1, -1]

    # Test index of int scalar in multi values
    await client.json().set(
        "test_num",
        ".",
        [
            {"arr": [0, 1, 3.0, 3, 2, 1, 0, 3]},
            {"nested1_found": {"arr": [5, 4, 3, 2, 1, 0, 1, 2, 3.0, 2, 4, 5]}},
            {"nested2_not_found": {"arr": [2, 4, 6]}},
            {"nested3_scalar": {"arr": "3"}},
            [
                {"nested41_not_arr": {"arr_renamed": [1, 2, 3]}},
                {"nested42_empty_arr": {"arr": []}},
            ],
        ],
    )

    assert await client.json().get("test_num", "$..arr") == [
        [0, 1, 3.0, 3, 2, 1, 0, 3],
        [5, 4, 3, 2, 1, 0, 1, 2, 3.0, 2, 4, 5],
        [2, 4, 6],
        "3",
        [],
    ]

    assert await client.json().arrindex("test_num", "$..arr", 3) == [3, 2, -1, None, -1]

    # Test index of double scalar in multi values
    assert await client.json().arrindex("test_num", "$..arr", 3.0) == [
        2,
        8,
        -1,
        None,
        -1,
    ]

    # Test index of string scalar in multi values
    await client.json().set(
        "test_string",
        ".",
        [
            {"arr": ["bazzz", "bar", 2, "baz", 2, "ba", "baz", 3]},
            {
                "nested1_found": {
                    "arr": [None, "baz2", "buzz", 2, 1, 0, 1, "2", "baz", 2, 4, 5]
                }
            },
            {"nested2_not_found": {"arr": ["baz2", 4, 6]}},
            {"nested3_scalar": {"arr": "3"}},
            [
                {"nested41_arr": {"arr_renamed": [1, "baz", 3]}},
                {"nested42_empty_arr": {"arr": []}},
            ],
        ],
    )
    assert await client.json().get("test_string", "$..arr") == [
        ["bazzz", "bar", 2, "baz", 2, "ba", "baz", 3],
        [None, "baz2", "buzz", 2, 1, 0, 1, "2", "baz", 2, 4, 5],
        ["baz2", 4, 6],
        "3",
        [],
    ]

    assert await client.json().arrindex("test_string", "$..arr", "baz") == [
        3,
        8,
        -1,
        None,
        -1,
    ]

    assert await client.json().arrindex("test_string", "$..arr", "baz", 2) == [
        3,
        8,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "baz", 4) == [
        6,
        8,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "baz", -5) == [
        3,
        8,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "baz", 4, 7) == [
        6,
        -1,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "baz", 4, -1) == [
        6,
        8,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "baz", 4, 0) == [
        6,
        8,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "5", 7, -1) == [
        -1,
        -1,
        -1,
        None,
        -1,
    ]
    assert await client.json().arrindex("test_string", "$..arr", "5", 7, 0) == [
        -1,
        -1,
        -1,
        None,
        -1,
    ]

    # Test index of None scalar in multi values
    await client.json().set(
        "test_None",
        ".",
        [
            {"arr": ["bazzz", "None", 2, None, 2, "ba", "baz", 3]},
            {
                "nested1_found": {
                    "arr": ["zaz", "baz2", "buzz", 2, 1, 0, 1, "2", None, 2, 4, 5]
                }
            },
            {"nested2_not_found": {"arr": ["None", 4, 6]}},
            {"nested3_scalar": {"arr": None}},
            [
                {"nested41_arr": {"arr_renamed": [1, None, 3]}},
                {"nested42_empty_arr": {"arr": []}},
            ],
        ],
    )
    assert await client.json().get("test_None", "$..arr") == [
        ["bazzz", "None", 2, None, 2, "ba", "baz", 3],
        ["zaz", "baz2", "buzz", 2, 1, 0, 1, "2", None, 2, 4, 5],
        ["None", 4, 6],
        None,
        [],
    ]

    # Fail with none-scalar value
    with pytest.raises(exceptions.ResponseError):
        await client.json().arrindex(
            "test_None", "$..nested42_empty_arr.arr", {"arr": []}
        )

    # Do not fail with none-scalar value in legacy mode
    assert (
        await client.json().arrindex(
            "test_None", ".[4][1].nested42_empty_arr.arr", '{"arr":[]}'
        )
        == -1
    )

    # Test legacy (path begins with dot)
    # Test index of int scalar in single value
    assert await client.json().arrindex("test_num", ".[0].arr", 3) == 3
    assert await client.json().arrindex("test_num", ".[0].arr", 9) == -1

    with pytest.raises(exceptions.ResponseError):
        await client.json().arrindex("test_num", ".[0].arr_not", 3)
    # Test index of string scalar in single value
    assert await client.json().arrindex("test_string", ".[0].arr", "baz") == 3
    assert await client.json().arrindex("test_string", ".[0].arr", "faz") == -1
    # Test index of None scalar in single value
    assert await client.json().arrindex("test_None", ".[0].arr", "None") == 1
    assert (
        await client.json().arrindex("test_None", "..nested2_not_found.arr", "None")
        == 0
    )


async def test_decoders_and_unstring():
    assert unstring("4") == 4
    assert unstring("45.55") == 45.55
    assert unstring("hello world") == "hello world"

    assert decode_list(b"45.55") == 45.55
    assert decode_list("45.55") == 45.55
    assert decode_list(["hello", b"world"]) == ["hello", "world"]


@pytest.mark.redismod
async def test_custom_decoder(client: aioredis.Redis):
    import json

    import ujson

    cj = client.json(encoder=ujson, decoder=ujson)
    assert await cj.set("foo", Path.rootPath(), "bar")
    assert "bar" == await cj.get("foo")
    assert await cj.get("baz") is None
    assert 1 == await cj.delete("foo")
    assert await client.exists("foo") == 0
    assert not isinstance(cj.__encoder__, json.JSONEncoder)
    assert not isinstance(cj.__decoder__, json.JSONDecoder)
