import asyncio
import bz2
import csv
import os
from io import TextIOWrapper

import pytest

import aioredis
import aioredis.commands.search
import aioredis.commands.search.aggregation as aggregations
import aioredis.commands.search.reducers as reducers
from aioredis import Redis
from aioredis.commands.json.path import Path
from aioredis.commands.search import Search
from aioredis.commands.search.field import GeoField, NumericField, TagField, TextField
from aioredis.commands.search.indexDefinition import IndexDefinition, IndexType
from aioredis.commands.search.query import GeoFilter, NumericFilter, Query
from aioredis.commands.search.result import Result
from aioredis.commands.search.suggestion import Suggestion

from .conftest import default_redismod_url, skip_ifmodversion_lt

pytestmark = pytest.mark.asyncio

WILL_PLAY_TEXT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "testdata", "will_play_text.csv.bz2")
)

TITLES_CSV = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "testdata", "titles.csv")
)


async def waitForIndex(env, idx, timeout=None):
    delay = 0.1
    while True:
        res = await env.execute_command("ft.info", idx)
        try:
            res.index("indexing")
        except ValueError:
            break

        if int(res[res.index("indexing") + 1]) == 0:
            break

        await asyncio.sleep(delay)
        if timeout is not None:
            timeout -= delay
            if timeout <= 0:
                break


def getClient():
    """
    Gets a client client attached to an index name which is ready to be
    created
    """
    rc = Redis.from_url(default_redismod_url, decode_responses=True)
    return rc


async def createIndex(client, num_docs=100, definition=None):
    try:
        await client.create_index(
            (TextField("play", weight=5.0), TextField("txt"), NumericField("chapter")),
            definition=definition,
        )
    except aioredis.ResponseError:
        await client.dropindex(delete_documents=True)
        return await createIndex(client, num_docs=num_docs, definition=definition)

    chapters = {}
    bzfp = TextIOWrapper(bz2.BZ2File(WILL_PLAY_TEXT), encoding="utf8")

    r = csv.reader(bzfp, delimiter=";")
    for n, line in enumerate(r):

        play, chapter, _, text = line[1], line[2], line[4], line[5]

        key = f"{play}:{chapter}".lower()
        d = chapters.setdefault(key, {})
        d["play"] = play
        d["txt"] = d.get("txt", "") + " " + text
        d["chapter"] = int(chapter or 0)
        if len(chapters) == num_docs:
            break

    indexer = client.batch_indexer(chunk_size=50)
    assert isinstance(indexer, Search.BatchIndexer)
    assert 50 == indexer.chunk_size

    for key, doc in chapters.items():
        await indexer.add_document(key, **doc)
    await indexer.commit()


# override the default module client, search requires both db=0, and text
@pytest.fixture
async def modclient():
    return Redis.from_url(default_redismod_url, db=0, decode_responses=True)


@pytest.fixture
async def client(modclient):
    await modclient.flushdb()
    return modclient


@pytest.mark.redismod
async def test_client(client):
    num_docs = 500
    await createIndex(client.ft(), num_docs=num_docs)
    await waitForIndex(client, "idx")
    # verify info
    info = await client.ft().info()
    for k in [
        "index_name",
        "index_options",
        "attributes",
        "num_docs",
        "max_doc_id",
        "num_terms",
        "num_records",
        "inverted_sz_mb",
        "offset_vectors_sz_mb",
        "doc_table_size_mb",
        "key_table_size_mb",
        "records_per_doc_avg",
        "bytes_per_record_avg",
        "offsets_per_term_avg",
        "offset_bits_per_record_avg",
    ]:
        assert k in info

    assert client.ft().index_name == info["index_name"]
    assert num_docs == int(info["num_docs"])

    res = await client.ft().search("henry iv")
    assert isinstance(res, Result)
    assert 225 == res.total
    assert 10 == len(res.docs)
    assert res.duration > 0

    for doc in res.docs:
        assert doc.id
        assert doc.play == "Henry IV"
        assert len(doc.txt) > 0

    # test no content
    res = await client.ft().search(Query("king").no_content())
    assert 194 == res.total
    assert 10 == len(res.docs)
    for doc in res.docs:
        assert "txt" not in doc.__dict__
        assert "play" not in doc.__dict__

    # test verbatim vs no verbatim
    total = (await client.ft().search(Query("kings").no_content())).total
    vtotal = (await client.ft().search(Query("kings").no_content().verbatim())).total
    assert total > vtotal

    # test in fields
    txt_total = (
        await client.ft().search(Query("henry").no_content().limit_fields("txt"))
    ).total
    play_total = (
        await client.ft().search(Query("henry").no_content().limit_fields("play"))
    ).total
    both_total = (
        await client.ft().search(
            Query("henry").no_content().limit_fields("play", "txt")
        )
    ).total
    assert 129 == txt_total
    assert 494 == play_total
    assert 494 == both_total

    # test load_document
    doc = await client.ft().load_document("henry vi part 3:62")
    assert doc is not None
    assert "henry vi part 3:62" == doc.id
    assert doc.play == "Henry VI Part 3"
    assert len(doc.txt) > 0

    # test in-keys
    ids = [x.id for x in (await client.ft().search(Query("henry"))).docs]
    assert 10 == len(ids)
    subset = ids[:5]
    docs = await client.ft().search(Query("henry").limit_ids(*subset))
    assert len(subset) == docs.total
    ids = [x.id for x in docs.docs]
    assert set(ids) == set(subset)

    # test slop and in order
    assert 193 == (await client.ft().search(Query("henry king"))).total
    assert 3 == (await client.ft().search(Query("henry king").slop(0).in_order())).total
    assert (
        52 == (await client.ft().search(Query("king henry").slop(0).in_order())).total
    )
    assert 53 == (await client.ft().search(Query("henry king").slop(0))).total
    assert 167 == (await client.ft().search(Query("henry king").slop(100))).total

    # test delete document
    await client.ft().add_document("doc-5ghs2", play="Death of a Salesman")
    res = await client.ft().search(Query("death of a salesman"))
    assert 1 == res.total

    assert 1 == await client.ft().delete_document("doc-5ghs2")
    res = await client.ft().search(Query("death of a salesman"))
    assert 0 == res.total
    assert 0 == await client.ft().delete_document("doc-5ghs2")

    await client.ft().add_document("doc-5ghs2", play="Death of a Salesman")
    res = await client.ft().search(Query("death of a salesman"))
    assert 1 == res.total
    await client.ft().delete_document("doc-5ghs2")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_payloads(client):
    await client.ft().create_index((TextField("txt"),))

    await client.ft().add_document("doc1", payload="foo baz", txt="foo bar")
    await client.ft().add_document("doc2", txt="foo bar")

    q = Query("foo bar").with_payloads()
    res = await client.ft().search(q)
    assert 2 == res.total
    assert "doc1" == res.docs[0].id
    assert "doc2" == res.docs[1].id
    assert "foo baz" == res.docs[0].payload
    assert res.docs[1].payload is None


@pytest.mark.redismod
async def test_scores(client):
    await client.ft().create_index((TextField("txt"),))

    await client.ft().add_document("doc1", txt="foo baz")
    await client.ft().add_document("doc2", txt="foo bar")

    q = Query("foo ~bar").with_scores()
    res = await client.ft().search(q)
    assert 2 == res.total
    assert "doc2" == res.docs[0].id
    assert 3.0 == res.docs[0].score
    assert "doc1" == res.docs[1].id
    # todo: enable once new RS version is tagged
    # self.assertEqual(0.2, res.docs[1].score)


@pytest.mark.redismod
async def test_replace(client):
    await client.ft().create_index((TextField("txt"),))

    await client.ft().add_document("doc1", txt="foo bar")
    await client.ft().add_document("doc2", txt="foo bar")
    waitForIndex(client, "idx")

    res = await client.ft().search("foo bar")
    assert 2 == res.total
    await client.ft().add_document("doc1", replace=True, txt="this is a replaced doc")

    res = await client.ft().search("foo bar")
    assert 1 == res.total
    assert "doc2" == res.docs[0].id

    res = await client.ft().search("replaced doc")
    assert 1 == res.total
    assert "doc1" == res.docs[0].id


@pytest.mark.redismod
async def test_stopwords(client):
    await client.ft().create_index((TextField("txt"),), stopwords=["foo", "bar", "baz"])
    await client.ft().add_document("doc1", txt="foo bar")
    await client.ft().add_document("doc2", txt="hello world")
    waitForIndex(client, "idx")

    q1 = Query("foo bar").no_content()
    q2 = Query("foo bar hello world").no_content()
    res1, res2 = await client.ft().search(q1), await client.ft().search(q2)
    assert 0 == res1.total
    assert 1 == res2.total


@pytest.mark.redismod
async def test_filters(client):
    await client.ft().create_index(
        (TextField("txt"), NumericField("num"), GeoField("loc"))
    )
    await client.ft().add_document(
        "doc1", txt="foo bar", num=3.141, loc="-0.441,51.458"
    )
    await client.ft().add_document("doc2", txt="foo baz", num=2, loc="-0.1,51.2")

    waitForIndex(client, "idx")
    # Test numerical filter
    q1 = Query("foo").add_filter(NumericFilter("num", 0, 2)).no_content()
    q2 = (
        Query("foo")
        .add_filter(NumericFilter("num", 2, NumericFilter.INF, minExclusive=True))
        .no_content()
    )
    res1, res2 = await client.ft().search(q1), await client.ft().search(q2)

    assert 1 == res1.total
    assert 1 == res2.total
    assert "doc2" == res1.docs[0].id
    assert "doc1" == res2.docs[0].id

    # Test geo filter
    q1 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 10)).no_content()
    q2 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 100)).no_content()
    res1, res2 = await client.ft().search(q1), await client.ft().search(q2)

    assert 1 == res1.total
    assert 2 == res2.total
    assert "doc1" == res1.docs[0].id

    # Sort results, after RDB reload order may change
    res = [res2.docs[0].id, res2.docs[1].id]
    res.sort()
    assert ["doc1", "doc2"] == res


@pytest.mark.redismod
async def test_payloads_with_no_content(client):
    await client.ft().create_index((TextField("txt"),))
    await client.ft().add_document("doc1", payload="foo baz", txt="foo bar")
    await client.ft().add_document("doc2", payload="foo baz2", txt="foo bar")

    q = Query("foo bar").with_payloads().no_content()
    res = await client.ft().search(q)
    assert 2 == len(res.docs)


@pytest.mark.redismod
async def test_sort_by(client):
    await client.ft().create_index(
        (TextField("txt"), NumericField("num", sortable=True))
    )
    await client.ft().add_document("doc1", txt="foo bar", num=1)
    await client.ft().add_document("doc2", txt="foo baz", num=2)
    await client.ft().add_document("doc3", txt="foo qux", num=3)

    # Test sort
    q1 = Query("foo").sort_by("num", asc=True).no_content()
    q2 = Query("foo").sort_by("num", asc=False).no_content()
    res1, res2 = await client.ft().search(q1), await client.ft().search(q2)

    assert 3 == res1.total
    assert "doc1" == res1.docs[0].id
    assert "doc2" == res1.docs[1].id
    assert "doc3" == res1.docs[2].id
    assert 3 == res2.total
    assert "doc1" == res2.docs[2].id
    assert "doc2" == res2.docs[1].id
    assert "doc3" == res2.docs[0].id


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_drop_index():
    """
    Ensure the index gets dropped by data remains by default
    """
    for x in range(20):
        for keep_docs in [[True, {}], [False, {"name": "haveit"}]]:
            idx = "HaveIt"
            index = getClient()
            await index.hset("index:haveit", mapping={"name": "haveit"})
            idef = IndexDefinition(prefix=["index:"])
            await index.ft(idx).create_index((TextField("name"),), definition=idef)
            await waitForIndex(index, idx)
            await index.ft(idx).dropindex(delete_documents=keep_docs[0])
            i = await index.hgetall("index:haveit")
            assert i == keep_docs[1]


@pytest.mark.redismod
async def test_example(client):
    # Creating the index definition and schema
    await client.ft().create_index((TextField("title", weight=5.0), TextField("body")))

    # Indexing a document
    await client.ft().add_document(
        "doc1",
        title="RediSearch",
        body="Redisearch impements a search engine on top of redis",
    )

    # Searching with complex parameters:
    q = Query("search engine").verbatim().no_content().paging(0, 5)

    res = await client.ft().search(q)
    assert res is not None


@pytest.mark.redismod
async def test_auto_complete(client):
    n = 0
    with open(TITLES_CSV) as f:
        cr = csv.reader(f)

        for row in cr:
            n += 1
            term, score = row[0], float(row[1])
            assert n == await client.ft().sugadd("ac", Suggestion(term, score=score))

    assert n == await client.ft().suglen("ac")
    ret = await client.ft().sugget("ac", "bad", with_scores=True)
    assert 2 == len(ret)
    assert "badger" == ret[0].string
    assert isinstance(ret[0].score, float)
    assert 1.0 != ret[0].score
    assert "badalte rishtey" == ret[1].string
    assert isinstance(ret[1].score, float)
    assert 1.0 != ret[1].score

    ret = await client.ft().sugget("ac", "bad", fuzzy=True, num=10)
    assert 10 == len(ret)
    assert 1.0 == ret[0].score
    strs = {x.string for x in ret}

    for sug in strs:
        assert 1 == await client.ft().sugdel("ac", sug)
    # make sure a second delete returns 0
    for sug in strs:
        assert 0 == await client.ft().sugdel("ac", sug)

    # make sure they were actually deleted
    ret2 = await client.ft().sugget("ac", "bad", fuzzy=True, num=10)
    for sug in ret2:
        assert sug.string not in strs

    # Test with payload
    await client.ft().sugadd("ac", Suggestion("pay1", payload="pl1"))
    await client.ft().sugadd("ac", Suggestion("pay2", payload="pl2"))
    await client.ft().sugadd("ac", Suggestion("pay3", payload="pl3"))

    sugs = await client.ft().sugget("ac", "pay", with_payloads=True, with_scores=True)
    assert 3 == len(sugs)
    for sug in sugs:
        assert sug.payload
        assert sug.payload.startswith("pl")


@pytest.mark.redismod
async def test_no_index(client):
    await client.ft().create_index(
        (
            TextField("field"),
            TextField("text", no_index=True, sortable=True),
            NumericField("numeric", no_index=True, sortable=True),
            GeoField("geo", no_index=True, sortable=True),
            TagField("tag", no_index=True, sortable=True),
        )
    )

    await client.ft().add_document(
        "doc1", field="aaa", text="1", numeric="1", geo="1,1", tag="1"
    )
    await client.ft().add_document(
        "doc2", field="aab", text="2", numeric="2", geo="2,2", tag="2"
    )
    waitForIndex(client, "idx")

    res = await client.ft().search(Query("@text:aa*"))
    assert 0 == res.total

    res = await client.ft().search(Query("@field:aa*"))
    assert 2 == res.total

    res = await client.ft().search(Query("*").sort_by("text", asc=False))
    assert 2 == res.total
    assert "doc2" == res.docs[0].id

    res = await client.ft().search(Query("*").sort_by("text", asc=True))
    assert "doc1" == res.docs[0].id

    res = await client.ft().search(Query("*").sort_by("numeric", asc=True))
    assert "doc1" == res.docs[0].id

    res = await client.ft().search(Query("*").sort_by("geo", asc=True))
    assert "doc1" == res.docs[0].id

    res = await client.ft().search(Query("*").sort_by("tag", asc=True))
    assert "doc1" == res.docs[0].id

    # Ensure exception is raised for non-indexable, non-sortable fields
    with pytest.raises(Exception):
        TextField("name", no_index=True, sortable=False)
    with pytest.raises(Exception):
        NumericField("name", no_index=True, sortable=False)
    with pytest.raises(Exception):
        GeoField("name", no_index=True, sortable=False)
    with pytest.raises(Exception):
        TagField("name", no_index=True, sortable=False)


@pytest.mark.redismod
async def test_partial(client):
    await client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
    await client.ft().add_document("doc1", f1="f1_val", f2="f2_val")
    await client.ft().add_document("doc2", f1="f1_val", f2="f2_val")
    await client.ft().add_document("doc1", f3="f3_val", partial=True)
    await client.ft().add_document("doc2", f3="f3_val", replace=True)
    waitForIndex(client, "idx")

    # Search for f3 value. All documents should have it
    res = await client.ft().search("@f3:f3_val")
    assert 2 == res.total

    # Only the document updated with PARTIAL should still have f1 and f2 values
    res = await client.ft().search("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert 1 == res.total


@pytest.mark.redismod
async def test_no_create(client):
    await client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
    await client.ft().add_document("doc1", f1="f1_val", f2="f2_val")
    await client.ft().add_document("doc2", f1="f1_val", f2="f2_val")
    await client.ft().add_document("doc1", f3="f3_val", no_create=True)
    await client.ft().add_document("doc2", f3="f3_val", no_create=True, partial=True)
    waitForIndex(client, "idx")

    # Search for f3 value. All documents should have it
    res = await client.ft().search("@f3:f3_val")
    assert 2 == res.total

    # Only the document updated with PARTIAL should still have f1 and f2 values
    res = await client.ft().search("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert 1 == res.total

    with pytest.raises(aioredis.ResponseError):
        await client.ft().add_document("doc3", f2="f2_val", f3="f3_val", no_create=True)


@pytest.mark.redismod
async def test_explain(client):
    await client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
    res = await client.ft().explain("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert res


@pytest.mark.redismod
async def test_explaincli(client):
    with pytest.raises(NotImplementedError):
        await client.ft().explain_cli("foo")


@pytest.mark.redismod
async def test_summarize(client):
    await createIndex(client.ft())
    await waitForIndex(client, "idx")

    q = Query("king henry").paging(0, 1)
    q.highlight(fields=("play", "txt"), tags=("<b>", "</b>"))
    q.summarize("txt")

    doc = sorted((await client.ft().search(q)).docs)[0]
    assert "<b>Henry</b> IV" == doc.play
    assert (
        "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "
        == doc.txt
    )

    q = Query("king henry").paging(0, 1).summarize().highlight()

    doc = sorted((await client.ft().search(q)).docs)[0]
    assert "<b>Henry</b> ... " == doc.play
    assert (
        "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "
        == doc.txt
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_alias():
    index1 = getClient()
    index2 = getClient()

    def1 = IndexDefinition(prefix=["index1:"])
    def2 = IndexDefinition(prefix=["index2:"])

    ftindex1 = index1.ft("testAlias")
    ftindex2 = index2.ft("testAlias2")
    await ftindex1.create_index((TextField("name"),), definition=def1)
    await ftindex2.create_index((TextField("name"),), definition=def2)

    await index1.hset("index1:lonestar", mapping={"name": "lonestar"})
    await index2.hset("index2:yogurt", mapping={"name": "yogurt"})

    res = (await ftindex1.search("*")).docs[0]
    assert "index1:lonestar" == res.id

    # create alias and check for results
    await ftindex1.aliasadd("spaceballs")
    alias_client = getClient().ft("spaceballs")
    res = (await alias_client.search("*")).docs[0]
    assert "index1:lonestar" == res.id

    # Throw an exception when trying to add an alias that already exists
    with pytest.raises(Exception):
        await ftindex2.aliasadd("spaceballs")

    # update alias and ensure new results
    await ftindex2.aliasupdate("spaceballs")
    alias_client2 = getClient().ft("spaceballs")

    res = (await alias_client2.search("*")).docs[0]
    assert "index2:yogurt" == res.id

    await ftindex2.aliasdel("spaceballs")
    with pytest.raises(Exception):
        (await alias_client2.search("*")).docs[0]


@pytest.mark.redismod
async def test_alias_basic():
    # Creating a client with one index
    await getClient().flushdb()
    index1 = getClient().ft("testAlias")

    await index1.create_index((TextField("txt"),))
    await index1.add_document("doc1", txt="text goes here")

    index2 = getClient().ft("testAlias2")
    await index2.create_index((TextField("txt"),))
    await index2.add_document("doc2", txt="text goes here")

    # add the actual alias and check
    await index1.aliasadd("myalias")
    alias_client = getClient().ft("myalias")
    res = sorted((await alias_client.search("*")).docs, key=lambda x: x.id)
    assert "doc1" == res[0].id

    # Throw an exception when trying to add an alias that already exists
    with pytest.raises(Exception):
        await index2.aliasadd("myalias")

    # update the alias and ensure we get doc2
    await index2.aliasupdate("myalias")
    alias_client2 = getClient().ft("myalias")
    res = sorted((await alias_client2.search("*")).docs, key=lambda x: x.id)
    assert "doc1" == res[0].id

    # delete the alias and expect an error if we try to query again
    await index2.aliasdel("myalias")
    with pytest.raises(Exception):
        _ = (await alias_client2.search("*")).docs[0]


@pytest.mark.redismod
async def test_tags(client):
    await client.ft().create_index((TextField("txt"), TagField("tags")))
    tags = "foo,foo bar,hello;world"
    tags2 = "soba,ramen"

    await client.ft().add_document("doc1", txt="fooz barz", tags=tags)
    await client.ft().add_document("doc2", txt="noodles", tags=tags2)
    await waitForIndex(client, "idx")

    q = Query("@tags:{foo}")
    res = await client.ft().search(q)
    assert 1 == res.total

    q = Query("@tags:{foo bar}")
    res = await client.ft().search(q)
    assert 1 == res.total

    q = Query("@tags:{foo\\ bar}")
    res = await client.ft().search(q)
    assert 1 == res.total

    q = Query("@tags:{hello\\;world}")
    res = await client.ft().search(q)
    assert 1 == res.total

    q2 = await client.ft().tagvals("tags")
    assert (tags.split(",") + tags2.split(",")).sort() == q2.sort()


@pytest.mark.redismod
async def test_textfield_sortable_nostem(client):
    # Creating the index definition with sortable and no_stem
    await client.ft().create_index((TextField("txt", sortable=True, no_stem=True),))

    # Now get the index info to confirm its contents
    response = await client.ft().info()
    assert "SORTABLE" in response["attributes"][0]
    assert "NOSTEM" in response["attributes"][0]


@pytest.mark.redismod
async def test_alter_schema_add(client):
    # Creating the index definition and schema
    await client.ft().create_index(TextField("title"))

    # Using alter to add a field
    await client.ft().alter_schema_add(TextField("body"))

    # Indexing a document
    await client.ft().add_document(
        "doc1", title="MyTitle", body="Some content only in the body"
    )

    # Searching with parameter only in the body (the added field)
    q = Query("only in the body")

    # Ensure we find the result searching on the added body field
    res = await client.ft().search(q)
    assert 1 == res.total


@pytest.mark.redismod
async def test_spell_check(client):
    await client.ft().create_index((TextField("f1"), TextField("f2")))

    await client.ft().add_document(
        "doc1", f1="some valid content", f2="this is sample text"
    )
    await client.ft().add_document("doc2", f1="very important", f2="lorem ipsum")
    waitForIndex(client, "idx")

    # test spellcheck
    res = await client.ft().spellcheck("impornant")
    assert "important" == res["impornant"][0]["suggestion"]

    res = await client.ft().spellcheck("contnt")
    assert "content" == res["contnt"][0]["suggestion"]

    # test spellcheck with Levenshtein distance
    res = await client.ft().spellcheck("vlis")
    assert res == {}
    res = await client.ft().spellcheck("vlis", distance=2)
    assert "valid" == res["vlis"][0]["suggestion"]

    # test spellcheck include
    await client.ft().dict_add("dict", "lore", "lorem", "lorm")
    res = await client.ft().spellcheck("lorm", include="dict")
    assert len(res["lorm"]) == 3
    assert (
        res["lorm"][0]["suggestion"],
        res["lorm"][1]["suggestion"],
        res["lorm"][2]["suggestion"],
    ) == ("lorem", "lore", "lorm")
    assert (res["lorm"][0]["score"], res["lorm"][1]["score"]) == ("0.5", "0")

    # test spellcheck exclude
    res = await client.ft().spellcheck("lorm", exclude="dict")
    assert res == {}


@pytest.mark.redismod
async def test_dict_operations(client):
    await client.ft().create_index((TextField("f1"), TextField("f2")))
    # Add three items
    res = await client.ft().dict_add("custom_dict", "item1", "item2", "item3")
    assert 3 == res

    # Remove one item
    res = await client.ft().dict_del("custom_dict", "item2")
    assert 1 == res

    # Dump dict and inspect content
    res = await client.ft().dict_dump("custom_dict")
    assert ["item1", "item3"] == res

    # Remove rest of the items before reload
    await client.ft().dict_del("custom_dict", *res)


@pytest.mark.redismod
async def test_phonetic_matcher(client):
    await client.ft().create_index((TextField("name"),))
    await client.ft().add_document("doc1", name="Jon")
    await client.ft().add_document("doc2", name="John")

    res = await client.ft().search(Query("Jon"))
    assert 1 == len(res.docs)
    assert "Jon" == res.docs[0].name

    # Drop and create index with phonetic matcher
    await client.flushdb()

    await client.ft().create_index((TextField("name", phonetic_matcher="dm:en"),))
    await client.ft().add_document("doc1", name="Jon")
    await client.ft().add_document("doc2", name="John")

    res = await client.ft().search(Query("Jon"))
    assert 2 == len(res.docs)
    assert ["John", "Jon"] == sorted(d.name for d in res.docs)


@pytest.mark.redismod
async def test_scorer(client):
    await client.ft().create_index((TextField("description"),))

    await client.ft().add_document(
        "doc1", description="The quick brown fox jumps over the lazy dog"
    )
    await client.ft().add_document(
        "doc2",
        description="Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.",
    )

    # default scorer is TFIDF
    res = await client.ft().search(Query("quick").with_scores())
    assert 1.0 == res.docs[0].score
    res = await client.ft().search(Query("quick").scorer("TFIDF").with_scores())
    assert 1.0 == res.docs[0].score
    res = await client.ft().search(Query("quick").scorer("TFIDF.DOCNORM").with_scores())
    assert 0.1111111111111111 == res.docs[0].score
    res = await client.ft().search(Query("quick").scorer("BM25").with_scores())
    assert 0.17699114465425977 == res.docs[0].score
    res = await client.ft().search(Query("quick").scorer("DISMAX").with_scores())
    assert 2.0 == res.docs[0].score
    res = await client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
    assert 1.0 == res.docs[0].score
    res = await client.ft().search(Query("quick").scorer("HAMMING").with_scores())
    assert 0.0 == res.docs[0].score


@pytest.mark.redismod
async def test_get(client):
    await client.ft().create_index((TextField("f1"), TextField("f2")))

    assert [None] == await client.ft().get("doc1")
    assert [None, None] == await client.ft().get("doc2", "doc1")

    await client.ft().add_document(
        "doc1", f1="some valid content dd1", f2="this is sample text ff1"
    )
    await client.ft().add_document(
        "doc2", f1="some valid content dd2", f2="this is sample text ff2"
    )

    assert [
        ["f1", "some valid content dd2", "f2", "this is sample text ff2"]
    ] == await client.ft().get("doc2")
    assert [
        ["f1", "some valid content dd1", "f2", "this is sample text ff1"],
        ["f1", "some valid content dd2", "f2", "this is sample text ff2"],
    ] == await client.ft().get("doc1", "doc2")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_config(client):
    assert await client.ft().config_set("TIMEOUT", "100")
    with pytest.raises(aioredis.ResponseError):
        await client.ft().config_set("TIMEOUT", "null")
    res = await client.ft().config_get("*")
    assert "100" == res["TIMEOUT"]
    res = await client.ft().config_get("TIMEOUT")
    assert "100" == res["TIMEOUT"]


@pytest.mark.redismod
async def test_aggregations(client):
    # Creating the index definition and schema
    await client.ft().create_index(
        (
            NumericField("random_num"),
            TextField("title"),
            TextField("body"),
            TextField("parent"),
        )
    )

    # Indexing a document
    await client.ft().add_document(
        "search",
        title="RediSearch",
        body="Redisearch impements a search engine on top of redis",
        parent="redis",
        random_num=10,
    )
    await client.ft().add_document(
        "ai",
        title="RedisAI",
        body="RedisAI executes Deep Learning/Machine Learning models and managing their data.",
        parent="redis",
        random_num=3,
    )
    await client.ft().add_document(
        "json",
        title="RedisJson",
        body="RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",
        parent="redis",
        random_num=8,
    )

    req = aggregations.AggregateRequest("redis").group_by(
        "@parent",
        reducers.count(),
        reducers.count_distinct("@title"),
        reducers.count_distinctish("@title"),
        reducers.sum("@random_num"),
        reducers.min("@random_num"),
        reducers.max("@random_num"),
        reducers.avg("@random_num"),
        reducers.stddev("random_num"),
        reducers.quantile("@random_num", 0.5),
        reducers.tolist("@title"),
        reducers.first_value("@title"),
        reducers.random_sample("@title", 2),
    )

    res = await client.ft().aggregate(req)

    res = res.rows[0]
    assert len(res) == 26
    assert "redis" == res[1]
    assert "3" == res[3]
    assert "3" == res[5]
    assert "3" == res[7]
    assert "21" == res[9]
    assert "3" == res[11]
    assert "10" == res[13]
    assert "7" == res[15]
    assert "3.60555127546" == res[17]
    assert "10" == res[19]
    assert ["RediSearch", "RedisAI", "RedisJson"] == res[21]
    assert "RediSearch" == res[23]
    assert 2 == len(res[25])


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_index_definition(client):
    """
    Create definition and test its args
    """
    with pytest.raises(RuntimeError):
        IndexDefinition(prefix=["hset:", "henry"], index_type="json")

    definition = IndexDefinition(
        prefix=["hset:", "henry"],
        filter="@f1==32",
        language="English",
        language_field="play",
        score_field="chapter",
        score=0.5,
        payload_field="txt",
        index_type=IndexType.JSON,
    )

    assert [
        "ON",
        "JSON",
        "PREFIX",
        2,
        "hset:",
        "henry",
        "FILTER",
        "@f1==32",
        "LANGUAGE_FIELD",
        "play",
        "LANGUAGE",
        "English",
        "SCORE_FIELD",
        "chapter",
        "SCORE",
        0.5,
        "PAYLOAD_FIELD",
        "txt",
    ] == definition.args

    createIndex(client.ft(), num_docs=500, definition=definition)


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_create_client_definition(client):
    """
    Create definition with no index type provided,
    and use hset to test the client definition (the default is HASH).
    """
    definition = IndexDefinition(prefix=["hset:", "henry"])
    await createIndex(client.ft(), num_docs=500, definition=definition)

    info = await client.ft().info()
    assert 494 == int(info["num_docs"])

    await client.ft().client.hset("hset:1", "f1", "v1")
    info = await client.ft().info()
    assert 495 == int(info["num_docs"])


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_create_client_definition_hash(client):
    """
    Create definition with IndexType.HASH as index type (ON HASH),
    and use hset to test the client definition.
    """
    definition = IndexDefinition(prefix=["hset:", "henry"], index_type=IndexType.HASH)
    await createIndex(client.ft(), num_docs=500, definition=definition)

    info = await client.ft().info()
    assert 494 == int(info["num_docs"])

    await client.ft().client.hset("hset:1", "f1", "v1")
    info = await client.ft().info()
    assert 495 == int(info["num_docs"])


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_create_client_definition_json(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON),
    and use json client to test it.
    """
    definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
    await client.ft().create_index((TextField("$.name"),), definition=definition)

    await client.json().set("king:1", Path.rootPath(), {"name": "henry"})
    await client.json().set("king:2", Path.rootPath(), {"name": "james"})

    res = await client.ft().search("henry")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].payload is None
    assert res.docs[0].json == '{"name":"henry"}'
    assert res.total == 1


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_fields_as_name(client):
    # create index
    SCHEMA = (
        TextField("$.name", sortable=True, as_name="name"),
        NumericField("$.age", as_name="just_a_number"),
    )
    definition = IndexDefinition(index_type=IndexType.JSON)
    await client.ft().create_index(SCHEMA, definition=definition)

    # insert json data
    res = await client.json().set("doc:1", Path.rootPath(), {"name": "Jon", "age": 25})
    assert res

    total = (
        await client.ft().search(Query("Jon").return_fields("name", "just_a_number"))
    ).docs
    assert 1 == len(total)
    assert "doc:1" == total[0].id
    assert "Jon" == total[0].name
    assert "25" == total[0].just_a_number


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_search_return_fields(client):
    res = await client.json().set(
        "doc:1",
        Path.rootPath(),
        {"t": "riceratops", "t2": "telmatosaurus", "n": 9072, "flt": 97.2},
    )
    assert res

    # create index on
    definition = IndexDefinition(index_type=IndexType.JSON)
    SCHEMA = (
        TextField("$.t"),
        NumericField("$.flt"),
    )
    await client.ft().create_index(SCHEMA, definition=definition)
    waitForIndex(client, "idx")

    total = (
        await client.ft().search(Query("*").return_field("$.t", as_field="txt"))
    ).docs
    assert 1 == len(total)
    assert "doc:1" == total[0].id
    assert "riceratops" == total[0].txt

    total = (
        await client.ft().search(Query("*").return_field("$.t2", as_field="txt"))
    ).docs
    assert 1 == len(total)
    assert "doc:1" == total[0].id
    assert "telmatosaurus" == total[0].txt


@pytest.mark.redismod
async def test_synupdate(client):
    definition = IndexDefinition(index_type=IndexType.HASH)
    await client.ft().create_index(
        (
            TextField("title"),
            TextField("body"),
        ),
        definition=definition,
    )

    await client.ft().synupdate("id1", True, "boy", "child", "offspring")
    await client.ft().add_document("doc1", title="he is a baby", body="this is a test")

    await client.ft().synupdate("id1", True, "baby")
    await client.ft().add_document(
        "doc2", title="he is another baby", body="another test"
    )

    res = await client.ft().search(Query("child").expander("SYNONYM"))
    assert res.docs[0].id == "doc2"
    assert res.docs[0].title == "he is another baby"
    assert res.docs[0].body == "another test"


@pytest.mark.redismod
async def test_syndump(client):
    definition = IndexDefinition(index_type=IndexType.HASH)
    await client.ft().create_index(
        (
            TextField("title"),
            TextField("body"),
        ),
        definition=definition,
    )

    await client.ft().synupdate("id1", False, "boy", "child", "offspring")
    await client.ft().synupdate("id2", False, "baby", "child")
    await client.ft().synupdate("id3", False, "tree", "wood")
    res = await client.ft().syndump()
    assert res == {
        "boy": ["id1"],
        "tree": ["id3"],
        "wood": ["id3"],
        "child": ["id1", "id2"],
        "baby": ["id2"],
        "offspring": ["id1"],
    }


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_create_json_with_alias(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON) with two
    fields with aliases, and use json client to test it.
    """
    definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
    await client.ft().create_index(
        (TextField("$.name", as_name="name"), NumericField("$.num", as_name="num")),
        definition=definition,
    )

    await client.json().set("king:1", Path.rootPath(), {"name": "henry", "num": 42})
    await client.json().set("king:2", Path.rootPath(), {"name": "james", "num": 3.14})

    res = await client.ft().search("@name:henry")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].json == '{"name":"henry","num":42}'
    assert res.total == 1

    res = await client.ft().search("@num:[0 10]")
    assert res.docs[0].id == "king:2"
    assert res.docs[0].json == '{"name":"james","num":3.14}'
    assert res.total == 1

    # Tests returns an error if path contain special characters (user should
    # use an alias)
    with pytest.raises(Exception):
        await client.ft().search("@$.name:henry")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_json_with_multipath(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON),
    and use json client to test it.
    """
    definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
    await client.ft().create_index(
        (TagField("$..name", as_name="name")), definition=definition
    )

    await client.json().set(
        "king:1", Path.rootPath(), {"name": "henry", "country": {"name": "england"}}
    )

    res = await client.ft().search("@name:{henry}")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
    assert res.total == 1

    res = await client.ft().search("@name:{england}")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
    assert res.total == 1


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
async def test_json_with_jsonpath(client):
    definition = IndexDefinition(index_type=IndexType.JSON)
    await client.ft().create_index(
        (
            TextField('$["prod:name"]', as_name="name"),
            TextField("$.prod:name", as_name="name_unsupported"),
        ),
        definition=definition,
    )

    await client.json().set("doc:1", Path.rootPath(), {"prod:name": "RediSearch"})

    # query for a supported field succeeds
    res = await client.ft().search(Query("@name:RediSearch"))
    assert res.total == 1
    assert res.docs[0].id == "doc:1"
    assert res.docs[0].json == '{"prod:name":"RediSearch"}'

    # query for an unsupported field fails
    res = await client.ft().search("@name_unsupported:RediSearch")
    assert res.total == 0

    # return of a supported field succeeds
    res = await client.ft().search(Query("@name:RediSearch").return_field("name"))
    assert res.total == 1
    assert res.docs[0].id == "doc:1"
    assert res.docs[0].name == "RediSearch"

    # return of an unsupported field fails
    res = await client.ft().search(
        Query("@name:RediSearch").return_field("name_unsupported")
    )
    assert res.total == 1
    assert res.docs[0].id == "doc:1"
    with pytest.raises(Exception):
        res.docs[0].name_unsupported
