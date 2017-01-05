import pytest

from aioredis.commands.geo import GeoCoord, GeoMember


@pytest.mark.run_loop
@pytest.redis_version(
    3, 2, 0, reason='GEOADD is available since redis >= 3.2.0')
def test_geoadd(redis):
    res = yield from redis.geoadd('geodata', 13.361389, 38.115556, 'Palermo')
    assert res == 1

    res = yield from redis.geoadd(
        'geodata',
        15.087269, 37.502669, 'Catania',
        12.424315, 37.802105, 'Marsala'
    )
    assert res == 2


@pytest.mark.run_loop
@pytest.redis_version(
    3, 2, 0, reason='GEODIST is available since redis >= 3.2.0')
def test_geodist(redis):
    res = yield from redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = yield from redis.geodist('geodata', 'Palermo', 'Catania')
    assert res == 166274.1516

    res = yield from redis.geodist('geodata', 'Palermo', 'Catania', 'km')
    assert res == 166.2742


@pytest.mark.run_loop
@pytest.redis_version(
    3, 2, 0, reason='GEOHASH is available since redis >= 3.2.0')
def test_geohash(redis):
    res = yield from redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = yield from redis.geohash(
        'geodata', 'Palermo', encoding='utf-8'
    )
    assert res == ['sqc8b49rny0']

    res = yield from redis.geohash(
        'geodata', 'Palermo', 'Catania', encoding='utf-8'
    )
    assert res == ['sqc8b49rny0', 'sqdtr74hyu0']


@pytest.mark.run_loop
@pytest.redis_version(
    3, 2, 0, reason='GEOPOS is available since redis >= 3.2.0')
def test_geopos(redis):
    res = yield from redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = yield from redis.geopos('geodata', 'Palermo')
    assert res == [
        GeoCoord(longitude=13.36138933897018433, latitude=38.11555639549629859)
    ]

    res = yield from redis.geopos('geodata', 'Catania', 'Palermo')
    assert res == [
        GeoCoord(longitude=15.087267458438873, latitude=37.50266842333162),
        GeoCoord(longitude=13.36138933897018433, latitude=38.11555639549629859)
    ]


@pytest.mark.run_loop
@pytest.redis_version(
    3, 2, 0, reason='GEORADIUS is available since redis >= 3.2.0')
def test_georadius(redis):
    res = yield from redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo', 15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km', encoding='utf-8'
    )
    assert res == ['Palermo', 'Catania']

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km', count=1, encoding='utf-8'
    )
    assert res == ['Catania']

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km', sort='ASC', encoding='utf-8'
    )
    assert res == ['Catania', 'Palermo']

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km', with_dist=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(member='Palermo', dist=190.4424, coord=None, hash=None),
        GeoMember(member='Catania', dist=56.4413, coord=None, hash=None)
    ]

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_dist=True, with_coord=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=190.4424, hash=None,
            coord=GeoCoord(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=56.4413, hash=None,
            coord=GeoCoord(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_dist=True, with_coord=True, with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=190.4424, hash=3479099956230698,
            coord=GeoCoord(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=56.4413, hash=3479447370796909,
            coord=GeoCoord(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_coord=True, with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=None, hash=3479099956230698,
            coord=GeoCoord(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=None, hash=3479447370796909,
            coord=GeoCoord(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km', with_coord=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=None, hash=None,
            coord=GeoCoord(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=None, hash=None,
            coord=GeoCoord(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = yield from redis.georadius(
        'geodata', 15, 37, 200, 'km', count=1, sort='DESC',
        with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=None, hash=3479099956230698, coord=None
        )
    ]

    with pytest.raises(TypeError):
        res = yield from redis.georadius(
            'geodata', 15, 37, 200, 'km', count=1.3, encoding='utf-8'
        )
    with pytest.raises(TypeError):
        res = yield from redis.georadius(
            'geodata', 15, 37, '200', 'km', encoding='utf-8'
        )
    with pytest.raises(ValueError):
        res = yield from redis.georadius(
            'geodata', 15, 37, 200, 'k', encoding='utf-8'
        )
    with pytest.raises(ValueError):
        res = yield from redis.georadius(
            'geodata', 15, 37, 200, 'km', sort='DESV', encoding='utf-8'
        )


@pytest.mark.run_loop
@pytest.redis_version(
    3, 2, 0, reason='GEORADIUSBYMEMBER is available since redis >= 3.2.0')
def test_georadiusbymember(redis):
    res = yield from redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = yield from redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km', with_dist=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(member='Palermo', dist=0.0, coord=None, hash=None),
        GeoMember(member='Catania', dist=166.2742, coord=None, hash=None)
    ]

    res = yield from redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km',
        with_dist=True, with_coord=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=0.0, hash=None,
            coord=GeoCoord(13.361389338970184, 38.1155563954963)
        ),
        GeoMember(
            member='Catania', dist=166.2742, hash=None,
            coord=GeoCoord(15.087267458438873, 37.50266842333162)
        )
    ]

    res = yield from redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km',
        with_dist=True, with_coord=True, with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=0.0, hash=3479099956230698,
            coord=GeoCoord(13.361389338970184, 38.1155563954963)
        ),
        GeoMember(
            member='Catania', dist=166.2742, hash=3479447370796909,
            coord=GeoCoord(15.087267458438873, 37.50266842333162)
        )
    ]
