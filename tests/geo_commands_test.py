import pytest

from aioredis import GeoPoint, GeoMember
from _testutils import redis_version


@redis_version(
    3, 2, 0, reason='GEOADD is available since redis >= 3.2.0')
async def test_geoadd(redis):
    res = await redis.geoadd('geodata', 13.361389, 38.115556, 'Palermo')
    assert res == 1

    res = await redis.geoadd(
        'geodata',
        15.087269, 37.502669, 'Catania',
        12.424315, 37.802105, 'Marsala'
    )
    assert res == 2


@redis_version(
    3, 2, 0, reason='GEODIST is available since redis >= 3.2.0')
async def test_geodist(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.geodist('geodata', 'Palermo', 'Catania')
    assert res == 166274.1516

    res = await redis.geodist('geodata', 'Palermo', 'Catania', 'km')
    assert res == 166.2742


@redis_version(
    3, 2, 0, reason='GEOHASH is available since redis >= 3.2.0')
async def test_geohash(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.geohash(
        'geodata', 'Palermo', encoding='utf-8'
    )
    assert res == ['sqc8b49rny0']

    res = await redis.geohash(
        'geodata', 'Palermo', 'Catania', encoding='utf-8'
    )
    assert res == ['sqc8b49rny0', 'sqdtr74hyu0']


@redis_version(
    3, 2, 0, reason='GEOPOS is available since redis >= 3.2.0')
async def test_geopos(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.geopos('geodata', 'Palermo')
    assert res == [
        GeoPoint(longitude=13.36138933897018433, latitude=38.11555639549629859)
    ]

    res = await redis.geopos('geodata', 'Catania', 'Palermo')
    assert res == [
        GeoPoint(longitude=15.087267458438873, latitude=37.50266842333162),
        GeoPoint(longitude=13.36138933897018433, latitude=38.11555639549629859)
    ]


@redis_version(
    3, 2, 0, reason='GEO* is available since redis >= 3.2.0')
async def test_geo_not_exist_members(redis):
    res = await redis.geoadd('geodata', 13.361389, 38.115556, 'Palermo')
    assert res == 1

    res = await redis.geoadd(
        'geodata',
        15.087269, 37.502669, 'Catania',
        12.424315, 37.802105, 'Marsala'
    )
    assert res == 2

    res = await redis.geohash(
        'geodata', 'NotExistMember'
    )
    assert res == [None]

    res = await redis.geodist('geodata', 'NotExistMember', 'Catania')
    assert res is None

    res = await redis.geopos(
        'geodata', 'Palermo', 'NotExistMember', 'Catania'
    )
    assert res == [
        GeoPoint(
            longitude=13.36138933897018433,
            latitude=38.11555639549629859
        ),
        None,
        GeoPoint(longitude=15.087267458438873, latitude=37.50266842333162)
    ]


@redis_version(
    3, 2, 0, reason='GEORADIUS is available since redis >= 3.2.0')
async def test_georadius_validation(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo', 15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    with pytest.raises(TypeError):
        res = await redis.georadius(
            'geodata', 15, 37, 200, 'km', count=1.3, encoding='utf-8'
        )
    with pytest.raises(TypeError):
        res = await redis.georadius(
            'geodata', 15, 37, '200', 'km', encoding='utf-8'
        )
    with pytest.raises(ValueError):
        res = await redis.georadius(
            'geodata', 15, 37, 200, 'k', encoding='utf-8'
        )
    with pytest.raises(ValueError):
        res = await redis.georadius(
            'geodata', 15, 37, 200, 'km', sort='DESV', encoding='utf-8'
        )


@redis_version(
    3, 2, 0, reason='GEORADIUS is available since redis >= 3.2.0')
async def test_georadius(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo', 15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', encoding='utf-8'
    )
    assert res == ['Palermo', 'Catania']

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', count=1, encoding='utf-8'
    )
    assert res == ['Catania']

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', sort='ASC', encoding='utf-8'
    )
    assert res == ['Catania', 'Palermo']

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', with_dist=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(member='Palermo', dist=190.4424, coord=None, hash=None),
        GeoMember(member='Catania', dist=56.4413, coord=None, hash=None)
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_dist=True, with_coord=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=190.4424, hash=None,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=56.4413, hash=None,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_dist=True, with_coord=True, with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=190.4424, hash=3479099956230698,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=56.4413, hash=3479447370796909,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_coord=True, with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=None, hash=3479099956230698,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=None, hash=3479447370796909,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', with_coord=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=None, hash=None,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member='Catania', dist=None, hash=None,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', count=1, sort='DESC',
        with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=None, hash=3479099956230698, coord=None
        )
    ]


@redis_version(
    3, 2, 0, reason='GEORADIUSBYMEMBER is available since redis >= 3.2.0')
async def test_georadiusbymember(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km', with_dist=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(member='Palermo', dist=0.0, coord=None, hash=None),
        GeoMember(member='Catania', dist=166.2742, coord=None, hash=None)
    ]
    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km', encoding='utf-8'
    )
    assert res == ['Palermo', 'Catania']

    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km',
        with_dist=True, with_coord=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=0.0, hash=None,
            coord=GeoPoint(13.361389338970184, 38.1155563954963)
        ),
        GeoMember(
            member='Catania', dist=166.2742, hash=None,
            coord=GeoPoint(15.087267458438873, 37.50266842333162)
        )
    ]

    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km',
        with_dist=True, with_coord=True, with_hash=True, encoding='utf-8'
    )
    assert res == [
        GeoMember(
            member='Palermo', dist=0.0, hash=3479099956230698,
            coord=GeoPoint(13.361389338970184, 38.1155563954963)
        ),
        GeoMember(
            member='Catania', dist=166.2742, hash=3479447370796909,
            coord=GeoPoint(15.087267458438873, 37.50266842333162)
        )
    ]


@redis_version(
    3, 2, 0, reason='GEOHASH is available since redis >= 3.2.0')
async def test_geohash_binary(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.geohash(
        'geodata', 'Palermo'
    )
    assert res == [b'sqc8b49rny0']

    res = await redis.geohash(
        'geodata', 'Palermo', 'Catania'
    )
    assert res == [b'sqc8b49rny0', b'sqdtr74hyu0']


@redis_version(
    3, 2, 0, reason='GEORADIUS is available since redis >= 3.2.0')
async def test_georadius_binary(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo', 15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km'
    )
    assert res == [b'Palermo', b'Catania']

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', count=1
    )
    assert res == [b'Catania']

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', sort='ASC'
    )
    assert res == [b'Catania', b'Palermo']

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', with_dist=True
    )
    assert res == [
        GeoMember(member=b'Palermo', dist=190.4424, coord=None, hash=None),
        GeoMember(member=b'Catania', dist=56.4413, coord=None, hash=None)
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_dist=True, with_coord=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=190.4424, hash=None,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member=b'Catania', dist=56.4413, hash=None,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_dist=True, with_coord=True, with_hash=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=190.4424, hash=3479099956230698,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member=b'Catania', dist=56.4413, hash=3479447370796909,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km',
        with_coord=True, with_hash=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=None, hash=3479099956230698,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member=b'Catania', dist=None, hash=3479447370796909,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', with_coord=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=None, hash=None,
            coord=GeoPoint(
                longitude=13.36138933897018433, latitude=38.11555639549629859
            )
        ),
        GeoMember(
            member=b'Catania', dist=None, hash=None,
            coord=GeoPoint(
                longitude=15.087267458438873, latitude=37.50266842333162
            ),
        )
    ]

    res = await redis.georadius(
        'geodata', 15, 37, 200, 'km', count=1, sort='DESC',
        with_hash=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=None, hash=3479099956230698, coord=None
        )
    ]


@redis_version(
    3, 2, 0, reason='GEORADIUSBYMEMBER is available since redis >= 3.2.0')
async def test_georadiusbymember_binary(redis):
    res = await redis.geoadd(
        'geodata',
        13.361389, 38.115556, 'Palermo',
        15.087269, 37.502669, 'Catania'
    )
    assert res == 2

    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km', with_dist=True
    )
    assert res == [
        GeoMember(member=b'Palermo', dist=0.0, coord=None, hash=None),
        GeoMember(member=b'Catania', dist=166.2742, coord=None, hash=None)
    ]

    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km',
        with_dist=True, with_coord=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=0.0, hash=None,
            coord=GeoPoint(13.361389338970184, 38.1155563954963)
        ),
        GeoMember(
            member=b'Catania', dist=166.2742, hash=None,
            coord=GeoPoint(15.087267458438873, 37.50266842333162)
        )
    ]

    res = await redis.georadiusbymember(
        'geodata', 'Palermo', 200, 'km',
        with_dist=True, with_coord=True, with_hash=True
    )
    assert res == [
        GeoMember(
            member=b'Palermo', dist=0.0, hash=3479099956230698,
            coord=GeoPoint(13.361389338970184, 38.1155563954963)
        ),
        GeoMember(
            member=b'Catania', dist=166.2742, hash=3479447370796909,
            coord=GeoPoint(15.087267458438873, 37.50266842333162)
        )
    ]
