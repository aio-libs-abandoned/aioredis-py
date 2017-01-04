from collections import namedtuple

from aioredis.util import wait_convert, _NOTSET


GeoCoord = namedtuple('GeoCoord', ('longitude', 'latitude'))
GeoMember = namedtuple('GeoMember', ('member', 'dist', 'hash', 'coord'))


class GeoCommandsMixin:
    """Geo commands mixin.

    For commands details see: http://redis.io/commands#geo
    """

    def geoadd(self, key, longitude, latitude, member, *args, **kwargs):
        """Add one or more geospatial items in the geospatial index represented
        using a sorted set
        """
        return self._conn.execute(
            b'GEOADD', key, longitude, latitude, member, *args, **kwargs
        )

    def geohash(self, key, member, *args, **kwargs):
        """Returns members of a geospatial index as standard geohash strings
        """
        return self._conn.execute(
            b'GEOHASH', key, member, *args, **kwargs
        )

    def geopos(self, key, member, *args, **kwargs):
        """Returns longitude and latitude of members of a geospatial index
        """
        fut = self._conn.execute(b'GEOPOS', key, member, *args, **kwargs)
        return wait_convert(fut, make_geopos)

    def geodist(self, key, member1, member2, unit='m'):
        """Returns the distance between two members of a geospatial index
        """
        fut = self._conn.execute(b'GEODIST', key, member1, member2, unit)
        return wait_convert(fut, float)

    def georadius(self, key, longitude, latitude, radius, unit='m',
                  dist=False, hash=False, coord=False, 
                  count=None, sort=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a point

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not float or int
        :raises ValueError: if unit not equal 'm' or 'km' or 'mi' or 'ft'
        :raises ValueError: if sort not equal 'ASC' or 'DESC'
        """
        args = validate_georadius_options(
            radius, unit, coord, dist, hash, count, sort
        )

        fut = self._conn.execute(
            b'GEORADIUS', key, longitude, latitude, radius,
            unit, *args, encoding=encoding
        )
        return wait_convert(
            fut, make_geomember,
            with_dist=dist, with_hash=hash, with_coord=coord
        )

    def georadiusbymember(self, key, member, radius, unit='m',
                          dist=False, hash=False, coord=False, 
                          count=None, sort=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a member

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not float or int
        :raises ValueError: if unit not equal 'm' or 'km' or 'mi' or 'ft'
        :raises ValueError: if sort not equal 'ASC' or 'DESC'
        """
        args = validate_georadius_options(
            radius, unit, coord, dist, hash, count, sort
        )

        fut = self._conn.execute(
            b'GEORADIUSBYMEMBER', key, member, radius,
            unit, *args, encoding=encoding
        )
        return wait_convert(
            fut, make_geomember,
            with_dist=dist, with_hash=hash, with_coord=coord
        )


def validate_georadius_options(radius, unit, coord, dist, hash, count, sort):
    args = []

    if coord:
        args.append(b'WITHCOORD')
    if dist:
        args.append(b'WITHDIST')
    if hash:
        args.append(b'WITHHASH')

    if unit not in ['m', 'km', 'mi', 'ft']:
        raise ValueError("unit argument must be 'm' or 'km' or 'mi' or 'ft'")
    if not isinstance(radius, (int, float)):
        raise TypeError("radius argument must be int or float")
    if count:
        if not isinstance(count, int):
            raise TypeError("count argument must be int")
        args += [b'COUNT', count]
    if sort:
        if sort not in ['ASC', 'DESC']:
            raise ValueError("sort argument must be euqal 'ASC' or 'DESC'")
        args.append(sort)
    return args


def make_geo_coord(value):
    return GeoCoord(*map(float, value))


def make_geopos(value):
    return [make_geo_coord(val) for val in value]


def make_geomember(value, with_dist, with_coord, with_hash):
    res_rows = []

    for row in value:
        name = row
        dist = hash = coord = None

        if isinstance(row, list):
            name = row.pop(0)
            if with_dist:
                dist = float(row.pop(0))
            if with_hash:
                hash = int(row.pop(0))
            if with_coord:
                coord = GeoCoord(*map(float, row.pop(0)))

        res_rows.append(GeoMember(name, dist, hash, coord))

    return res_rows
