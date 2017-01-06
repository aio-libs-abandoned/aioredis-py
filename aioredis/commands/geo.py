from collections import namedtuple

from aioredis.util import wait_convert, _NOTSET


GeoPoint = namedtuple('GeoPoint', ('longitude', 'latitude'))
GeoMember = namedtuple('GeoMember', ('member', 'dist', 'hash', 'coord'))


class GeoCommandsMixin:
    """Geo commands mixin.

    For commands details see: http://redis.io/commands#geo
    """

    def geoadd(self, key, longitude, latitude, member, *args, **kwargs):
        """Add one or more geospatial items in the geospatial index represented
        using a sorted set.
        """
        return self._conn.execute(
            b'GEOADD', key, longitude, latitude, member, *args, **kwargs
        )

    def geohash(self, key, member, *args, **kwargs):
        """Returns members of a geospatial index as standard geohash strings.
        """
        return self._conn.execute(
            b'GEOHASH', key, member, *args, **kwargs
        )

    def geopos(self, key, member, *args, **kwargs):
        """Returns longitude and latitude of members of a geospatial index.

        :rtype: list[GeoPoint or None]
        """
        fut = self._conn.execute(b'GEOPOS', key, member, *args, **kwargs)
        return wait_convert(fut, make_geopos)

    def geodist(self, key, member1, member2, unit='m'):
        """Returns the distance between two members of a geospatial index.

        :rtype: list[float or None]
        """
        fut = self._conn.execute(b'GEODIST', key, member1, member2, unit)
        return wait_convert(fut, make_geodist)

    def georadius(self, key, longitude, latitude, radius, unit='m', *,
                  with_dist=False, with_hash=False, with_coord=False,
                  count=None, sort=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a point.

        Return value follows Redis convention:

        * if none of ``WITH*`` flags are set -- list of strings returned:

            >>> await redis.georadius('Sicily', 15, 37, 200, 'km')
            [b"Palermo", b"Catania"]

        * if any flag (or all) is set -- list of named tuples returned:

            >>> await redis.georadius('Sicily', 15, 37, 200, 'km',
            ...                       with_dist=True)
            [GeoMember(name=b"Palermo", dist=190.4424, hash=None, coord=None),
             GeoMember(name=b"Catania", dist=56.4413, hash=None, coord=None)]

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not int
        :raises ValueError: if unit not equal ``m``, ``km``, ``mi`` or ``ft``
        :raises ValueError: if sort not equal ``ASC`` or ``DESC``

        :rtype: list[str] or list[GeoMember]
        """
        args = validate_georadius_options(
            radius, unit, with_dist, with_hash, with_coord, count, sort
        )

        fut = self._conn.execute(
            b'GEORADIUS', key, longitude, latitude, radius,
            unit, *args, encoding=encoding
        )
        if with_dist or with_hash or with_coord:
            return wait_convert(fut, make_geomember,
                                with_dist=with_dist,
                                with_hash=with_hash,
                                with_coord=with_coord)
        return fut

    def georadiusbymember(self, key, member, radius, unit='m', *,
                          with_dist=False, with_hash=False, with_coord=False,
                          count=None, sort=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a member.

        Return value follows Redis convention:

        * if none of ``WITH*`` flags are set -- list of strings returned:

            >>> await redis.georadiusbymember('Sicily', 'Palermo', 200, 'km')
            [b"Palermo", b"Catania"]

        * if any flag (or all) is set -- list of named tuples returned:

            >>> await redis.georadiusbymember('Sicily', 'Palermo', 200, 'km',
            ...                               with_dist=True)
            [GeoMember(name=b"Palermo", dist=190.4424, hash=None, coord=None),
             GeoMember(name=b"Catania", dist=56.4413, hash=None, coord=None)]

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not int
        :raises ValueError: if unit not equal ``m``, ``km``, ``mi`` or ``ft``
        :raises ValueError: if sort not equal ``ASC`` or ``DESC``

        :rtype: list[str] or list[GeoMember]
        """
        args = validate_georadius_options(
            radius, unit, with_dist, with_hash, with_coord, count, sort
        )

        fut = self._conn.execute(
            b'GEORADIUSBYMEMBER', key, member, radius,
            unit, *args, encoding=encoding)
        if with_dist or with_hash or with_coord:
            return wait_convert(fut, make_geomember,
                                with_dist=with_dist,
                                with_hash=with_hash,
                                with_coord=with_coord)
        return fut


def validate_georadius_options(radius, unit, with_dist, with_hash, with_coord,
                               count, sort):
    args = []

    if with_dist:
        args.append(b'WITHDIST')
    if with_hash:
        args.append(b'WITHHASH')
    if with_coord:
        args.append(b'WITHCOORD')

    if unit not in ['m', 'km', 'mi', 'ft']:
        raise ValueError("unit argument must be 'm', 'km', 'mi' or 'ft'")
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


def make_geocoord(value):
    if isinstance(value, list):
        return GeoPoint(*map(float, value))
    return value


def make_geodist(value):
    if value:
        return float(value)
    return value


def make_geopos(value):
    return [make_geocoord(val) for val in value]


def make_geomember(value, with_dist, with_coord, with_hash):
    res_rows = []

    for row in value:
        name = row.pop(0)
        dist = hash_ = coord = None
        if with_dist:
            dist = float(row.pop(0))
        if with_hash:
            hash_ = int(row.pop(0))
        if with_coord:
            coord = GeoPoint(*map(float, row.pop(0)))

        res_rows.append(GeoMember(name, dist, hash_, coord))

    return res_rows
