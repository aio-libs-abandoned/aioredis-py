from collections import namedtuple

from aioredis.util import wait_convert, _NOTSET


GeoCoord = namedtuple('GeoCoord', ('longitude', 'latitude'))
GeoMember = namedtuple('GeoRadius', ('member', 'dist', 'hash', 'coord'))


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
                  with_coord=False, with_dist=False, with_hash=False,
                  count=None, sort_dir=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a point

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not float or int
        :raises ValueError: if unit not equal 'm' or 'km' or 'mi' or 'ft'
        :raises ValueError: if sort not equal 'ASC' or 'DESC'
        """
        args = validate_georadius_options(
            radius, unit, count, sort_dir, with_coord, with_dist, with_hash
        )

        fut = self._conn.execute(
            b'GEORADIUS', key, longitude, latitude, radius,
            unit, *args, encoding=encoding
        )
        return wait_convert(
            fut, process_geomember,
            with_coord=with_coord, with_dist=with_dist, with_hash=with_hash
        )

    def georadiusbymember(self, key, member, radius, unit='m',
                          with_coord=False, with_dist=False, with_hash=False,
                          count=None, sort_dir=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a member

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not float or int
        :raises ValueError: if unit not equal 'm' or 'km' or 'mi' or 'ft'
        :raises ValueError: if sort not equal 'ASC' or 'DESC'
        """
        args = validate_georadius_options(
            radius, unit, count, sort_dir, with_coord, with_dist, with_hash
        )

        fut = self._conn.execute(
            b'GEORADIUSBYMEMBER', key, member, radius,
            unit, *args, encoding=encoding
        )
        return wait_convert(
            fut, process_geomember,
            with_coord=with_coord, with_dist=with_dist, with_hash=with_hash
        )


def validate_georadius_options(radius, unit, count, sort_dir,
                               with_coord, with_dist, with_hash):
    args = []

    if with_coord:
        args.append(b'WITHCOORD')
    if with_dist:
        args.append(b'WITHDIST')
    if with_hash:
        args.append(b'WITHHASH')

    if unit not in ['m', 'km', 'mi', 'ft']:
        raise ValueError("unit argument must be 'm' or 'km' or 'mi' or 'ft'")
    if not isinstance(radius, (int, float)):
        raise TypeError("radius argument must be int or float")
    if count:
        if not isinstance(count, int):
            raise TypeError("count argument must be int")
        args += [b'COUNT', count]
    if sort_dir:
        if sort_dir not in ['ASC', 'DESC']:
            raise ValueError("sort_dir argument must be euqal 'ASC' or 'DESC'")
        args.append(sort_dir)
    return args


def make_geo_coord(value):
    return GeoCoord(*map(float, value))


def make_geopos(value):
    return [make_geo_coord(val) for val in value]


def make_geomember(member, distance, hash_, coord):
    if distance is not None:
        distance = float(distance)
    if hash_ is not None:
        hash_ = int(hash_)
    if coord is not None:
        coord = GeoCoord(*map(float, coord))

    return GeoMember(member, distance, hash_, coord)


def process_geomember(value, with_dist, with_coord, with_hash):
    res_rows = []
    for row in value:
        member, distance, coord, hash_ = None, None, None, None

        if isinstance(row, list):
            member = row[0]

            if with_dist and with_coord and with_hash:
                distance, hash_, coord = row[1], row[2], row[3]
            elif with_dist and with_coord:
                distance, coord = row[1], row[2]
            elif with_hash and with_coord:
                hash_, coord = row[1], row[2]
            elif with_dist and with_hash:
                distance, hash_ = row[1], row[2]
            elif with_dist:
                distance = row[1]
            elif with_hash:
                hash_ = row[1]
            elif with_coord:
                coord = row[1]
        else:
            member = row

        res_rows.append(make_geomember(member, distance, hash_, coord))

    return res_rows
