from aioredis.util import wait_convert, wait_convert_with_opts, _NOTSET


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
        encoding = _NOTSET
        if 'encoding' in kwargs:
            encoding = kwargs.pop('encoding')

        return self._conn.execute(
            b'GEOHASH', key, member, encoding=encoding, *args, **kwargs
        )

    def geopos(self, key, member, *args, **kwargs):
        """Returns longitude and latitude of members of a geospatial index
        """
        fut = self._conn.execute(b'GEOPOS', key, member, *args, **kwargs)
        return wait_convert(fut, pairs_float)

    def geodist(self, key, member1, member2, unit='m'):
        """Returns the distance between two members of a geospatial index
        """
        fut = self._conn.execute(b'GEODIST', key, member1, member2, unit)
        return wait_convert(fut, float)

    def georadius(self, key, longitude, latitude, radius, unit='m',
                  with_coord=False, with_dist=False, with_hash=False,
                  count=None, sort=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a point

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not float or int
        :raises ValueError: if sort not equal ASC or DESC
        """
        args = []

        if with_coord:
            args.append('WITHCOORD')
        if with_dist:
            args.append('WITHDIST')
        if with_hash:
            args.append('WITHHASH')

        if not isinstance(radius, (int, float)):
            raise TypeError("radius argument must be int or float")
        if count:
            if not isinstance(count, int):
                raise TypeError("count argument must be int")
            args.append(count)
        if sort:
            if sort in ['ASC', 'DESC']:
                raise TypeError("sort argument must be euqal ASC or DESC")
            args.append(sort)

        fut = self._conn.execute(
            b'GEORADIUS', key, longitude, latitude, radius,
            unit, *args, encoding=encoding
        )
        return wait_convert_with_opts(
            fut, geo_data_row,
            with_coord=with_coord, with_dist=with_dist, with_hash=with_hash
        )

    def georadiusbymember(self, key, member, radius, unit='m',
                          with_coord=False, with_dist=False, with_hash=False,
                          count=None, sort=None, encoding=_NOTSET):
        """Query a sorted set representing a geospatial index to fetch members
        matching a given maximum distance from a member

        :raises TypeError: radius is not float or int
        :raises TypeError: count is not float or int
        :raises ValueError: if sort not equal ASC or DESC
        """
        args = []

        if with_coord:
            args.append('WITHCOORD')
        if with_dist:
            args.append('WITHDIST')
        if with_hash:
            args.append('WITHHASH')

        if not isinstance(radius, (int, float)):
            raise TypeError("radius argument must be int or float")
        if count:
            if not isinstance(count, int):
                raise TypeError("count argument must be int")
            args.append(count)
        if sort:
            if sort in ['ASC', 'DESC']:
                raise TypeError("sort argument must be euqal ASC or DESC")
            args.append(sort)

        fut = self._conn.execute(
            b'GEORADIUSBYMEMBER', key, member, radius,
            unit, *args, encoding=encoding
        )
        return wait_convert_with_opts(
            fut, geo_data_row,
            with_coord=with_coord, with_dist=with_dist, with_hash=with_hash
        )


def pairs_float(value):
    return [[float(val[0]), float(val[1])] for val in value]


def geo_data_row(value, with_dist, with_coord, with_hash):
    res_rows = []
    for row in value:
        res = []

        res.append(row[0])
        if with_dist and with_coord and with_hash:
            res.append(float(row[1]))
            res.append(int(row[2]))

            res.append([float(row[3][0]), float(row[3][1])])
        elif with_dist and with_coord:
            res.append(float(row[1]))
            res.append([float(row[2][0]), float(row[2][1])])
        elif with_dist and with_hash:
            res.append(float(row[1]))
            res.append(int(row[2]))
        elif with_hash and with_coord:
            res.append(int(row[1]))
            res.append([float(row[2][0]), float(row[2][1])])
        elif with_dist:
            res.append(float(row[1]))
        elif with_hash:
            res.append(int(row[1]))
        elif with_coord:
            res.append([float(row[1][0]), float(row[1][1])])

        res_rows.append(res)

    return res_rows
