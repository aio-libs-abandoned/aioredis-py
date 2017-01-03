from aioredis.util import _NOTSET, wait_ok


class GeoCommandsMixin:
    """Geo commands mixin.

    For commands details see: http://redis.io/commands#geo
    """

    def geoadd(self, key, longitude, latitude, member):
        """Add one or more geospatial items in the geospatial index represented 
        using a sorted set
        """
        return self._conn.execute(b'GEOADD', key, longitude, latitude, member)

    def geodist(self, key, member1, member2, unit='m'):
        """Returns the distance between two members of a geospatial index
        """
        return self._conn.execute(b'GEODIST', key, member1, member2, unit)

    def geohash(self, key, member):
        """Returns members of a geospatial index as standard geohash strings
        """
        return self._conn.execute(b'GEOHASH', key, member)

    def geopos(self, key, member):
        """Returns longitude and latitude of members of a geospatial index
        """
        return self._conn.execute(b'GEOPOS', key, member)

    def georadius(self, key, longitude, latitude, radius, unit='m',
                  with_coord=False, with_dist=False, with_hash=False,
                  count=None, sort=None):
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

        return self._conn.execute(b'GEORADIUS', key, longitude, latitude, radius, unit, *args)

    def georadiusbymember(self, key, member, radius, unit='m',
                          with_coord=False, with_dist=False, with_hash=False,
                          count=None, sort=None):
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

        return self._conn.execute(b'GEORADIUSBYMEMBER', key, member, radius, unit, *args)
