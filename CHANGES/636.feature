Added an unwatch flag to redis.multi_exec() that pipelines an UNWATCH command to
the beginning of the MULTI/EXEC.
