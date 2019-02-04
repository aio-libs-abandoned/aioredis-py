import aioredis
from aiohttp import web


async def setup_redis(app):
    pool = await aioredis.create_redis_pool(
        ('localhost', 6379),
        encoding='utf-8'
    )

    async def close_redis(app):
        pool.close()
        await pool.wait_closed()

    app.on_cleanup.append(close_redis)
    app['redis_pool'] = pool
    return pool


async def index(request):
    conn = request.app['redis_pool']
    val = await conn.ping()
    return web.Response(text=f"Redis response: {val}")


async def init_app():
    app = web.Application()
    app.add_routes([web.get('/', index)])

    await setup_redis(app)

    return app


def main():
    app = init_app()
    web.run_app(app)


if __name__ == '__main__':
    main()
