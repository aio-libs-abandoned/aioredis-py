import os
import sys
import logging


logger = logging.getLogger('aioredis')

if os.environ.get("AIOREDIS_DEBUG"):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(stream=sys.stderr))
    os.environ["AIOREDIS_DEBUG"] = ""
