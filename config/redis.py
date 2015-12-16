import os

# Redis settings
REDIS_DEFAULT_HOST = os.getenv("FM_REDIS_HOST", 'localhost')
REDIS_DEFAULT_PORT = os.getenv("FM_REDIS_PORT", 6379)
REDIS_DEFAULT_EXPIRE = os.getenv("FM_REDIS_EXPIRE", 86400)
