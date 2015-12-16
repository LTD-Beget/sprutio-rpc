import redis
from config import redis as redis_conf


class RedisConnection(object):
    _redis = {}

    _host = redis_conf.REDIS_DEFAULT_HOST
    _port = redis_conf.REDIS_DEFAULT_PORT

    instance = None

    def __new__(cls):
        if not cls.instance:
            cls.instance = super(RedisConnection, cls).__new__(cls)

        return cls.instance

    def get(self, db=0):
        """
        :param db:
        :rtype: redis.StrictRedis
        """
        try:
            if db not in self._redis or not self._redis[db].ping():
                self._redis[db] = redis.StrictRedis(host=self._host, port=self._port, db=db)
        except redis.ConnectionError:
            self._redis[db] = redis.StrictRedis(host=self._host, port=self._port, db=db)
        return self._redis[db]
