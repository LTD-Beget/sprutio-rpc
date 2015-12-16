TYPE_FCGI = 'fcgi'
TYPE_MSGPACK = 'msgpack'

MSGPACK_CONTROLLERS = 'controllers'

servers = dict(
    # используется по умолнчаению
    default=dict(
        type=TYPE_MSGPACK,
        host="127.0.0.1",
        port=8500
    )
)
