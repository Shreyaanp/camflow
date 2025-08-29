import redis

def make_redis(host, port):
    return redis.Redis(host=host, port=port, decode_responses=True)

def xadd_trim(r, stream, fields, maxlen=5000):
    return r.xadd(stream, fields, maxlen=maxlen, approximate=True)

def ensure_group(r, stream, group, start_id="0"):
    try:
        r.xgroup_create(stream, group, id=start_id, mkstream=True)
    except redis.ResponseError:
        pass
