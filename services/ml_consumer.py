import json
from common.config import load_config
from common.redisq import make_redis, ensure_group

CFG = load_config()
R = make_redis(CFG.queue.host, CFG.queue.port)
TSTREAM = CFG.queue.stream_tasks
GROUP = CFG.queue.group_tasks

ensure_group(R, TSTREAM, GROUP, start_id="0")

def process_window(fields):
    sid = fields["session_id"]
    rgb = json.loads(fields["rgb"])
    ir1 = json.loads(fields["ir1"])
    ir2 = json.loads(fields["ir2"])
    # TODO: run your ML here using the segment paths in rgb/ir1/ir2
    return {"session_id": sid, "segments": len(rgb) + len(ir1) + len(ir2)}

def loop():
    consumer = "ml-1"
    while True:
        resp = R.xreadgroup(GROUP, consumer, {TSTREAM: ">"}, count=1, block=5000)
        if not resp:
            continue
        _, entries = resp[0]
        for mid, f in entries:
            _ = process_window(f)
            R.xack(TSTREAM, GROUP, mid)

if __name__ == "__main__":
    loop()
