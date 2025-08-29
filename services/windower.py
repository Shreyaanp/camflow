import time, collections, json
from common.config import load_config
from common.redisq import make_redis, ensure_group, xadd_trim

CFG = load_config()
R = make_redis(CFG.queue.host, CFG.queue.port)
FSTREAM = CFG.queue.stream_fragments
TSTREAM = CFG.queue.stream_tasks
GROUP = CFG.queue.group_fragments

ensure_group(R, FSTREAM, GROUP, start_id="0")
WIN = CFG.capture.analysis_window_sec * 1000
FRAG_MS = CFG.capture.fragment_sec * 1000
MIN_SEG = max(1, WIN // FRAG_MS)

buf = {}  # sid -> {rgb: deque[(ts,path)], ir1: deque[], ir2: deque[]}

def push_buf(sid, s, ts, path):
    d = buf.setdefault(sid, {"rgb": collections.deque(), "ir1": collections.deque(), "ir2": collections.deque()})
    dq = d[s]
    dq.append((ts, path))
    tmin = ts - WIN
    while dq and dq[0][0] < tmin:
        dq.popleft()

def try_emit(sid):
    d = buf.get(sid)
    if not d or not all(d[k] for k in ("rgb", "ir1", "ir2")):
        return
    tmax = min(d["rgb"][-1][0], d["ir1"][-1][0], d["ir2"][-1][0])
    lo = tmax - WIN
    def sel(dq): return [p for (ts, p) in dq if lo <= ts <= tmax]
    rgb = sel(d["rgb"]); ir1 = sel(d["ir1"]); ir2 = sel(d["ir2"])
    if min(len(rgb), len(ir1), len(ir2)) >= MIN_SEG:
        xadd_trim(R, TSTREAM, {
            "session_id": sid, "t0": str(lo), "t1": str(tmax),
            "rgb": json.dumps(rgb), "ir1": json.dumps(ir1), "ir2": json.dumps(ir2)
        }, maxlen=2000)

def loop():
    consumer = "windower-1"
    while True:
        resp = R.xreadgroup(GROUP, consumer, {FSTREAM: ">"}, count=10, block=2000)
        if not resp:
            continue
        _, entries = resp[0]
        ack_ids = []
        for mid, f in entries:
            sid = f["session_id"]; s = f["stream"]; ts = int(f["ts"]); path = f["path"]
            push_buf(sid, s, ts, path)
            try_emit(sid)
            ack_ids.append(mid)
        if ack_ids:
            R.xack(FSTREAM, GROUP, *ack_ids)

if __name__ == "__main__":
    loop()
