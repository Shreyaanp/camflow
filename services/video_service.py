import os, time, pathlib, subprocess, signal, threading
from common.config import load_config
from common.redisq import make_redis, xadd_trim, ensure_group
import pyrealsense2 as rs

CFG = load_config()
R = make_redis(CFG.queue.host, CFG.queue.port)
FSTREAM = CFG.queue.stream_fragments
CSTREAM = CFG.queue.stream_controls
CTRL_GROUP = CFG.queue.group_controls

ROOT = CFG.paths.fragments_root
ARCH = CFG.paths.archive_root
os.makedirs(ROOT, exist_ok=True)
os.makedirs(ARCH, exist_ok=True)

def ffmpeg_hls(outdir, w, h, fps, pix_fmt, bitrate_kbps, frag_sec):
    plist = os.path.join(outdir, "index.m3u8")
    cmd = [
        "ffmpeg","-nostdin","-hide_banner","-loglevel","error",
        "-f","rawvideo","-pix_fmt",pix_fmt,"-s",f"{w}x{h}","-r",str(fps),"-i","-",
        "-an","-c:v","h264_v4l2m2m","-b:v",f"{bitrate_kbps}k","-g",str(int(fps*2)),
        "-f","hls","-hls_time",str(frag_sec)," -hls_list_size","0","-hls_flags","independent_segments",
        plist
    ]
    # fix: remove stray space in "-hls_list_size"
    cmd[14] = "-hls_list_size"
    return subprocess.Popen(cmd, stdin=subprocess.PIPE)

def watcher_enqueue(outdir, sid, stream, stop_evt):
    seen = set()
    p = pathlib.Path(outdir)
    while not stop_evt.is_set():
        for seg in sorted(p.glob("*.ts")):
            if seg.name in seen:
                continue
            seen.add(seg.name)
            ts_ms = int(seg.stat().st_mtime * 1000)
            xadd_trim(R, FSTREAM, {
                "session_id": sid, "stream": stream, "ts": str(ts_ms), "path": str(seg)
            }, maxlen=5000)
        time.sleep(0.1)

def finalize_archive(sid, stream_dir, stream_name):
    m3u8 = os.path.join(stream_dir, "index.m3u8")
    if not os.path.exists(m3u8):
        return
    out_mp4 = os.path.join(ARCH, f"{sid}_{stream_name}.mp4")
    subprocess.run([
        "ffmpeg","-y","-hide_banner","-loglevel","error",
        "-protocol_whitelist","file,crypto,udp,rtp,tcp,https,tls,rtmp",
        "-i", m3u8, "-c","copy", out_mp4
    ], check=False)

def run_session(sid, stop_evt):
    base = os.path.join(ROOT, sid)
    rgb_dir = os.path.join(base, "rgb")
    ir1_dir = os.path.join(base, "ir1")
    ir2_dir = os.path.join(base, "ir2")
    for d in (rgb_dir, ir1_dir, ir2_dir): os.makedirs(d, exist_ok=True)

    enc_rgb = ffmpeg_hls(rgb_dir, CFG.capture.width, CFG.capture.height, CFG.capture.fps, "bgr24", CFG.capture.bitrate_kbps, CFG.capture.fragment_sec)
    enc_ir1 = ffmpeg_hls(ir1_dir, CFG.capture.width, CFG.capture.height, CFG.capture.fps, "gray8", CFG.capture.bitrate_kbps, CFG.capture.fragment_sec)
    enc_ir2 = ffmpeg_hls(ir2_dir, CFG.capture.width, CFG.capture.height, CFG.capture.fps, "gray8", CFG.capture.bitrate_kbps, CFG.capture.fragment_sec)

    threading.Thread(target=watcher_enqueue, args=(rgb_dir, sid, "rgb", stop_evt), daemon=True).start()
    threading.Thread(target=watcher_enqueue, args=(ir1_dir, sid, "ir1", stop_evt), daemon=True).start()
    threading.Thread(target=watcher_enqueue, args=(ir2_dir, sid, "ir2", stop_evt), daemon=True).start()

    pipe = rs.pipeline()
    cfg = rs.config()
    cfg.enable_stream(rs.stream.color, CFG.capture.width, CFG.capture.height, rs.format.bgr8, CFG.capture.fps)
    cfg.enable_stream(rs.stream.infrared, 1, CFG.capture.width, CFG.capture.height, rs.format.y8, CFG.capture.fps)
    cfg.enable_stream(rs.stream.infrared, 2, CFG.capture.width, CFG.capture.height, rs.format.y8, CFG.capture.fps)
    pipe.start(cfg)

    # warm-up
    for _ in range(10):
        if stop_evt.is_set(): break
        pipe.wait_for_frames()

    try:
        while not stop_evt.is_set():
            frames = pipe.wait_for_frames()
            c = frames.get_color_frame()
            i1 = frames.get_infrared_frame(1)
            i2 = frames.get_infrared_frame(2)
            if not c or not i1 or not i2:
                continue
            enc_rgb.stdin.write(bytes(c.get_data()))
            enc_ir1.stdin.write(bytes(i1.get_data()))
            enc_ir2.stdin.write(bytes(i2.get_data()))
    finally:
        pipe.stop()
        for p in (enc_rgb, enc_ir1, enc_ir2):
            try: p.stdin.close()
            except: pass
            try: p.send_signal(signal.SIGINT)
            except: pass
            try: p.wait(timeout=5)
            except: pass
        # archive
        finalize_archive(sid, rgb_dir, "rgb")
        finalize_archive(sid, ir1_dir, "ir1")
        finalize_archive(sid, ir2_dir, "ir2")

def control_loop():
    ensure_group(R, CSTREAM, CTRL_GROUP, start_id="$")
    consumer = "video-1"
    active = {}  # sid -> stop_event

    while True:
        resp = R.xreadgroup(CTRL_GROUP, consumer, {CSTREAM: ">"}, count=10, block=2000)
        if not resp:
            continue
        _, entries = resp[0]
        for mid, fields in entries:
            typ = fields.get("type")
            sid = fields.get("session_id")
            if typ == "start":
                if sid not in active:
                    stop_evt = threading.Event()
                    active[sid] = stop_evt
                    th = threading.Thread(target=run_session, args=(sid, stop_evt), daemon=True)
                    th.start()
            elif typ == "stop":
                ev = active.pop(sid, None)
                if ev:
                    ev.set()
            R.xack(CSTREAM, CTRL_GROUP, mid)

if __name__ == "__main__":
    control_loop()
