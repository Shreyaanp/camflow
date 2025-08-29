import time, uuid
from common.config import load_config
from common.redisq import make_redis, xadd_trim
from adafruit_extended_bus import ExtendedI2C as I2C
import adafruit_vl53l0x

CFG = load_config()
R = make_redis(CFG.queue.host, CFG.queue.port)
CSTREAM = CFG.queue.stream_controls

def now_ms(): return int(time.time() * 1000)
def new_sid(): return time.strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]

class ToFFSM:
    def __init__(self):
        self.state = "IDLE"
        self.sid = None
        self.t0 = 0.0
        self.last_det = 0.0
        self.quiet_until = 0.0

    def emit(self, typ, sid, reason=None):
        msg = {"type": typ, "session_id": sid, "ts": str(now_ms())}
        if reason: msg["reason"] = reason
        xadd_trim(R, CSTREAM, msg, maxlen=2000)

    def tick(self, dist_mm):
        t = time.time()
        if self.state == "IDLE":
            if dist_mm <= CFG.sensor.threshold_mm and t >= self.quiet_until:
                time.sleep(CFG.sensor.arm_delay_ms / 1000.0)
                if dist_mm <= CFG.sensor.threshold_mm:
                    self.sid = new_sid()
                    self.t0 = t
                    self.last_det = t
                    self.emit("start", self.sid)
                    self.state = "ACTIVE"
        elif self.state == "ACTIVE":
            if dist_mm <= CFG.sensor.threshold_mm:
                self.last_det = t
            maxd = CFG.capture.max_session_min * 60.0 if CFG.capture.max_session_min else 0.0
            stop_inact = (t - self.last_det) >= (CFG.sensor.inactivity_timeout_ms / 1000.0)
            stop_max = (maxd > 0 and (t - self.t0) >= maxd)
            if stop_inact or stop_max:
                time.sleep(CFG.sensor.tail_hold_ms / 1000.0)
                self.emit("stop", self.sid, reason=("max_duration" if stop_max else "inactivity"))
                self.quiet_until = time.time() + (CFG.sensor.min_quiet_ms / 1000.0)
                self.sid = None
                self.state = "IDLE"

def main():
    i2c = I2C(CFG.sensor.i2c_bus)
    tof = adafruit_vl53l0x.VL53L0X(i2c)
    tof.measurement_timing_budget = 200000  # ~200ms
    fsm = ToFFSM()
    while True:
        dist = int(tof.range)  # mm
        fsm.tick(dist)
        time.sleep(0.05)

if __name__ == "__main__":
    main()
