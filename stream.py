#!/usr/bin/env python3
"""SIYI / real-camera RTSP -> MJPEG bridge.

Serves multipart MJPEG at http://0.0.0.0:<port>/video.mjpeg
Health check at /healthz

Default URL is the HM30 camera on the SIYI LAN:
  rtsp://192.168.144.25:8554/main.264
"""
import os
import json
import select
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cv2
import numpy as np

PORT = int(os.environ.get("CAM_BRIDGE_PORT", "8091"))
RTSP_URL = os.environ.get("FPV_RTSP_URL", "rtsp://192.168.144.25:8554/main.264")
OUT_W = int(os.environ.get("CAM_OUT_W", "960"))
JPEG_Q = int(os.environ.get("CAM_JPEG_Q", "70"))
FPS = float(os.environ.get("CAM_FPS", "15"))
FRAME_MAX_AGE_S = 2.0
STREAM_TIMEOUT_S = 5.0

# ffmpeg mjpeg quality scale: 2 (best) .. 31 (worst). Q70 -> 11, Q90 -> 5
QV = max(2, min(31, round(31 - JPEG_Q * 0.29)))

_lock = threading.Lock()
_latest = {"jpeg": None, "ts": 0.0}


def _placeholder(text):
    img = np.full((360, 640, 3), 30, np.uint8)
    cv2.putText(img, text, (28, 190), cv2.FONT_HERSHEY_SIMPLEX, 0.7,
                (180, 180, 180), 2, cv2.LINE_AA)
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, JPEG_Q])
    return buf.tobytes() if ok else None


_WAITING = _placeholder("Waiting for FPV camera...")


def frame_snapshot():
    """Never present a frozen frame as a live camera view."""
    with _lock:
        jpeg, ts = _latest["jpeg"], _latest["ts"]
    age = max(0.0, time.monotonic() - ts) if jpeg is not None else None
    fresh = age is not None and age < FRAME_MAX_AGE_S
    return (jpeg if fresh else _WAITING), {"ok": True, "fresh": fresh,
                                          "frame_age_s": age}


def _publish(bgr):
    if bgr is None or bgr.size == 0:
        return
    h, w = bgr.shape[:2]
    if OUT_W and w > OUT_W:
        bgr = cv2.resize(bgr, (OUT_W, int(h * OUT_W / w)))
    ok, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, JPEG_Q])
    if ok:
        with _lock:
            _latest["jpeg"] = buf.tobytes()
            _latest["ts"] = time.monotonic()


def _opencv_loop():
    os.environ.setdefault("OPENCV_FFMPEG_CAPTURE_OPTIONS",
                          "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay")
    while True:
        cap = cv2.VideoCapture(RTSP_URL, cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not cap.isOpened():
            cap.release()
            print(f"[rtsp_bridge] opencv could not open {RTSP_URL}", flush=True)
            time.sleep(2.0)
            continue
        print(f"[rtsp_bridge] opencv opened {RTSP_URL}", flush=True)
        fail = 0
        while True:
            ok, frame = cap.read()
            if not ok or frame is None:
                fail += 1
                if fail > 30:
                    break
                time.sleep(0.05)
                continue
            fail = 0
            _publish(frame)
        cap.release()
        print("[rtsp_bridge] opencv stream dropped, reconnecting", flush=True)
        time.sleep(1.0)


def _ffmpeg_loop():
    """ffmpeg MJPEG pipe (handles SIYI HEVC reliably)."""
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-rtsp_transport", "tcp",
        "-fflags", "nobuffer+discardcorrupt", "-flags", "low_delay",
        "-i", RTSP_URL, "-an",
        "-vf", f"fps={FPS},scale='min({OUT_W},iw)':-2",
        "-pix_fmt", "yuvj420p",
        "-f", "mjpeg", "-q:v", str(QV),
        "pipe:1",
    ]
    while True:
        print(f"[rtsp_bridge] ffmpeg {RTSP_URL}", flush=True)
        try:
            # stderr is inherited so errors show in the bridge log
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=0)
        except FileNotFoundError:
            print("[rtsp_bridge] ffmpeg not found", flush=True)
            time.sleep(5.0)
            continue
        buf = b""
        last_frame = time.monotonic()
        try:
            while True:
                if time.monotonic() - last_frame > STREAM_TIMEOUT_S:
                    print("[rtsp_bridge] no camera frames; reconnecting", flush=True)
                    break
                ready, _, _ = select.select([proc.stdout], [], [], 1.0)
                if not ready:
                    continue
                chunk = os.read(proc.stdout.fileno(), 16384)
                if not chunk:
                    break
                buf += chunk
                while True:
                    soi = buf.find(b"\xff\xd8")
                    if soi < 0:
                        if len(buf) > 65536:
                            buf = buf[-2:]  # keep a possible split marker
                        break
                    if soi > 0:
                        buf = buf[soi:]  # drop junk before SOI
                    eoi = buf.find(b"\xff\xd9", 2)
                    if eoi < 0:
                        if len(buf) > 4_000_000:
                            buf = b""
                        break
                    jpeg = buf[:eoi + 2]
                    buf = buf[eoi + 2:]
                    with _lock:
                        _latest["jpeg"] = jpeg
                        _latest["ts"] = time.monotonic()
                    last_frame = time.monotonic()
        finally:
            if proc.poll() is None:
                proc.kill()
            try:
                proc.wait(timeout=2)
            except Exception:
                pass
            proc.stdout.close()
        print("[rtsp_bridge] ffmpeg dropped, reconnecting", flush=True)
        time.sleep(1.0)


def capture_loop():
    if os.environ.get("FPV_USE_OPENCV", "0") == "1":
        _opencv_loop()
        return
    _ffmpeg_loop()


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/healthz":
            _, status = frame_snapshot()
            body = json.dumps(status).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if not self.path.startswith("/video.mjpeg"):
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "multipart/x-mixed-replace;boundary=frame")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        last, last_sent = None, 0.0
        try:
            while True:
                j, _ = frame_snapshot()
                now = time.monotonic()
                # send only on a new frame (or a 1s keepalive)
                if j is not last or now - last_sent > 1.0:
                    self.wfile.write(b"--frame\r\nContent-Type: image/jpeg\r\n")
                    self.wfile.write(f"Content-Length: {len(j)}\r\n\r\n".encode())
                    self.wfile.write(j)
                    self.wfile.write(b"\r\n")
                    last, last_sent = j, now
                time.sleep(0.01)
        except OSError:
            pass


def main():
    threading.Thread(target=capture_loop, daemon=True).start()
    print(f"[rtsp_bridge] {RTSP_URL} -> MJPEG http://0.0.0.0:{PORT}/video.mjpeg",
          flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()


if __name__ == "__main__":
    main()
