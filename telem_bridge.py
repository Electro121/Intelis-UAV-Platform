#!/usr/bin/env python3
"""
Intelis GCS - MAVLink to WebSocket telemetry bridge (PX4 / MAVLink autopilots)

    Flight controller --(USB | radio | UART | UDP | TCP)--> this bridge --WebSocket :8765--> index.html

Start it with no arguments and it finds the vehicle by itself, like QGroundControl:

    python3 telem_bridge.py

It searches every link at the same time and connects to the first one that carries a
vehicle heartbeat:
  * USB / serial  every serial port the computer has (Pixhawk USB, SiK/RFD telemetry radios,
                  FTDI / CP210x / CH340 adapters, Raspberry Pi and Jetson UARTs).
                  Native USB is opened directly; other ports are tried at
                  57600, 115200, 921600, 460800 and 230400 baud.
  * UDP           listens on port 14550 (SITL, WiFi telemetry, mavlink-router).
  * TCP           connects to 127.0.0.1:5760 (SITL, mavlink-router) and any address you add.
If the vehicle goes away (unplugged, rebooted, out of range) it starts searching again.

Options
    python3 telem_bridge.py tcp:192.168.4.1:5760     also try this address (tried first)
    python3 telem_bridge.py /dev/ttyUSB0 57600       also try this port at this baud
    python3 telem_bridge.py --only /dev/ttyUSB0      use only what you list, no scanning
    --udp-port 14551        listen on a different UDP port (repeatable)
    --tcp HOST:PORT         try another TCP address (repeatable)
    --ignore /dev/ttyUSB1   never open this port, e.g. a GPS (wildcards allowed, repeatable)
    --new-token             replace the operator key

Operator key
    Anyone who opens the dashboard can watch. Flight controls and the MAVLink console need
    the operator key: click "View only" in the dashboard and enter it. The key is created on
    the first run, saved in operator_token.txt next to this script and printed at every start.
    Set the GCS_TOKEN environment variable to use your own key instead.

Missions
    Operators upload a mission from the dashboard; the bridge validates it, uploads it into the
    flight controller, reads it back to verify it, and only then allows Start. The drone flies it
    onboard. The last verified mission is remembered in mission_state.json next to this script,
    so a restarted bridge recognises it.

Linux notes
    Serial access needs the dialout group:  sudo usermod -aG dialout $USER  (then log in again).
    Remove ModemManager, it grabs new USB serial devices:  sudo apt remove modemmanager
    Only one program can use a serial port or UDP port 14550 at a time: close QGroundControl.
"""

import os
os.environ.setdefault("MAVLINK20", "1")     # speak MAVLink 2 from the start (MAVLink 1 is still parsed)

import argparse
import asyncio
import collections
import fnmatch
import glob
import hashlib
import json
import math
import queue
import secrets
import socket
import struct
import sys
import threading
import time
import traceback
from pathlib import Path

from pymavlink import mavutil
import websockets

try:
    from serial.tools import list_ports     # pyserial, installed with pymavlink
except ImportError:
    list_ports = None


# ================================================================ arguments + config
def parse_args():
    p = argparse.ArgumentParser(
        description="MAVLink to WebSocket bridge with automatic vehicle detection.")
    p.add_argument("endpoints", nargs="*",
                   help="extra endpoints to try first: /dev/ttyUSB0 [BAUD], COM5 [BAUD], "
                        "udp:0.0.0.0:14551, udpout:HOST:PORT, tcp:HOST:PORT")
    p.add_argument("--only", action="store_true", help="use only the endpoints given, no scanning")
    p.add_argument("--udp-port", type=int, action="append", metavar="PORT",
                   help="UDP port to listen on (default 14550, repeatable)")
    p.add_argument("--tcp", action="append", metavar="HOST:PORT",
                   help="TCP address to try (default 127.0.0.1:5760, repeatable)")
    p.add_argument("--ignore", action="append", default=[], metavar="PORT",
                   help="serial port to skip, wildcards allowed (repeatable)")
    p.add_argument("--ws-port", type=int, default=8765, help="WebSocket port (default 8765)")
    p.add_argument("--token-file", default=str(Path(__file__).resolve().with_name("operator_token.txt")))
    p.add_argument("--new-token", action="store_true", help="generate a new operator key")
    return p.parse_args()


ARGS = parse_args()

BRIDGE_VERSION = "2026.10.10"
BRIDGE_FEATURES = ["missions"]
WS_HOST, WS_PORT = "0.0.0.0", ARGS.ws_port
STALE = 3.0                 # heartbeat age (s) after which commands are refused
LINK_TIMEOUT = 5.0          # no heartbeat for this long -> drop the link and search again
CMD_MAX_AGE = 2.0           # commands older than this are discarded, never executed late
ATTITUDE_HZ = 10            # pitch/roll/yaw rate (lower to 5 on a slow radio link)
BROADCAST_S = 0.1           # dashboard update interval

MAX_AUTH_FAILS = 5          # wrong keys per connection before it is closed
MAX_MSG_BYTES = 8192        # largest message accepted from a browser

# ---- Missions. The dashboard shows the same limits; the bridge is the authority.
MISSION_MAX_WAYPOINTS = 50
MISSION_MIN_ALT, MISSION_MAX_ALT = 5.0, 120.0           # m above the takeoff point
MISSION_MIN_SPEED, MISSION_MAX_SPEED = 1.0, 12.0        # m/s
MISSION_MIN_SPACING = 2.0                               # m between consecutive waypoints
MISSION_MAX_HOLD_S = 600                                # longest "wait here"
MISSION_LOW_BATTERY = 25                                # %: Start warns below this, never blocks
MISSION_MAX_MOVE = 30.0                                 # m the drone may move after upload before re-upload is needed
MISSION_STEP_TIMEOUT = 1.5                              # s per transfer step
MISSION_RETRIES = 3
MISSION_FILE = Path(__file__).resolve().with_name("mission_state.json")
# Failsafe settings checked before every start: (name, plain-language meaning, is_safe, safe_value, optional)
SAFETY_PARAMS = [
    ("NAV_DLL_ACT", "If the radio link to the drone is lost, it returns home",
     lambda v: v == 2, lambda v: 2, False),
    ("COM_LOW_BAT_ACT", "If the battery runs low, it returns home",
     lambda v: v == 3, lambda v: 3, False),
    ("COM_DLL_EXCEPT", "The radio-link rule also applies during missions",       # only on newer PX4
     lambda v: not (int(v) & 1), lambda v: int(v) & ~1, True),
]
INFO_PARAMS = ["RTL_RETURN_ALT", "MIS_DIST_1WP"]        # shown to the operator, never changed

DEBUG_MAVLINK = False       # print every received message (very noisy)
DEBUG_WEBSOCKET = False     # print every message received from the browser
DEBUG_STATUS = True         # one status line every 2 s

mav = mavutil.mavlink
NAN = float("nan")
SENSOR_RC = getattr(mav, "MAV_SYS_STATUS_SENSOR_RC_RECEIVER", 0x10000)
PREARM_CHECK = getattr(mav, "MAV_SYS_STATUS_PREARM_CHECK", 0x10000000)
AUTOPILOT_NAMES = {mav.MAV_AUTOPILOT_PX4: "PX4", mav.MAV_AUTOPILOT_ARDUPILOTMEGA: "ArduPilot"}


# ================================================================ operator key
def load_token():
    env = os.environ.get("GCS_TOKEN", "").strip()
    if env:
        return env, "GCS_TOKEN environment variable"
    path = Path(ARGS.token_file)
    token = ""
    if path.exists() and not ARGS.new_token:
        token = path.read_text().strip()
    if not token:
        token = secrets.token_urlsafe(18)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(token + "\n")
        return token, f"new key saved to {path}"
    return token, f"loaded from {path}"


TOKEN, TOKEN_SOURCE = load_token()


# ================================================================ shared state
S = {   # what the dashboard sees
    "lat": None, "lon": None, "pos_src": None, "alt_m": None, "alt_amsl": None,
    "heading_deg": None, "pitch_deg": None, "roll_deg": None,
    "groundspeed_mps": None,
    "battery_pct": None, "voltage_v": None,
    "gps_fix": None, "satellites": None, "hdop": None,
    "mode": None, "rc_rssi": None,
    "home_lat": None, "home_lon": None,
    "landed": None, "armed": False, "flying": False,
}
R = {}                                                  # internal timers / flags (see reset_vehicle)
V = {"sys": None, "comp": None, "hb": None, "autopilot": None, "custom_mode": None}
LINK = {"state": "searching", "kind": None, "label": None}

PREARM_WORDS = ("preflight", "prearm", "pre-arm", "arming denied", "arming refused",
                "arm check", "not ready", "cannot arm", "can't arm", "unable to arm")
PREARM_MSGS = {}                                        # text -> time received
PREARM_LOCK = threading.Lock()

conn = None
loop = None
clients = set()        # every connected browser (viewers + operators)
operators = set()      # subset that has sent the correct key
cmd_q = queue.Queue()  # (time queued, command) from operators


def reset_vehicle():
    """Forget the vehicle. The last known position and home are kept so the map can show them."""
    V.update(sys=None, comp=None, hb=None, autopilot=None, custom_mode=None)
    # Overwrite in place (never clear): the WebSocket thread reads R at any moment.
    R.update(rc_at=0.0, rc_chans=0, rc_present=False, rc_present_seen=False, rc_healthy=False,
             sys_at=0.0, prearm_present=False, prearm_ok=False,
             ekf_pos_at=0.0, pos_at=0.0, att=0.0, gpi=0.0)
    keep = {k: S[k] for k in ("lat", "lon", "pos_src", "home_lat", "home_lon")}
    S.update({k: None for k in S})
    S.update(armed=False, flying=False, **keep)
    with PREARM_LOCK:
        PREARM_MSGS.clear()


reset_vehicle()


# ================================================================ PX4 modes
PX4_MAIN = {1: "Manual", 2: "Altitude", 3: "Position", 5: "Acro", 6: "Offboard", 7: "Stabilized"}
PX4_AUTO = {1: "Hold", 2: "Takeoff", 3: "Hold", 4: "Mission", 5: "Return", 6: "Land",
            7: "Return", 8: "Follow Target", 9: "Precision Landing", 10: "VTOL Takeoff"}


def px4_mode(custom_mode):
    main = (custom_mode >> 16) & 0xFF
    sub = (custom_mode >> 24) & 0xFF
    if main == 4:
        return PX4_AUTO.get(sub, "Auto")
    if main == 3:
        return {1: "Orbit", 2: "Position Slow"}.get(sub, "Position")
    return PX4_MAIN.get(main, "Unknown")


# ================================================================ websocket helpers
async def broadcast(payload, operators_only=False):
    targets = operators if operators_only else clients
    if not targets:
        return
    dead = []
    for client in list(targets):
        try:
            await client.send(payload)
        except Exception:
            dead.append(client)
    for client in dead:
        clients.discard(client)
        operators.discard(client)


def emit(obj, operators_only=False):
    """Send an object from the MAVLink thread to browsers."""
    if loop is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(broadcast(json.dumps(obj), operators_only), loop)
    except Exception as e:
        print("[WS ERROR]", e)


def note(text, level="info", operators_only=False):
    emit({"type": "text", "level": level, "text": text}, operators_only)


def emit_text(text, operators_only=False):
    """Send an already-encoded JSON string from the MAVLink thread."""
    if loop is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(broadcast(text, operators_only), loop)
    except Exception as e:
        print("[WS ERROR]", e)


# ================================================================ derived vehicle state
def update_flying():
    ls = S["landed"]
    if ls is not None:
        S["flying"] = bool(S["armed"]) and ls in (2, 3, 4)
    else:
        S["flying"] = bool(S["armed"]) and (S["alt_m"] or 0) > 1.0


def rc_state(now):
    """True when the autopilot has a usable RC / manual-control signal.

    RC_CHANNELS alone can't tell: a receiver in failsafe keeps sending frames after the
    transmitter is switched off. PX4 clears the RC-receiver 'present' flag in SYS_STATUS as
    soon as it considers manual control lost, so that flag is the authority when available.
    """
    sys_fresh = now - R["sys_at"] < 3
    if sys_fresh and (V["autopilot"] == mav.MAV_AUTOPILOT_PX4 or R["rc_present_seen"]):
        return R["rc_present"] and R["rc_healthy"]
    return now - R["rc_at"] < 1.5 and R["rc_chans"] > 0


def ready_state(now):
    """True / False from the autopilot's own pre-arm check, None if it doesn't report one.

    PX4 sets the PREARM_CHECK health bit when it can arm in the current flight mode
    (PX4 1.14+ sets only the health bit; older PX4 and ArduPilot also set 'present').
    """
    if V["hb"] is None or now - R["sys_at"] > 3:
        return None
    if R["prearm_present"] or V["autopilot"] == mav.MAV_AUTOPILOT_PX4:
        return R["prearm_ok"]
    return None


def prearm_list(now):
    with PREARM_LOCK:
        items = sorted(PREARM_MSGS.items(), key=lambda kv: kv[1])
    return [text for text, t in items if now - t < 120][-6:]


# ================================================================ MAVLink TX
def gcs_heartbeat(c):
    c.mav.heartbeat_send(mav.MAV_TYPE_GCS, mav.MAV_AUTOPILOT_INVALID, 0, 0, mav.MAV_STATE_ACTIVE)


def send_cmd(command, *params):
    if conn is None or V["sys"] is None or V["comp"] is None:
        return
    p = (list(params) + [0] * 7)[:7]
    try:
        conn.mav.command_long_send(V["sys"], V["comp"], command, 0, *p)
        print(f"[MAVLINK TX] COMMAND_LONG command={command} params={p}")
    except Exception as e:
        print("[MAVLINK TX ERROR]", e)


def request_streams():
    """SET_MESSAGE_INTERVAL (511) for everything the dashboard needs."""
    print("[MAVLINK TX] Requesting telemetry streams...")
    streams = [
        (1, 2),             # SYS_STATUS (battery, RC present, pre-arm)
        (24, 2),            # GPS_RAW_INT
        (30, ATTITUDE_HZ),  # ATTITUDE
        (32, 5),            # LOCAL_POSITION_NED (altitude without GPS)
        (33, 5),            # GLOBAL_POSITION_INT
        (65, 2),            # RC_CHANNELS
        (74, 5),            # VFR_HUD
        (147, 1),           # BATTERY_STATUS
        (242, 1),           # HOME_POSITION
        (245, 2),           # EXTENDED_SYS_STATE
    ]
    for message_id, hz in streams:
        send_cmd(511, message_id, int(1_000_000 / hz))


# ================================================================ MAVLink RX
def handle(m):
    t = m.get_type()

    if DEBUG_MAVLINK:
        print(f"[MAVLINK RX] {t} (sys={m.get_srcSystem()}, comp={m.get_srcComponent()})")

    if t == "BAD_DATA":
        return

    src = m.get_srcSystem()
    now = time.time()

    # ---------------- HEARTBEAT
    if t == "HEARTBEAT":
        # ignore GCS / companion heartbeats (they would corrupt armed + mode)
        if m.type == mav.MAV_TYPE_GCS or m.autopilot == mav.MAV_AUTOPILOT_INVALID:
            return

        if V["sys"] is None:
            V["sys"], V["comp"] = src, m.get_srcComponent()
            print(f"[VEHICLE] sys={V['sys']} comp={V['comp']} "
                  f"autopilot={AUTOPILOT_NAMES.get(m.autopilot, m.autopilot)}")
            request_streams()
            MISSION.on_link_up(now)                     # [mission] read params + the mission on the drone

        if src != V["sys"]:
            return

        # link came back after a drop / reboot -> streams must be requested again
        if V["hb"] is not None and now - V["hb"] > 3:
            request_streams()
            MISSION.on_link_up(now)                     # [mission] re-read after a reboot

        V["hb"] = now
        V["autopilot"] = m.autopilot
        V["custom_mode"] = m.custom_mode                # [mission] to restore the mode after a refused start
        S["armed"] = bool(m.base_mode & mav.MAV_MODE_FLAG_SAFETY_ARMED)

        if m.autopilot == mav.MAV_AUTOPILOT_PX4:
            S["mode"] = px4_mode(m.custom_mode)
        else:
            try:
                S["mode"] = mavutil.mode_string_v10(m)
            except Exception:
                S["mode"] = "Unknown"

        update_flying()
        return

    # ---------------- everything else: only from the vehicle autopilot
    if V["sys"] is None or src != V["sys"] or m.get_srcComponent() != V["comp"]:
        return

    if t in MISSION_MSGS:                     # [mission] transfers, progress, parameters
        MISSION.handle(m, now)
        return

    have_att = now - R["att"] < 1.0           # ATTITUDE is the preferred heading source

    if t == "ATTITUDE":
        if all(math.isfinite(v) for v in (m.roll, m.pitch, m.yaw)):
            R["att"] = now
            S["roll_deg"] = round(math.degrees(m.roll), 2)
            S["pitch_deg"] = round(math.degrees(m.pitch), 2)
            S["heading_deg"] = round(math.degrees(m.yaw) % 360.0, 2)

    elif t == "GLOBAL_POSITION_INT":
        R["gpi"] = now
        if m.lat != 0 or m.lon != 0:            # 0,0 = no position estimate yet
            S["lat"], S["lon"], S["pos_src"] = m.lat / 1e7, m.lon / 1e7, "ekf"
            R["ekf_pos_at"] = R["pos_at"] = now
        S["alt_m"] = m.relative_alt / 1000.0
        S["alt_amsl"] = m.alt / 1000.0
        if m.hdg != 65535 and not have_att:
            S["heading_deg"] = m.hdg / 100.0
        update_flying()

    elif t == "LOCAL_POSITION_NED":
        if now - R["gpi"] > 2:                  # no global position -> local altitude
            S["alt_m"] = -m.z
            update_flying()

    elif t == "VFR_HUD":
        S["groundspeed_mps"] = m.groundspeed
        if not have_att and now - R["gpi"] > 2:
            S["heading_deg"] = m.heading

    elif t == "SYS_STATUS":
        if m.battery_remaining >= 0:
            S["battery_pct"] = m.battery_remaining
        if m.voltage_battery != 65535:
            S["voltage_v"] = m.voltage_battery / 1000.0
        present, health = m.onboard_control_sensors_present, m.onboard_control_sensors_health
        R["sys_at"] = now
        R["rc_present"] = bool(present & SENSOR_RC)
        R["rc_healthy"] = bool(health & SENSOR_RC)
        if R["rc_present"]:
            R["rc_present_seen"] = True
        was_ok = R["prearm_ok"]
        R["prearm_present"] = bool(present & PREARM_CHECK)
        R["prearm_ok"] = bool(health & PREARM_CHECK)
        if R["prearm_ok"] and not was_ok:
            with PREARM_LOCK:
                PREARM_MSGS.clear()

    elif t == "BATTERY_STATUS":
        if m.battery_remaining >= 0:
            S["battery_pct"] = m.battery_remaining
        if S["voltage_v"] is None:
            cells = [v for v in m.voltages if v != 65535]
            if cells:
                S["voltage_v"] = sum(cells) / 1000.0

    elif t == "GPS_RAW_INT":
        S["gps_fix"] = m.fix_type
        S["satellites"] = m.satellites_visible if m.satellites_visible != 255 else None
        S["hdop"] = m.eph / 100.0 if m.eph != 65535 else None
        # Before the estimator has a global position, show the raw GPS fix on the map.
        if m.fix_type >= 2 and (m.lat != 0 or m.lon != 0) and now - R["ekf_pos_at"] > 2:
            S["lat"], S["lon"], S["pos_src"] = m.lat / 1e7, m.lon / 1e7, "gps"
            R["pos_at"] = now

    elif t == "RC_CHANNELS":
        R["rc_at"] = now
        R["rc_chans"] = m.chancount
        # MAVLink defines 0-254 (255 = unknown), but PX4 sends its 0-100 % value unscaled.
        raw = m.rssi
        if raw == 255:
            S["rc_rssi"] = None
        elif V["autopilot"] == mav.MAV_AUTOPILOT_PX4:
            S["rc_rssi"] = raw if raw <= 100 else None
        else:
            S["rc_rssi"] = round(raw / 254 * 100)

    elif t == "EXTENDED_SYS_STATE":
        S["landed"] = m.landed_state or None
        update_flying()

    elif t == "HOME_POSITION":
        if m.latitude != 0 or m.longitude != 0:
            S["home_lat"] = m.latitude / 1e7
            S["home_lon"] = m.longitude / 1e7

    elif t == "COMMAND_ACK":
        print(f"[COMMAND ACK] command={m.command} result={m.result}")
        emit({"type": "ack", "command": m.command, "result": m.result}, operators_only=True)
        MISSION.on_ack(m, now)                  # [mission] start / pause / resume confirmations

    elif t == "STATUSTEXT":
        text = str(m.text).strip("\x00").strip()
        level = "critical" if m.severity <= 3 else "warn" if m.severity == 4 else "info"
        print(f"[VEHICLE] {text}")
        if any(w in text.lower() for w in PREARM_WORDS):
            with PREARM_LOCK:
                PREARM_MSGS[text] = now
        MISSION.on_text(m.severity, text, now)  # [mission] used to explain refusals
        note(text, level)

    elif t == "SERIAL_CONTROL":
        try:
            emit({"type": "console",
                  "data": bytes(m.data[:m.count]).decode("utf-8", "replace")},
                 operators_only=True)
        except Exception as e:
            print("[SERIAL CONTROL ERROR]", e)


class LinkLost(Exception):
    pass


def pump(duration):
    end = time.time() + duration
    while time.time() < end:
        try:
            msg = conn.recv_match(blocking=True, timeout=0.1)
        except Exception as e:
            raise LinkLost(f"read error: {e}")
        if msg is not None:
            try:
                handle(msg)
            except Exception as e:
                print("[HANDLE ERROR]", msg.get_type(), repr(e))
                traceback.print_exc()


# ================================================================ PX4 shell
def shell_write(text):
    if conn is None:
        return
    data = text.encode()[:400]
    flags = (mav.SERIAL_CONTROL_FLAG_EXCLUSIVE
             | mav.SERIAL_CONTROL_FLAG_RESPOND
             | mav.SERIAL_CONTROL_FLAG_MULTI)
    for i in range(0, len(data), 70):
        chunk = data[i:i + 70]
        try:
            conn.mav.serial_control_send(
                mav.SERIAL_CONTROL_DEV_SHELL, flags, 0, 0,
                len(chunk), list(chunk) + [0] * (70 - len(chunk)))
        except Exception as e:
            print("[SHELL TX ERROR]", e)


# ================================================================ commands from operators
def run_command(item):
    queued_at, c = item
    key = c.get("cmd")

    if key == "shell":
        return shell_write(str(c.get("data", "")))

    if time.time() - queued_at > CMD_MAX_AGE:
        return note(f"Command '{key}' expired before it could be sent", "warn", operators_only=True)

    if isinstance(key, str) and key.startswith("mission_"):     # [mission]
        return MISSION.command(c, time.time())

    if V["hb"] is None or time.time() - V["hb"] > STALE:
        return note("Command rejected — no vehicle link", "warn", operators_only=True)

    armed, flying = S["armed"], S["flying"]

    if key == "arm" and not armed:
        print("[COMMAND] ARM")
        send_cmd(400, 1)

    elif key == "disarm" and armed and not flying:
        print("[COMMAND] DISARM")
        send_cmd(400, 0)

    elif key == "land" and flying:
        print("[COMMAND] LAND")
        send_cmd(176, 1, 4, 6)                  # DO_SET_MODE: AUTO.LAND

    elif key == "rtl" and flying:
        print("[COMMAND] RTL")
        send_cmd(176, 1, 4, 5)                  # DO_SET_MODE: AUTO.RTL

    elif key == "takeoff" and not flying:
        try:
            alt = min(120.0, max(2.0, float(c.get("alt", 10))))
        except (TypeError, ValueError):
            alt = 10.0
        print(f"[COMMAND] TAKEOFF altitude={alt}m")

        if not armed:                           # arm first, wait for confirmation
            send_cmd(400, 1)
            end = time.time() + 5
            while not S["armed"] and time.time() < end:
                pump(0.1)
            if not S["armed"]:
                return note("Takeoff aborted — vehicle did not arm", "critical")

        if S["alt_amsl"] is not None and S["alt_m"] is not None:
            amsl = S["alt_amsl"] - S["alt_m"] + alt
        else:
            amsl = NAN
        send_cmd(22, NAN, 0, 0, NAN, NAN, NAN, amsl)

    else:
        note(f"Command '{key}' rejected in current state", "warn", operators_only=True)


def reject_queued_commands():
    while True:
        try:
            _, c = cmd_q.get_nowait()
        except queue.Empty:
            return
        if c.get("cmd") != "shell":
            note("Command rejected — no vehicle connected", "warn", operators_only=True)


# ================================================================ missions
# The mission is uploaded INTO the flight controller and flown onboard. This bridge only
# uploads, verifies (reads it back), starts, pauses, resumes, clears and monitors.
# Everything in MissionManager runs in the MAVLink thread (the only thread that touches the
# connection). The WebSocket thread only reads the cached JSON strings, under MISSION.lock.

MISSION_TYPE = 0                                          # MAV_MISSION_TYPE_MISSION
FRAME_MISSION, FRAME_REL = 2, 6                           # MAV_FRAME_MISSION, GLOBAL_RELATIVE_ALT_INT
CMD_WAYPOINT, CMD_RTL, CMD_LAND, CMD_TAKEOFF = 16, 20, 21, 22
CMD_SET_MODE, CMD_SPEED, CMD_MISSION_START = 176, 178, 300
MS_NO_MISSION, MS_NOT_STARTED, MS_ACTIVE, MS_PAUSED, MS_COMPLETE = 1, 2, 3, 4, 5   # MISSION_CURRENT.mission_state
MODE_AUTO, SUB_HOLD, SUB_MISSION = 4, 3, 4                # PX4 custom main mode / sub modes (for DO_SET_MODE)
SHOWN_CMDS = {16, 17, 18, 19, 82}                         # positions shown for missions made in other apps
GLOBAL_FRAMES = {0, 3, 5, 6, 10, 11}
MISSION_MSGS = {"MISSION_REQUEST_INT", "MISSION_REQUEST", "MISSION_ACK", "MISSION_COUNT",
                "MISSION_ITEM_INT", "MISSION_CURRENT", "MISSION_ITEM_REACHED", "PARAM_VALUE"}
ACK_TEXT = {1: "the drone reported an error", 2: "the drone doesn't support this coordinate type",
            3: "the drone doesn't support one of the steps", 4: "the mission is too long for the drone",
            13: "the steps arrived out of order", 14: "the drone refused the mission",
            15: "the transfer was cancelled"}
WATCHED_PARAMS = [p[0] for p in SAFETY_PARAMS] + INFO_PARAMS


def json_safe(o):
    """Replace NaN/Infinity (invalid JSON; browsers silently drop the whole message) with null."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [json_safe(v) for v in o]
    return o


def ground_dist(lat1, lon1, lat2, lon2):
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2 + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2)) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 6371000 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def param_decode(value, ptype):
    """PX4 sends integer parameters bit-for-bit inside the float field."""
    if 1 <= ptype <= 6:
        return struct.unpack("<i", struct.pack("<f", value))[0]
    return value


def param_encode_int(v):
    return struct.unpack("<f", struct.pack("<i", int(v)))[0]


def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def validate_mission(raw):
    """Return (normalized mission, []) or (None, [plain-language problems])."""
    if not isinstance(raw, dict):
        return None, ["The mission isn't in a format the ground station understands."]
    errs = []
    alt_d, speed, end, wps = raw.get("alt_default"), raw.get("speed"), raw.get("end"), raw.get("waypoints")
    if not _num(alt_d) or not MISSION_MIN_ALT <= alt_d <= MISSION_MAX_ALT:
        errs.append(f"Flight altitude must be between {MISSION_MIN_ALT:g} and {MISSION_MAX_ALT:g} m.")
    if not _num(speed) or not MISSION_MIN_SPEED <= speed <= MISSION_MAX_SPEED:
        errs.append(f"Speed must be between {MISSION_MIN_SPEED:g} and {MISSION_MAX_SPEED:g} m/s.")
    if end not in ("rtl", "land", "hover"):
        errs.append("Choose what the drone does when it finishes.")
    if not isinstance(wps, list) or not wps:
        return None, errs + ["Add at least one waypoint."]
    if len(wps) > MISSION_MAX_WAYPOINTS:
        errs.append(f"The mission has {len(wps)} waypoints. Maximum is {MISSION_MAX_WAYPOINTS}.")
    out = []
    for i, w in enumerate(wps, 1):
        w = w if isinstance(w, dict) else {}
        lat, lon, alt, hold = w.get("lat"), w.get("lon"), w.get("alt"), w.get("hold_s", 0) or 0
        if not (_num(lat) and _num(lon)) or abs(lat) > 90 or abs(lon) > 180 or (lat == 0 and lon == 0):
            errs.append(f"Waypoint {i} has an invalid position.")
            continue
        if alt is not None and (not _num(alt) or not MISSION_MIN_ALT <= alt <= MISSION_MAX_ALT):
            errs.append(f"Waypoint {i} altitude must be between {MISSION_MIN_ALT:g} and {MISSION_MAX_ALT:g} m.")
        if not _num(hold) or not 0 <= hold <= MISSION_MAX_HOLD_S:
            errs.append(f"Waypoint {i} wait time must be between 0 and {MISSION_MAX_HOLD_S} seconds.")
            hold = 0
        out.append({"lat": float(lat), "lon": float(lon), "alt": None if alt is None else float(alt),
                    "hold_s": int(round(hold))})
    if not errs:
        for i in range(1, len(out)):
            d = ground_dist(out[i - 1]["lat"], out[i - 1]["lon"], out[i]["lat"], out[i]["lon"])
            if d < MISSION_MIN_SPACING:
                errs.append(f"Waypoints {i} and {i + 1} are only {d:.1f} m apart. "
                            f"Minimum is {MISSION_MIN_SPACING:g} m.")
    if errs:
        return None, errs
    return {"alt_default": float(alt_d), "speed": float(speed), "end": end, "waypoints": out}, []


def mk_item(cmd, frame=FRAME_MISSION, p1=0.0, p2=0.0, p3=0.0, p4=0.0, lat=None, lon=None, z=0.0):
    return {"cmd": cmd, "frame": frame, "p": [float(p1), float(p2), float(p3), float(p4)],
            "x": 0 if lat is None else int(round(lat * 1e7)),
            "y": 0 if lon is None else int(round(lon * 1e7)), "z": float(z)}


def build_items(m, takeoff_lat, takeoff_lon):
    """seq 0 takeoff, 1 speed, then one waypoint each, then the end action."""
    wps = m["waypoints"]
    alt = lambda w: w["alt"] if w["alt"] is not None else m["alt_default"]
    items = [mk_item(CMD_TAKEOFF, FRAME_REL, p4=NAN, lat=takeoff_lat, lon=takeoff_lon, z=alt(wps[0])),
             mk_item(CMD_SPEED, p1=1, p2=m["speed"], p3=-1)]            # ground speed, throttle unchanged
    for w in wps:
        items.append(mk_item(CMD_WAYPOINT, FRAME_REL, p1=w["hold_s"], p4=NAN,
                             lat=w["lat"], lon=w["lon"], z=alt(w)))      # p2 = 0: default acceptance radius
    if m["end"] == "rtl":
        items.append(mk_item(CMD_RTL))
    elif m["end"] == "land":
        items.append(mk_item(CMD_LAND, FRAME_REL, p4=NAN, lat=wps[-1]["lat"], lon=wps[-1]["lon"]))
    return items


def item_from_msg(m):
    return {"cmd": m.command, "frame": m.frame, "p": [m.param1, m.param2, m.param3, m.param4],
            "x": m.x, "y": m.y, "z": m.z}


def same_item(a, b):
    """Compare what matters; the drone may normalise fields we don't use."""
    if a["cmd"] != b["cmd"]:
        return False
    if a["cmd"] in (CMD_WAYPOINT, CMD_TAKEOFF, CMD_LAND):
        if abs(a["x"] - b["x"]) > 1 or abs(a["y"] - b["y"]) > 1:
            return False
        if a["cmd"] != CMD_LAND and abs(a["z"] - b["z"]) > 0.05:
            return False
    if a["cmd"] == CMD_WAYPOINT and abs(a["p"][0] - (b["p"][0] if math.isfinite(b["p"][0]) else 0)) > 0.01:
        return False
    if a["cmd"] == CMD_SPEED and abs(a["p"][1] - b["p"][1]) > 0.01:
        return False
    return True


def same_items(a, b):
    return a is not None and b is not None and len(a) == len(b) and all(map(same_item, a, b))


def mission_hash(items):
    canon = [[i["cmd"], i["x"], i["y"], round(i["z"], 2),
              [round(p, 3) if math.isfinite(p) else None for p in i["p"]]] for i in items]
    return hashlib.sha256(json.dumps(canon).encode()).hexdigest()[:12]


def display_of(items):
    """What the map shows, plus the item seq of each shown waypoint (for progress)."""
    takeoff = next(([i["x"] / 1e7, i["y"] / 1e7] for i in items
                    if i["cmd"] == CMD_TAKEOFF and (i["x"] or i["y"])), None)
    wps, seqs = [], []
    for seq, i in enumerate(items):
        if i["cmd"] in SHOWN_CMDS and i["frame"] in GLOBAL_FRAMES and (i["x"] or i["y"]):
            hold = i["p"][0] if i["cmd"] == CMD_WAYPOINT and math.isfinite(i["p"][0]) else 0
            wps.append({"lat": i["x"] / 1e7, "lon": i["y"] / 1e7, "alt": round(i["z"], 1), "hold_s": int(hold)})
            seqs.append(seq)
    cmds = [i["cmd"] for i in items]
    end = "rtl" if CMD_RTL in cmds else "land" if cmds and cmds[-1] == CMD_LAND else "hover"
    speed = next((i["p"][1] for i in items if i["cmd"] == CMD_SPEED), None)
    return {"takeoff": takeoff, "waypoints": wps, "end": end, "speed": speed}, seqs


class MissionManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.cache_status = None        # last mission_status JSON (read by the WebSocket thread)
        self.cache_mission = None       # last mission JSON
        self.record = self._load()      # last mission this bridge uploaded and verified
        self.onboard = None             # what the drone holds: {"items", "source", "display", "seqs"}; None = nothing
        self.onboard_known = False
        self.uploading = None
        self.transfer = None            # one mission transfer at a time
        self.pending = None             # command waiting for COMMAND_ACK
        self.restore = None             # mode to restore after a refused start
        self.error = None               # last problem, shown until the next action
        self.errors = []                # all validation problems of the last upload attempt
        self.message = None
        self.run = None                 # the flight in progress
        self.result = None              # summary of the last finished flight
        self.rejected = None            # the drone said it can't fly the uploaded mission
        self.feasible_check_at = None
        self.verified_at = 0.0
        self.mc = {"seq": -1, "total": 0, "state": 0, "id": 0, "at": 0.0}
        self.last_mc_id = 0
        self.reached_seq = -1
        self.params = {}                # name -> (value, time received)
        self.param_req_at = 0.0
        self.applying = None
        self.linked = False
        self.download_at = None
        self.last_download_end = 0.0
        self.texts = collections.deque(maxlen=12)   # recent vehicle messages, used to explain refusals
        self.status_pub_at = 0.0

    # ---------------------------------------------------------- persistence
    def _load(self):
        try:
            rec = json.loads(MISSION_FILE.read_text())
            for it in rec["items"]:
                it["p"] = [NAN if v is None else v for v in it["p"]]
            print(f"[MISSION] Last uploaded mission {rec['id']} loaded from {MISSION_FILE.name}")
            return rec
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"[MISSION] Ignoring unreadable {MISSION_FILE.name}: {e}")
            return None

    def _save(self):
        try:
            if self.record is None:
                MISSION_FILE.unlink(missing_ok=True)
                return
            rec = dict(self.record)
            rec["items"] = [dict(i, p=[p if math.isfinite(p) else None for p in i["p"]]) for i in rec["items"]]
            tmp = MISSION_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(rec))
            os.replace(tmp, MISSION_FILE)
        except Exception as e:
            print("[MISSION] Can't save mission state:", e)

    # ---------------------------------------------------------- link events
    def on_link_up(self, now):
        if self.run and self.run.get("lost_contact") and not self.linked:
            note("Contact with the drone is back", "ok")             # [step c]
        self.linked = True
        self.download_at = now + 1.0            # read what the drone holds once streams are set up
        self.request_params(now)

    def on_link_lost(self, now):
        self.linked = False
        if self.run:                                     # [step c] the drone keeps flying on its own
            self.run["lost_contact"] = True
            note("No contact with the drone. It follows its own safety settings and returns home "
                 "if the radio link stays lost.", "critical")
        if self.transfer:
            self._finish(False, "the connection to the drone was lost")
        if self.pending:
            self.pending = None
            self.error = "Lost contact with the drone before it confirmed the command."
        self.applying = self.download_at = self.restore = None
        self.publish(now)

    def on_text(self, severity, text, now):
        self.texts.append((now, severity, text))

    def _reason(self, now, window=3.0):
        """Most recent warning-or-worse message from the drone, to explain a refusal."""
        for t, sev, text in reversed(self.texts):
            if now - t <= window and sev <= 4:
                return text
        return ""

    # ---------------------------------------------------------- parameters
    def request_params(self, now):
        self.param_req_at = now
        for name in WATCHED_PARAMS:
            conn.mav.param_request_read_send(V["sys"], V["comp"], name.encode(), -1)

    def safety(self):
        items, verdict = [], True
        for name, label, is_safe, _fix, optional in SAFETY_PARAMS:
            v = self.params.get(name)
            if v is None:
                if not optional:
                    items.append({"label": label, "ok": None})
                    verdict = None if verdict is True else verdict
                continue
            ok = bool(is_safe(v[0]))
            items.append({"label": label, "ok": ok})
            if not ok:
                verdict = False
        rtl = self.params.get("RTL_RETURN_ALT")
        first = self.params.get("MIS_DIST_1WP")
        return {"ok": verdict, "items": items, "applying": bool(self.applying),
                "return_alt": round(rtl[0], 1) if rtl else None,
                "first_wp_max": round(first[0]) if first and first[0] > 0 else None}

    # ---------------------------------------------------------- transfers
    def _begin(self, kind, done, items=None):
        self.transfer = {"kind": kind, "done": done, "items": items, "sent": set(), "last": None,
                         "count": None, "got": [], "tries": 0,
                         "deadline": time.time() + MISSION_STEP_TIMEOUT}
        self._kick()
        self.publish(time.time())

    def _kick(self):
        """(Re)send whatever the current transfer step needs."""
        t, s, c = self.transfer, V["sys"], V["comp"]
        if t["kind"] == "upload":
            if t["last"] is None:
                conn.mav.mission_count_send(s, c, len(t["items"]), MISSION_TYPE)
            else:
                self._send_item(t["last"])
        elif t["kind"] == "download":
            if t["count"] is None:
                conn.mav.mission_request_list_send(s, c, MISSION_TYPE)
            else:
                conn.mav.mission_request_int_send(s, c, len(t["got"]), MISSION_TYPE)
        else:
            conn.mav.mission_clear_all_send(s, c, MISSION_TYPE)

    def _send_item(self, seq):
        it = self.transfer["items"][seq]
        conn.mav.mission_item_int_send(V["sys"], V["comp"], seq, it["frame"], it["cmd"], 0, 1,
                                       *it["p"], it["x"], it["y"], it["z"], MISSION_TYPE)

    def _progress_step(self):
        self.transfer["tries"] = 0
        self.transfer["deadline"] = time.time() + MISSION_STEP_TIMEOUT

    def _finish(self, ok, result=None):
        t, self.transfer = self.transfer, None
        if t:
            if t["kind"] in ("download", "clear"):
                self.last_download_end = time.time()
            t["done"](ok, result)
        self.publish(time.time())

    def handle(self, m, now):
        t = m.get_type()
        if t == "MISSION_CURRENT":
            self.mc.update(seq=m.seq, total=getattr(m, "total", 0), state=getattr(m, "mission_state", 0),
                           id=getattr(m, "mission_id", 0), at=now)
            mid = self.mc["id"]
            if mid and self.record and self.onboard and self.onboard["source"] == "app" \
                    and self.record.get("px4_id") is None and now - self.verified_at < 15:
                self.record["px4_id"] = mid
                self._save()
            # Someone else (another app, or a reboot that lost it) changed the mission: read it again.
            if mid and self.last_mc_id and mid != self.last_mc_id and not self.transfer \
                    and now - self.last_download_end > 3 and now - self.verified_at > 3:
                print(f"[MISSION] The mission on the drone changed ({self.last_mc_id:#x} -> {mid:#x}); reading it")
                self.download_at = now
            if mid:
                self.last_mc_id = mid
            return
        if t == "MISSION_ITEM_REACHED":
            if self.run:
                self.reached_seq = max(self.reached_seq, m.seq)
            return
        if t == "PARAM_VALUE":
            name = m.param_id.rstrip("\x00") if isinstance(m.param_id, str) else m.param_id.decode().rstrip("\x00")
            if name in WATCHED_PARAMS:
                self.params[name] = (param_decode(m.param_value, m.param_type), now)
                a = self.applying
                if a and all(self.params.get(k, (None,))[0] == v for k, v in a["want"].items()):
                    self.applying = None
                    print(f"[MISSION] Safety settings applied: {a['want']}")
                    note("Safety settings applied: " + "; ".join(a["labels"]), "info")
            return

        # ---- transfer messages
        if getattr(m, "mission_type", 0) != MISSION_TYPE or getattr(m, "target_system", 254) not in (0, 254):
            return
        tr = self.transfer
        if not tr:
            return
        kind = tr["kind"]
        if t in ("MISSION_REQUEST_INT", "MISSION_REQUEST") and kind == "upload":
            if 0 <= m.seq < len(tr["items"]):
                tr["sent"].add(m.seq)
                tr["last"] = m.seq
                self._send_item(m.seq)
                self._progress_step()
        elif t == "MISSION_ACK":
            if kind == "upload":
                if m.type == 0 and len(tr["sent"]) == len(tr["items"]):
                    self._finish(True)
                elif m.type != 0:
                    self._finish(False, ACK_TEXT.get(m.type, "the drone found an invalid value"))
            elif kind == "clear":
                self._finish(m.type == 0, None if m.type == 0 else ACK_TEXT.get(m.type, "the drone refused"))
            elif m.type != 0:
                self._finish(False, ACK_TEXT.get(m.type, "the drone refused"))
        elif t == "MISSION_COUNT" and kind == "download" and tr["count"] is None:
            tr["count"] = m.count
            if m.count == 0:
                conn.mav.mission_ack_send(V["sys"], V["comp"], 0, MISSION_TYPE)
                self._finish(True, [])
            else:
                self._kick()
                self._progress_step()
        elif t == "MISSION_ITEM_INT" and kind == "download" and tr["count"] and m.seq == len(tr["got"]):
            tr["got"].append(item_from_msg(m))
            if len(tr["got"]) == tr["count"]:
                conn.mav.mission_ack_send(V["sys"], V["comp"], 0, MISSION_TYPE)
                self._finish(True, tr["got"])
            else:
                self._kick()
                self._progress_step()

    # ---------------------------------------------------------- what is on the drone
    def _set_onboard(self, items, now):
        if not items:
            self.onboard = None
        else:
            display, seqs = display_of(items)
            src = "app" if self.record and same_items(self.record["items"], items) else "other"
            self.onboard = {"items": items, "source": src, "display": display, "seqs": seqs}
            if src == "app" and self.mc["id"] and now - self.mc["at"] < 5:
                self.record["px4_id"] = self.mc["id"]
                self._save()
        self.onboard_known = True
        self.feasible_check_at = now + 3.5 if self.onboard else None
        self.publish_mission()

    def _download_done(self, ok, items):
        now = time.time()
        if not ok:
            self.error = f"Couldn't read the mission from the drone ({items}). Retrying."
            self.download_at = now + 5
            return
        before = self.onboard["source"] if self.onboard else None
        self._set_onboard(items, now)
        after = self.onboard["source"] if self.onboard else None
        print(f"[MISSION] Read {len(items)} items from the drone: "
              + {"app": "matches the last mission uploaded here", "other": "made in another app",
                 None: "no mission"}[after])
        if after == "other" and before != "other":
            note("A mission made in another app is loaded on the drone", "info")

    # ---------------------------------------------------------- commands
    def command(self, c, now):
        key = c.get("cmd")
        handler = {"mission_upload": self.cmd_upload, "mission_start": self.cmd_start,
                   "mission_pause": self.cmd_pause, "mission_resume": self.cmd_resume,
                   "mission_clear": self.cmd_clear, "mission_download": self.cmd_download,
                   "mission_apply_safe": self.cmd_apply_safe}.get(key)
        if handler is None:
            return note(f"Unknown mission command '{key}'", "warn", operators_only=True)
        self.error, self.errors = None, []
        if not self.linked or V["hb"] is None:
            return self._fail("The drone isn't connected.")
        handler(c, now)
        self.publish(now)

    def _fail(self, msg):
        self.error = msg
        print("[MISSION]", msg)
        self.publish(time.time())

    def cmd_upload(self, c, now):
        if self.transfer:
            return self._fail("Wait for the current transfer to finish.")
        if S["armed"]:
            return self._fail("Upload only while the drone is on the ground with its motors off.")
        mission, errs = validate_mission(c.get("mission"))
        if errs:
            self.errors = errs
            more = f" ({len(errs) - 1} more)" if len(errs) > 1 else ""
            return self._fail(errs[0] + more)
        if R["pos_at"] and now - R["pos_at"] < 3 and S["lat"] is not None:
            takeoff = (S["lat"], S["lon"])                     # take off from where the drone is now
        elif S["home_lat"] is not None:
            takeoff = (S["home_lat"], S["home_lon"])
        else:
            return self._fail("Waiting for the drone's GPS position. The mission takes off from where the drone is.")
        items = build_items(mission, *takeoff)
        self.uploading = {"id": mission_hash(items), "mission": mission, "items": items, "takeoff": list(takeoff)}
        self.result = self.rejected = None
        print(f"[MISSION] Uploading {len(items)} items ({len(mission['waypoints'])} waypoints), id {self.uploading['id']}")
        self._begin("upload", self._upload_done, items)

    def _upload_done(self, ok, err):
        if not ok:
            self.uploading = None
            self.error = f"Upload failed: {err}. Try again."
            note(self.error, "warn")
            self.download_at = time.time() + 0.5           # find out what the drone holds now
            return
        self._begin("download", self._verify_done)      # read it back and compare item by item
        self.transfer["purpose"] = "verify"

    def _verify_done(self, ok, items):
        now, up, self.uploading = time.time(), self.uploading, None
        if not ok:
            self.error = f"The mission was sent but couldn't be read back to check it ({items}). Upload again."
            note(self.error, "warn")
            self.download_at = now + 0.5
            return
        if not same_items(up["items"], items):
            self.error = "The drone stored a different mission than the one sent. Upload again."
            note(self.error, "warn")
            self._set_onboard(items, now)
            return
        self.record = {"id": up["id"], "mission": up["mission"], "items": up["items"],
                       "takeoff": up["takeoff"], "px4_id": None,
                       "saved": time.strftime("%Y-%m-%d %H:%M:%S")}
        self._save()
        self.verified_at = now
        self._set_onboard(items, now)
        print(f"[MISSION] Uploaded and verified, id {up['id']}")
        note(f"Mission uploaded and verified ({len(up['mission']['waypoints'])} waypoints)", "info")

    def cmd_download(self, c, now):
        if self.transfer:
            return self._fail("Wait for the current transfer to finish.")
        self._begin("download", self._download_done)

    def cmd_clear(self, c, now):
        if self.transfer:
            return self._fail("Wait for the current transfer to finish.")
        if S["armed"] or self.run:
            return self._fail("Land and stop the motors before clearing the mission.")
        self._begin("clear", self._clear_done)

    def _clear_done(self, ok, err):
        if not ok:
            self.error = f"Couldn't clear the mission: {err}."
            return
        self.record = self.result = self.rejected = None
        self._save()
        self._set_onboard([], time.time())
        print("[MISSION] Cleared")
        note("Mission cleared from the drone", "info")

    def start_blockers(self, now):
        """Why Start can't run right now, in plain language. Empty list = allowed."""
        if not self.linked or V["hb"] is None:
            return ["The drone isn't connected."]
        b = []
        if self.transfer:
            b.append("Wait for the current transfer to finish.")
        if self.onboard and self.onboard["source"] == "other":
            b.append("The mission on the drone was made in another app. Replace it with one from here first.")
        elif not self.onboard or not self.record:
            b.append("Upload a mission first.")
        elif self.rejected:
            b.append("The drone can't fly this mission: " + self.rejected)
        if self.run:
            b.append("A mission is already in progress.")
        elif S["armed"]:
            b.append("The drone's motors are already running.")
        if (S["gps_fix"] or 0) < 3:
            b.append("Waiting for GPS fix.")
        ready = ready_state(now)
        if ready is False:
            why = prearm_list(now)
            b.append("The drone isn't ready to fly" + (f": {why[-1]}" if why else "."))
        elif ready is None:
            b.append("Waiting for the drone's readiness check.")
        sf = self.safety()
        if sf["ok"] is False:
            b.append("Safety settings need attention. Use “Apply safe settings”.")
        elif sf["ok"] is None:
            b.append("Checking the drone's safety settings.")
        if self.record and R["pos_at"] and now - R["pos_at"] < 3 and S["lat"] is not None:
            d = ground_dist(self.record["takeoff"][0], self.record["takeoff"][1], S["lat"], S["lon"])
            if d > MISSION_MAX_MOVE:
                b.append(f"The drone is {d:.0f} m from where this mission takes off. Upload again.")
        return b

    def warnings(self, now):
        w = []
        if S["battery_pct"] is not None and 0 <= S["battery_pct"] < MISSION_LOW_BATTERY:
            w.append(f"Battery is at {S['battery_pct']}%. Charge it before a long mission.")
        return w

    def cmd_start(self, c, now):
        b = self.start_blockers(now)
        if not b and c.get("id") != self.record["id"]:
            b = ["This isn't the mission that's on the drone. Upload again."]
        if b:
            return self._fail(b[0])
        for w in self.warnings(now):
            note(w, "warn")
        self.result = None
        send_cmd(CMD_MISSION_START, 0, len(self.record["items"]) - 1)   # PX4: switch to Mission and arm
        self.pending = {"cmd": CMD_MISSION_START, "kind": "start", "deadline": now + 4, "prev": V["custom_mode"]}
        self.message = "Starting the mission…"

    def cmd_pause(self, c, now):
        if not self.run or not S["armed"]:
            return self._fail("There's no mission in flight to pause.")
        if S["mode"] == "Hold":
            return self._fail("The mission is already paused.")
        send_cmd(CMD_SET_MODE, 1, MODE_AUTO, SUB_HOLD)
        self.pending = {"cmd": CMD_SET_MODE, "kind": "pause", "deadline": now + 3}
        self.message = "Pausing…"

    def cmd_resume(self, c, now):
        if not self.run or not S["armed"]:
            return self._fail("There's no paused mission to resume.")
        if self.run["finished"]:
            return self._fail("The mission has already finished.")
        send_cmd(CMD_SET_MODE, 1, MODE_AUTO, SUB_MISSION)               # continues from the current waypoint
        self.pending = {"cmd": CMD_SET_MODE, "kind": "resume", "deadline": now + 3}
        self.message = "Resuming…"

    def cmd_apply_safe(self, c, now):
        if S["armed"]:
            return self._fail("Change safety settings only while the drone is on the ground.")
        want, labels = {}, []
        for name, label, is_safe, fix, optional in SAFETY_PARAMS:
            v = self.params.get(name)
            if v is None:
                if optional:
                    continue
                self.request_params(now)
                return self._fail("Still reading the safety settings from the drone. Try again in a moment.")
            if not is_safe(v[0]):
                want[name] = fix(v[0])
                labels.append(label)
        if not want:
            return note("The safety settings are already correct", "info")
        for name, val in want.items():
            print(f"[MISSION] Setting {name} = {val}")
            conn.mav.param_set_send(V["sys"], V["comp"], name.encode(), param_encode_int(val),
                                    mav.MAV_PARAM_TYPE_INT32)
        self.applying = {"want": want, "labels": labels, "deadline": now + 4}

    def on_ack(self, m, now):
        p = self.pending
        if not p or m.command != p["cmd"] or m.result == 5:           # 5 = in progress
            return
        self.pending, self.message = None, None
        if m.result == 0:
            if p["kind"] == "start":
                self._start_run(now)
                self.run.update(grace_until=now + 2.5, grace_text="Starting")
                note("Mission started", "ok")
            elif p["kind"] == "pause":
                note("Mission paused. The drone is hovering in place.", "info")
            else:
                if self.run:
                    self.run.update(grace_until=now + 2.5, grace_text="Resuming")
                note("Mission resumed", "info")
            return
        why = self._reason(now)
        if p["kind"] == "start":
            self.error = "The drone refused to start" + (f": {why}" if why else ".")
            self.restore = {"until": now + 3, "custom": p["prev"]}  # PX4 may switch to Mission before refusing
        else:
            self.error = f"The drone didn't accept {'the pause' if p['kind'] == 'pause' else 'resume'}" + \
                         (f": {why}" if why else ".")
        note(self.error, "warn")

    # ---------------------------------------------------------- flight tracking
    def _start_run(self, now, joined=False):
        self.run = {"started": now, "armed_seen": bool(S["armed"]), "max_alt": 0.0, "flown": 0.0,
                    "last": None, "finished": False, "seen_active": False, "noted": 0, "joined": joined}
        if not joined:
            self.reached_seq = -1
        self.result = None
        if joined:
            print("[MISSION] The drone is flying its mission; following its progress")

    def progress(self):
        """(current waypoint 1-based or None, waypoints reached)."""
        if not self.onboard or not self.onboard["seqs"]:
            return None, 0
        seqs = self.onboard["seqs"]
        n = len(seqs)
        reached = sum(1 for s in seqs if s <= self.reached_seq)
        if not self.run:
            return None, (self.result or {}).get("reached", 0)
        if self.run["finished"]:
            return None, n
        seq = self.mc["seq"]
        if seq < seqs[0]:
            cur = 1
        elif seq in seqs:
            cur = seqs.index(seq) + 1
            reached = max(reached, cur - 1)
        else:
            cur = None
            if seq > seqs[-1]:
                reached = n
        return cur, reached

    def _end(self):
        if self.onboard and self.onboard["source"] == "app" and self.record:
            return self.record["mission"]["end"]
        return self.onboard["display"]["end"] if self.onboard else None

    def _update_run(self, now):
        armed, mode = S["armed"], S["mode"]
        mc_ok = now - self.mc["at"] < 5
        if self.onboard and not self.run and armed and \
                (mode == "Mission" or (mc_ok and self.mc["state"] in (MS_ACTIVE, MS_PAUSED))):
            self._start_run(now, joined=True)             # e.g. after a bridge restart mid-flight
        r = self.run
        if not r:
            return
        if armed:
            r["armed_seen"] = True
            r["max_alt"] = max(r["max_alt"], S["alt_m"] or 0)
            if R["pos_at"] and now - R["pos_at"] < 3 and S["lat"] is not None:
                p = (S["lat"], S["lon"])
                if r["last"]:
                    d = ground_dist(r["last"][0], r["last"][1], p[0], p[1])
                    if d > 0.5:
                        r["flown"] += d
                        r["last"] = p
                else:
                    r["last"] = p
        if mc_ok and self.mc["state"] == MS_ACTIVE:
            r["seen_active"] = True
        cur, reached = self.progress()
        n = len(self.onboard["seqs"]) if self.onboard else 0
        if n and (reached >= n or (mc_ok and self.mc["state"] == MS_COMPLETE and (r["seen_active"] or r["joined"]))):
            r["finished"] = True
            reached = n
        while r["noted"] < reached:
            r["noted"] += 1
            note(f"Reached waypoint {r['noted']} of {n}", "info")
        if r["armed_seen"] and not armed:
            if r["max_alt"] < 1.5 and reached == 0 and not r["joined"]:
                self.run = None
                why = self._reason(now, 10)
                self.error = "The drone stopped its motors before taking off" + (f": {why}" if why else ".")
                note(self.error, "warn")
                return
            self._finish_run(now, reached, n)

    def _finish_run(self, now, reached, n):
        r, self.run = self.run, None
        self.result = {"finished": r["finished"], "reached": reached, "total": n,
                       "duration_s": int(now - r["started"]), "flown_m": round(r["flown"]),
                       "lost_contact": bool(r.get("lost_contact")), "joined": bool(r.get("joined"))}
        mins, secs = divmod(self.result["duration_s"], 60)
        dist = f"{r['flown'] / 1000:.2f} km" if r["flown"] >= 1000 else f"{r['flown']:.0f} m"
        if r["finished"]:
            text = f"Mission complete. Flew {dist} in {mins} min {secs:02d} s."
        elif r.get("lost_contact"):
            text = (f"The drone landed while out of contact, after {reached} of {n} waypoints. "
                    "Its safety settings brought it down.")
        else:
            text = f"Mission ended early, after {reached} of {n} waypoints. The drone has landed."
        print("[MISSION]", text)
        note(text, "ok" if r["finished"] else "warn")      # "ok" shows a toast on every dashboard

    def _run_state(self, now):
        r, mode = self.run, S["mode"]
        cur, _ = self.progress()
        n = len(self.onboard["seqs"]) if self.onboard else 0
        end = self._end()
        if mode == "Return":
            return "running", "Returning home"
        if mode == "Land" or (r["finished"] and end == "land"):
            return "running", "Landing"
        if r["finished"] and end == "hover":
            return "complete", "Finished. Hovering at the last waypoint until you act."
        if r["finished"]:
            return "running", "Finishing"
        if mode != "Mission" and now < r.get("grace_until", 0):        # heartbeat hasn't caught up yet
            return "running", r.get("grace_text", "Starting")
        if mode == "Hold" or (now - self.mc["at"] < 5 and self.mc["state"] == MS_PAUSED):
            return "paused", "Paused. The drone is hovering in place."
        if mode not in ("Mission", "Takeoff"):
            return "paused", "Someone has taken manual control of the drone."
        if not S["armed"]:
            return "running", "Starting"
        if cur == 1 and self.mc["seq"] < (self.onboard["seqs"][0] if self.onboard else 0):
            return "running", "Taking off"
        return "running", f"Flying to waypoint {cur} of {n}" if cur else "Flying"

    # ---------------------------------------------------------- periodic work
    def tick(self, now):
        if not self.linked:
            return
        t = self.transfer
        if t and now > t["deadline"]:
            if t["tries"] >= MISSION_RETRIES:
                self._finish(False, "the drone stopped answering")
            else:
                t["tries"] += 1
                t["deadline"] = now + MISSION_STEP_TIMEOUT
                self._kick()
        if self.download_at and now >= self.download_at and not self.transfer:
            self.download_at = None
            self._begin("download", self._download_done)
        if now - self.param_req_at > 30:
            self.request_params(now)
        if self.applying and now > self.applying["deadline"]:
            self.applying = None
            self.error = "Couldn't change the drone's safety settings. Try again."
            note(self.error, "warn")
            self.request_params(now)
        p = self.pending
        if p and now > p["deadline"]:
            self.pending, self.message = None, None
            self.error = {"start": "The drone didn't answer the start command.",
                          "pause": "The drone didn't answer the pause command.",
                          "resume": "The drone didn't answer the resume command."}[p["kind"]]
            note(self.error, "warn")
            if p["kind"] == "start":
                self.restore = {"until": now + 3, "custom": p.get("prev")}
        r = self.restore
        if r:
            if now > r["until"]:
                self.restore = None
            elif S["mode"] == "Mission" and not S["armed"]:
                self.restore = None                        # never leave a disarmed drone waiting in Mission mode
                cm = r["custom"]
                main, sub = ((cm >> 16) & 0xFF, (cm >> 24) & 0xFF) if cm else (3, 0)   # default Position
                if (main, sub) == (MODE_AUTO, SUB_MISSION):
                    main, sub = 3, 0
                print(f"[MISSION] Start refused; switching the drone back to its previous mode ({main}/{sub})")
                send_cmd(CMD_SET_MODE, 1, main, sub)
        if self.feasible_check_at and now >= self.feasible_check_at:
            self.feasible_check_at = None
            if self.onboard and now - self.mc["at"] < 3 and self.mc["total"] == len(self.onboard["items"]) \
                    and self.mc["state"] == MS_NO_MISSION:
                self.rejected = self._reason(now, 8) or "it reported the mission as not flyable"
                note("The drone can't fly this mission: " + self.rejected, "warn")
        self._update_run(now)
        if now - self.status_pub_at >= 0.5:
            self.publish(now)

    # ---------------------------------------------------------- to the dashboards
    def _state(self, now):
        t = self.transfer
        if t:
            if t["kind"] == "upload" or t.get("purpose") == "verify":
                return "uploading", ("Checking the mission on the drone…" if t.get("purpose") == "verify"
                                     else "Sending the mission to the drone…")
            return ("clearing", "Clearing the mission…") if t["kind"] == "clear" else ("checking", "Reading the mission on the drone…")
        if self.run:
            if not self.linked:                          # [step c] keep the last known state, say why
                return (self.run.get("last_state", "running"),
                        "No contact with the drone. It follows its own safety settings.")
            st = self._run_state(now)
            self.run["last_state"] = st[0]
            return st
        if not self.onboard_known:
            return ("checking", "Reading the mission on the drone…") if self.linked else ("unknown", "Waiting for the drone")
        if self.result:
            return "complete", ("Mission complete." if self.result["finished"] else "Mission ended early.")
        if not self.onboard:
            return "idle", "No mission on the drone."
        if self.onboard["source"] == "other":
            return "other", "A mission made in another app is loaded on the drone."
        if self.rejected:
            return "error", "The drone can't fly this mission: " + self.rejected
        return "verified", "Mission uploaded and verified."

    def status(self, now):
        state, msg = self._state(now)
        n = len(self.onboard["seqs"]) if self.onboard else 0
        cur, reached = self.progress()
        t = self.transfer
        tp = None
        if t and t["kind"] == "upload":
            tp = {"done": len(t["sent"]), "total": len(t["items"])}
        elif t and t["kind"] == "download" and t["count"]:
            tp = {"done": len(t["got"]), "total": t["count"]}
        blockers = self.start_blockers(now)
        return {
            "type": "mission_status", "state": state, "message": self.message or msg,
            "id": self.record["id"] if self.onboard and self.onboard["source"] == "app" and self.record else None,
            "source": self.onboard["source"] if self.onboard else None,
            "total": n, "current": cur, "reached": reached, "progress": round(reached / n, 3) if n else 0,
            "error": self.error, "errors": self.errors, "warnings": self.warnings(now),
            "vehicle_link": self.linked, "transfer": tp,
            "can_start": not blockers, "start_blockers": blockers,
            "safety": self.safety(), "result": self.result,
            "duration_s": int(now - self.run["started"]) if self.run else None,
            "flown_m": round(self.run["flown"]) if self.run else None,
        }

    def publish(self, now):
        self.status_pub_at = now
        try:
            txt = json.dumps(json_safe(self.status(now)), allow_nan=False)
        except Exception as e:
            print("[MISSION STATUS ERROR]", repr(e))
            traceback.print_exc()
            txt = json.dumps({"type": "mission_status", "state": "error", "message": "The ground station hit an internal error with missions.",
                              "error": f"Internal error: {e!r}. Please restart the ground station.", "errors": [], "warnings": [],
                              "id": None, "source": None, "total": 0, "current": None, "reached": 0, "progress": 0,
                              "vehicle_link": self.linked, "transfer": None, "can_start": False,
                              "start_blockers": ["The ground station hit an internal error. Please restart it."],
                              "safety": {"ok": None, "items": [], "applying": False}, "result": None,
                              "duration_s": None, "flown_m": None})
        if txt != self.cache_status:
            with self.lock:
                self.cache_status = txt
            emit_text(txt)

    def publish_mission(self):
        if not self.onboard_known:
            msg = {"type": "mission", "known": False}
        elif not self.onboard:
            msg = {"type": "mission", "known": True, "source": None}
        else:
            msg = {"type": "mission", "known": True, "source": self.onboard["source"],
                   "display": self.onboard["display"]}
            if self.onboard["source"] == "app":
                msg.update(id=self.record["id"], mission=self.record["mission"])
        txt = json.dumps(json_safe(msg), allow_nan=False)
        with self.lock:
            self.cache_mission = txt
        emit_text(txt)

    def cached(self):
        with self.lock:
            return [t for t in (self.cache_mission, self.cache_status) if t]


MISSION = MissionManager()


# ================================================================ link discovery
USB_AUTOPILOT_VIDS = {0x26AC, 0x2DAE, 0x3162, 0x1209, 0x0483, 0x35A7, 0x3185}
AUTOPILOT_WORDS = ("px4", "pixhawk", "ardupilot", "cube", "holybro", "fmu", "autopilot",
                   "flight controller", "pixracer", "durandal", "matek", "kakute")
UART_BAUDS = [57600, 115200, 921600, 460800, 230400]
DEFAULT_IGNORE = ["*Bluetooth*", "*debug-console*", "*wlan-debug*"]
LAST_GOOD_BAUD = {}
_reported = set()


def serial_candidate(dev, vid=None, desc="", explicit=False):
    name = os.path.basename(dev)
    d = (desc or "").lower()
    native = (vid in USB_AUTOPILOT_VIDS or name.startswith(("ttyACM", "cu.usbmodem", "tty.usbmodem"))
              or any(w in d for w in AUTOPILOT_WORDS))
    return {"key": "serial:" + os.path.realpath(dev), "kind": "serial", "dev": dev,
            "usb": native, "bauds": [115200] if native else list(UART_BAUDS), "explicit": explicit}


def udp_candidate(hostport, mode="udpin"):
    host, port = hostport.rsplit(":", 1)
    return {"key": f"{mode}:{hostport}", "kind": "udp", "url": f"{mode}:{hostport}",
            "host": host or "0.0.0.0", "port": int(port)}


def tcp_candidate(hostport):
    host, port = hostport.rsplit(":", 1)
    return {"key": f"tcp:{host}:{port}", "kind": "tcp", "host": host, "port": int(port)}


def parse_endpoints(items):
    out = []
    for item in items:
        if item.isdigit() and out and out[-1]["kind"] == "serial":
            out[-1]["bauds"] = [int(item)]          # "/dev/ttyUSB0 57600"
            continue
        low = item.lower()
        if low.startswith(("udp:", "udpin:")):
            out.append(udp_candidate(item.split(":", 1)[1]))
        elif low.startswith("udpout:"):
            out.append(udp_candidate(item.split(":", 1)[1], mode="udpout"))
        elif low.startswith("tcp:"):
            out.append(tcp_candidate(item[4:]))
        else:
            out.append(serial_candidate(item, explicit=True))
    return out


EXPLICIT = parse_endpoints(ARGS.endpoints)


def ignored(dev):
    real = os.path.realpath(dev)
    return any(fnmatch.fnmatch(dev, g) or fnmatch.fnmatch(real, g) for g in ARGS.ignore + DEFAULT_IGNORE)


def serial_ports():
    found = {}
    if list_ports:
        for p in list_ports.comports():
            if p.device:
                desc = " ".join(filter(None, (p.description, p.manufacturer, p.product)))
                found.setdefault(os.path.realpath(p.device), (p.device, p.vid, desc))
    if sys.platform.startswith("linux"):       # on-board UARTs of Raspberry Pi / Jetson companions
        for pattern in ("/dev/serial0", "/dev/serial1", "/dev/ttyAMA*", "/dev/ttyTHS*"):
            for dev in sorted(glob.glob(pattern)):
                found.setdefault(os.path.realpath(dev), (dev, None, ""))
    return [serial_candidate(dev, vid, desc) for dev, vid, desc in found.values() if not ignored(dev)]


def candidates():
    cands = list(EXPLICIT)
    if not ARGS.only:
        cands += serial_ports()
        cands += [udp_candidate(f"0.0.0.0:{p}") for p in (ARGS.udp_port or [14550])]
        cands += [tcp_candidate(t) for t in (ARGS.tcp or ["127.0.0.1:5760"])]
    seen, out = set(), []
    for c in cands:
        if c["key"] not in seen:
            seen.add(c["key"])
            out.append(c)
    return out


def describe(c, baud=None):
    if c["kind"] == "serial":
        if c["usb"]:
            return f"USB {c['dev']}"
        return f"Serial {c['dev']}" + (f" @ {baud}" if baud else "")
    if c["kind"] == "udp":
        return f"UDP :{c['port']}" if c["url"].startswith("udpin") else f"UDP {c['host']}:{c['port']}"
    return f"TCP {c['host']}:{c['port']}"


def report_once(c, err):
    msg = str(err)
    key = (c["key"], type(err).__name__)
    if key in _reported:
        return
    _reported.add(key)
    hint = ""
    if isinstance(err, PermissionError) or "Permission denied" in msg:
        hint = " — add your user to the dialout group: sudo usermod -aG dialout $USER"
    elif "busy" in msg.lower() or "in use" in msg.lower():
        hint = " — another program has it open (QGroundControl?)"
    print(f"[LINK] Can't open {describe(c)}: {msg}{hint}")


def close_quietly(c):
    try:
        c.close()
    except Exception:
        pass


class Search:
    """One search round: probers race, the first to see a vehicle heartbeat wins."""

    def __init__(self):
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.result = None

    def claim(self, c, cand, baud, hb):
        with self.lock:
            if self.result is None and not self.stop.is_set():
                self.result = (c, cand, baud, hb)
                self.stop.set()
                return True
        return False


def is_vehicle_heartbeat(m):
    return (m.get_type() == "HEARTBEAT" and m.autopilot != mav.MAV_AUTOPILOT_INVALID
            and m.type != mav.MAV_TYPE_GCS)


def listen(c, search, seconds, extend_on_mavlink=0.0):
    """Read for `seconds` while sending GCS heartbeats. Returns a vehicle HEARTBEAT or None.
    The heartbeats matter: PX4 only starts MAVLink on its USB port after it hears from a GCS."""
    end = time.time() + seconds
    last_tx = 0.0
    extended = False
    while time.time() < end and not search.stop.is_set():
        now = time.time()
        if now - last_tx >= 0.5:
            last_tx = now
            try:
                gcs_heartbeat(c)
            except Exception:
                pass
        m = c.recv_match(blocking=True, timeout=0.2)
        if m is None or m.get_type() == "BAD_DATA":
            continue
        if extend_on_mavlink and not extended:     # right baud, give the heartbeat time to arrive
            end = max(end, time.time() + extend_on_mavlink)
            extended = True
        if is_vehicle_heartbeat(m):
            return m
    return None


def probe_serial(cand, search):
    dev = cand["dev"]
    bauds = list(cand["bauds"])
    good = LAST_GOOD_BAUD.get(cand["key"])
    if good:
        bauds = [good] + [b for b in bauds if b != good]
    while not search.stop.is_set():
        for baud in bauds:
            if search.stop.is_set():
                return
            if sys.platform != "win32" and not os.path.exists(dev):
                return                              # unplugged
            try:
                c = mavutil.mavlink_connection(dev, baud=baud, source_system=254,
                                               source_component=190, autoreconnect=False)
            except Exception as e:
                report_once(cand, e)
                return
            try:
                try:
                    c.port.write_timeout = 0.5      # never block on a port nobody reads
                except Exception:
                    pass
                hb = listen(c, search, 4.0 if cand["usb"] else 2.5, extend_on_mavlink=3.0)
            except Exception:
                close_quietly(c)                    # device vanished or I/O error
                return
            if hb is not None and search.claim(c, cand, baud, hb):
                return
            close_quietly(c)
        search.stop.wait(1.0)


def probe_udp(cand, search):
    try:
        c = mavutil.mavlink_connection(cand["url"], source_system=254, source_component=190)
    except Exception as e:
        report_once(cand, e)
        search.stop.wait(5)
        return
    try:
        while not search.stop.is_set():
            hb = listen(c, search, 2.0)
            if hb is not None:
                if search.claim(c, cand, None, hb):
                    return
                break
    except Exception:
        pass
    close_quietly(c)


def probe_tcp(cand, search):
    try:
        socket.create_connection((cand["host"], cand["port"]), timeout=1.0).close()
    except OSError:
        search.stop.wait(3)                         # nothing listening there (yet)
        return
    try:
        c = mavutil.mavlink_connection(f"tcp:{cand['host']}:{cand['port']}", source_system=254,
                                       source_component=190, retries=0)
    except Exception:
        search.stop.wait(3)
        return
    try:
        hb = listen(c, search, 5.0)
        if hb is not None and search.claim(c, cand, None, hb):
            return
    except Exception:
        pass
    close_quietly(c)
    search.stop.wait(2)


PROBERS = {"serial": probe_serial, "udp": probe_udp, "tcp": probe_tcp}


def find_vehicle():
    search = Search()
    threads, started = {}, {}
    last_listing = None
    while not search.stop.is_set():
        cands = candidates()
        listing = ", ".join(describe(c) for c in cands) or "nothing to search"
        if listing != last_listing:
            print(f"[LINK] Searching: {listing}")
            last_listing = listing
        now = time.time()
        for c in cands:
            t = threads.get(c["key"])
            if (t is None or not t.is_alive()) and now - started.get(c["key"], 0) > 3:
                t = threading.Thread(target=PROBERS[c["kind"]], args=(c, search), daemon=True)
                threads[c["key"]], started[c["key"]] = t, now
                t.start()
        reject_queued_commands()
        search.stop.wait(1.0)
    for t in threads.values():
        t.join(timeout=2)
    return search.result


def link_label(cand, baud, c):
    label = describe(cand, baud)
    if cand["kind"] == "udp":
        peer = getattr(c, "last_address", None)
        if peer:
            label += f" from {peer[0]}"
    kind = "usb" if cand["kind"] == "serial" and cand["usb"] else cand["kind"]
    return kind, label


# ================================================================ MAVLink thread
def run_link():
    last_tx = last_status = 0.0
    while True:
        now = time.time()
        if now - last_tx >= 1:
            last_tx = now
            try:
                gcs_heartbeat(conn)
            except Exception as e:
                raise LinkLost(f"write error: {e}")

        pump(0.1)

        while True:
            try:
                run_command(cmd_q.get_nowait())
            except queue.Empty:
                break

        now = time.time()
        try:
            MISSION.tick(now)                   # [mission] transfer timeouts, progress, status
        except Exception as e:
            print("[MISSION TICK ERROR]", repr(e))
            traceback.print_exc()
        if V["hb"] is not None and now - V["hb"] > LINK_TIMEOUT:
            raise LinkLost(f"no heartbeat for {LINK_TIMEOUT:.0f} s")

        if DEBUG_STATUS and now - last_status > 2:
            last_status = now
            rc = rc_state(now)
            print(f"[STATUS] {LINK['label']} HB={now - V['hb']:.1f}s mode={S['mode']} "
                  f"armed={S['armed']} flying={S['flying']} ready={ready_state(now)} "
                  f"alt={S['alt_m']} pitch={S['pitch_deg']} roll={S['roll_deg']} "
                  f"GPS={S['gps_fix']} sat={S['satellites']} batt={S['battery_pct']} "
                  f"rc={'OK' if rc else 'LOST'} rssi={S['rc_rssi']} "
                  f"viewers={len(clients) - len(operators)} operators={len(operators)}")


def reader():
    global conn
    while True:
        LINK.update(state="searching", kind=None, label=None)
        found = find_vehicle()
        if not found:
            continue
        c, cand, baud, hb = found
        if baud:
            LAST_GOOD_BAUD[cand["key"]] = baud
        kind, label = link_label(cand, baud, c)
        reset_vehicle()
        conn = c
        LINK.update(state="connected", kind=kind, label=label)
        print("=" * 60)
        print(f"[LINK] Vehicle found on {label}")
        print("=" * 60)
        note("Drone connected", "info")
        try:
            handle(hb)
            run_link()
        except LinkLost as e:
            print(f"[LINK] Lost {label}: {e}")
            note("Connection to the drone lost. Reconnecting…", "warn")
        except Exception as e:
            print("[LINK ERROR]", repr(e))
        finally:
            conn = None
            close_quietly(c)
            MISSION.on_link_lost(time.time())   # [mission] abort transfers; flight state is kept
            reset_vehicle()


# ================================================================ snapshot + websocket server
def snapshot():
    now = time.time()
    hb = V["hb"]
    rc = bool(hb) and rc_state(now)
    state = dict(S)
    state.update({
        "type": "state",
        "hb_age": None if hb is None else now - hb,
        "rc": rc,
        "rc_rssi": S["rc_rssi"] if rc and now - R["rc_at"] < 1.5 else None,
        "ready": ready_state(now),
        "prearm_msgs": prearm_list(now),
        "pos_age": round(now - R["pos_at"], 1) if R["pos_at"] else None,
        "autopilot": AUTOPILOT_NAMES.get(V["autopilot"]),
        "link": dict(LINK),
    })
    return state


async def handle_client(ws):
    clients.add(ws)
    fails = 0
    print(f"[WS] Dashboard connected ({len(clients)} client(s), {len(operators)} operator(s))")
    try:
        await ws.send(json.dumps({"type": "hello", "version": BRIDGE_VERSION, "features": BRIDGE_FEATURES}))
        await ws.send(json.dumps({"type": "auth", "ok": False}))     # everyone starts view-only
        await ws.send(json.dumps(snapshot()))
        for txt in MISSION.cached():                                  # [mission] current mission + progress
            await ws.send(txt)

        async for raw in ws:
            if DEBUG_WEBSOCKET:
                print("[WS RX]", raw if ws not in operators else "(operator message)")
            try:
                c = json.loads(raw)
            except Exception:
                continue
            if not isinstance(c, dict):
                continue
            cmd = c.get("cmd")

            if cmd == "auth":
                supplied = str(c.get("token", ""))
                if secrets.compare_digest(supplied.encode(), TOKEN.encode()):
                    if ws not in operators:
                        print("[WS] Operator signed in")
                    operators.add(ws)
                    fails = 0
                    await ws.send(json.dumps({"type": "auth", "ok": True, "attempt": True}))
                else:
                    fails += 1
                    print(f"[WS] Wrong operator key ({fails}/{MAX_AUTH_FAILS})")
                    operators.discard(ws)
                    await asyncio.sleep(1)                              # slow down guessing
                    await ws.send(json.dumps({"type": "auth", "ok": False, "attempt": True,
                                              "left": MAX_AUTH_FAILS - fails}))
                    if fails >= MAX_AUTH_FAILS:
                        await ws.close(code=1008, reason="too many wrong keys")
                        break
                continue

            if cmd == "logout":
                if ws in operators:
                    print("[WS] Operator signed out")
                operators.discard(ws)
                await ws.send(json.dumps({"type": "auth", "ok": False, "logout": True}))
                continue

            if ws in operators:
                cmd_q.put((time.time(), c))
            else:
                await ws.send(json.dumps({"type": "text", "level": "warn",
                                          "text": "View only — sign in as operator to use controls"}))
    except Exception as e:
        print("[WS CLIENT ERROR]", e)
    finally:
        clients.discard(ws)
        operators.discard(ws)
        print(f"[WS] Dashboard disconnected ({len(clients)} client(s), {len(operators)} operator(s))")


async def main():
    global loop
    loop = asyncio.get_running_loop()

    print("=" * 60)
    print("INTELIS GCS TELEMETRY BRIDGE")
    print("=" * 60)
    print(f"Operator key : {TOKEN}")
    print(f"               ({TOKEN_SOURCE})")
    print(f"Version      : {BRIDGE_VERSION} (missions supported)")
    print(f"WebSocket    : ws://{WS_HOST}:{WS_PORT}")
    print("Vehicle link : " + ("only " if ARGS.only else "automatic search") +
          (f" + {', '.join(describe(c) for c in EXPLICIT)}" if EXPLICIT else ""))
    print("=" * 60)

    MISSION.publish(time.time())                # [mission] initial status for the first dashboards
    threading.Thread(target=reader, daemon=True).start()

    async with websockets.serve(handle_client, WS_HOST, WS_PORT, max_size=MAX_MSG_BYTES):
        print(f"[WS] Server ready: ws://{WS_HOST}:{WS_PORT}")
        while True:
            try:
                await broadcast(json.dumps(snapshot()))
                await asyncio.sleep(BROADCAST_S)
            except Exception as e:
                print("[WS LOOP ERROR]", e)
                await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBridge stopped.")
