#!/usr/bin/env python3
"""
Real Drone MAVLink -> WebSocket Telemetry Bridge (PX4)

    Flight controller -> USB/serial -> pymavlink -> WebSocket :8765 -> index.html

Usage:
    GCS_TOKEN=your-long-secret python3 telem_bridge.py /dev/ttyACM0
    GCS_TOKEN=your-long-secret python3 telem_bridge.py /dev/ttyUSB0 57600   (SiK radio)

ACCESS MODEL
    Anyone who connects can WATCH (telemetry, position, status messages).
    Only a client that sends the operator token can CONTROL (arm, takeoff, land,
    RTL) or use the PX4 shell. The shell output is also sent to operators only.
    If GCS_TOKEN is not set, a random token is generated and printed at startup.

Only one program can open the serial port: close QGroundControl first.
"""

import asyncio
import json
import os
import queue
import secrets
import sys
import threading
import time

from pymavlink import mavutil
import websockets

# ---------------------------------------------------------------- config
ENDPOINT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyACM0"
BAUD = int(sys.argv[2]) if len(sys.argv) > 2 else 57600
WS_HOST, WS_PORT = "0.0.0.0", 8765
STALE = 3.0

TOKEN = os.environ.get("GCS_TOKEN") or secrets.token_urlsafe(16)
TOKEN_FROM_ENV = bool(os.environ.get("GCS_TOKEN"))
MAX_AUTH_FAILS = 5          # per connection, then the socket is closed
MAX_MSG_BYTES = 8192        # largest message accepted from a browser

DEBUG_MAVLINK = False      # print every received message (very noisy)
DEBUG_WEBSOCKET = False    # print every message received from the browser
DEBUG_STATUS = True        # one status line every 2 s

mav = mavutil.mavlink
NAN = float("nan")

# ---------------------------------------------------------------- state
S = {
    "lat": None, "lon": None, "alt_m": None, "alt_amsl": None,
    "heading_deg": None, "groundspeed_mps": None,
    "battery_pct": None, "voltage_v": None,
    "gps_fix": None, "satellites": None, "hdop": None,
    "mode": None, "rc_rssi": None,
    "home_lat": None, "home_lon": None,
    "landed": None, "last_update": None,
    "armed": False, "flying": False,
}
R = {"at": 0.0, "chans": 0, "sensor": None, "gpi": 0.0}   # RC + last GLOBAL_POSITION_INT time
V = {"sys": None, "comp": None, "hb": None}               # vehicle identity / last heartbeat

conn = None
loop = None
clients = set()        # every connected browser (viewers + operators)
operators = set()      # subset that has sent the correct token
cmd_q = queue.Queue()

# ---------------------------------------------------------------- PX4 modes
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


# ---------------------------------------------------------------- websocket helpers
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
        asyncio.run_coroutine_threadsafe(
            broadcast(json.dumps(obj), operators_only), loop)
    except Exception as e:
        print("[WS ERROR]", e)


def note(text, level="info", operators_only=False):
    emit({"type": "text", "level": level, "text": text}, operators_only)


# ---------------------------------------------------------------- flight state
def update_flying():
    ls = S["landed"]
    if ls is not None:
        S["flying"] = bool(S["armed"]) and ls in (2, 3, 4)
    else:
        S["flying"] = bool(S["armed"]) and (S["alt_m"] or 0) > 1.0


# ---------------------------------------------------------------- MAVLink TX
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
        (1, 2),     # SYS_STATUS
        (24, 2),    # GPS_RAW_INT
        (32, 5),    # LOCAL_POSITION_NED (altitude without GPS)
        (33, 5),    # GLOBAL_POSITION_INT
        (65, 2),    # RC_CHANNELS
        (74, 5),    # VFR_HUD
        (147, 1),   # BATTERY_STATUS
        (242, 1),   # HOME_POSITION
        (245, 2),   # EXTENDED_SYS_STATE
    ]
    for message_id, hz in streams:
        send_cmd(511, message_id, int(1_000_000 / hz))


# ---------------------------------------------------------------- MAVLink RX
def handle(m):
    t = m.get_type()

    if DEBUG_MAVLINK:
        print(f"[MAVLINK RX] {t} (sys={m.get_srcSystem()}, comp={m.get_srcComponent()})")

    if t == "BAD_DATA":
        return

    src = m.get_srcSystem()

    # ---------------- HEARTBEAT
    if t == "HEARTBEAT":
        # ignore GCS / companion heartbeats (they would corrupt armed + mode)
        if m.type == mav.MAV_TYPE_GCS or m.autopilot == mav.MAV_AUTOPILOT_INVALID:
            return

        if V["sys"] is None:
            V["sys"], V["comp"] = src, m.get_srcComponent()
            print("=" * 48)
            print(f"[VEHICLE FOUND] sys={V['sys']} comp={V['comp']}")
            print("=" * 48)
            request_streams()

        if src != V["sys"]:
            return

        # link came back after a drop / reboot -> streams must be requested again
        if V["hb"] is not None and time.time() - V["hb"] > 5:
            request_streams()

        V["hb"] = time.time()
        S["armed"] = bool(m.base_mode & mav.MAV_MODE_FLAG_SAFETY_ARMED)

        if m.autopilot == mav.MAV_AUTOPILOT_PX4:
            S["mode"] = px4_mode(m.custom_mode)
        else:
            try:
                S["mode"] = mavutil.mode_string_v10(m)
            except Exception:
                S["mode"] = "Unknown"

        update_flying()
        S["last_update"] = time.time()
        return

    # ---------------- everything else: only from the vehicle autopilot
    if V["sys"] is None or src != V["sys"] or m.get_srcComponent() != V["comp"]:
        return

    if t == "GLOBAL_POSITION_INT":
        R["gpi"] = time.time()
        if m.lat != 0 or m.lon != 0:            # 0,0 means no fix, don't plot it
            S["lat"] = m.lat / 1e7
            S["lon"] = m.lon / 1e7
        S["alt_m"] = m.relative_alt / 1000.0
        S["alt_amsl"] = m.alt / 1000.0
        if m.hdg != 65535:
            S["heading_deg"] = m.hdg / 100.0
        update_flying()

    elif t == "LOCAL_POSITION_NED":
        # no global position (no GPS lock) -> use the local estimate for altitude
        if time.time() - R["gpi"] > 2:
            S["alt_m"] = -m.z
            update_flying()

    elif t == "VFR_HUD":
        S["groundspeed_mps"] = m.groundspeed
        if time.time() - R["gpi"] > 2:
            S["heading_deg"] = m.heading

    elif t == "SYS_STATUS":
        if m.battery_remaining >= 0:
            S["battery_pct"] = m.battery_remaining
        if m.voltage_battery != 65535:
            S["voltage_v"] = m.voltage_battery / 1000.0

        bit = 0x10000000                        # MAV_SYS_STATUS_SENSOR_RC_RECEIVER
        has = (m.onboard_control_sensors_present
               & m.onboard_control_sensors_enabled & bit)
        R["sensor"] = bool(m.onboard_control_sensors_health & bit) if has else None

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

    elif t == "RC_CHANNELS":
        R["at"] = time.time()
        R["chans"] = m.chancount
        S["rc_rssi"] = round(m.rssi / 254 * 100) if m.rssi != 255 else None

    elif t == "EXTENDED_SYS_STATE":
        S["landed"] = m.landed_state or None
        update_flying()

    elif t == "HOME_POSITION":
        S["home_lat"] = m.latitude / 1e7
        S["home_lon"] = m.longitude / 1e7

    elif t == "COMMAND_ACK":
        print(f"[COMMAND ACK] command={m.command} result={m.result}")
        # command results are only relevant to the people sending commands
        emit({"type": "ack", "command": m.command, "result": m.result},
             operators_only=True)

    elif t == "STATUSTEXT":
        text = str(m.text).strip("\x00")
        level = "critical" if m.severity <= 3 else "warn" if m.severity == 4 else "info"
        print(f"[PX4 STATUS] {text}")
        note(text, level)

    elif t == "SERIAL_CONTROL":
        # PX4 shell output: operators only
        try:
            emit({"type": "console",
                  "data": bytes(m.data[:m.count]).decode("utf-8", "replace")},
                 operators_only=True)
        except Exception as e:
            print("[SERIAL CONTROL ERROR]", e)
        return

    else:
        return

    S["last_update"] = time.time()


def pump(duration):
    end = time.time() + duration
    while time.time() < end:
        try:
            msg = conn.recv_match(blocking=True, timeout=0.1)
            if msg is not None:
                handle(msg)
        except Exception as e:
            print("[MAVLINK RX ERROR]", e)
            return


# ---------------------------------------------------------------- PX4 shell
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


# ---------------------------------------------------------------- commands from the browser
# (only operators ever reach this queue)
def run_command(c):
    key = c.get("cmd")

    if key == "shell":
        return shell_write(str(c.get("data", "")))

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


# ---------------------------------------------------------------- MAVLink thread
def reader():
    global conn

    print()
    print("=" * 60)
    print("REAL DRONE MAVLINK BRIDGE")
    print("=" * 60)
    print(f"Serial device : {ENDPOINT}")
    print(f"Baud rate     : {BAUD}")
    print(f"WebSocket     : ws://{WS_HOST}:{WS_PORT}")
    print("=" * 60)

    try:
        conn = mavutil.mavlink_connection(
            ENDPOINT, baud=BAUD,
            source_system=254, source_component=190,
            autoreconnect=True)
    except Exception as e:
        print("[FATAL] Could not open MAVLink connection:", e)
        return

    print("[MAVLINK] Serial opened. Waiting for vehicle HEARTBEAT...")

    last_gcs_hb = 0.0
    last_status = 0.0

    while True:
        try:
            now = time.time()

            if now - last_gcs_hb > 1:
                conn.mav.heartbeat_send(mav.MAV_TYPE_GCS, mav.MAV_AUTOPILOT_INVALID, 0, 0, 0)
                last_gcs_hb = now

            pump(0.1)

            while True:
                try:
                    run_command(cmd_q.get_nowait())
                except queue.Empty:
                    break

            if DEBUG_STATUS and now - last_status > 2:
                last_status = now
                if V["hb"] is None:
                    print("[STATUS] NO VEHICLE HEARTBEAT")
                else:
                    print(f"[STATUS] HB={now - V['hb']:.1f}s mode={S['mode']} "
                          f"armed={S['armed']} flying={S['flying']} alt={S['alt_m']} "
                          f"GPS={S['gps_fix']} sat={S['satellites']} "
                          f"batt={S['battery_pct']} rc={S['rc_rssi']} "
                          f"viewers={len(clients) - len(operators)} operators={len(operators)}")

        except Exception as e:
            print("[MAVLINK THREAD ERROR]", repr(e))
            time.sleep(1)


# ---------------------------------------------------------------- snapshot + websocket server
def snapshot():
    now = time.time()
    hb = V["hb"]
    rc = now - R["at"] < 2 and R["chans"] > 0 and R["sensor"] is not False
    state = dict(S)
    state.update({"type": "state", "hb_age": None if hb is None else now - hb, "rc": rc})
    return state


async def handle_client(ws):
    clients.add(ws)
    fails = 0
    print(f"[WS] Dashboard connected ({len(clients)} client(s), {len(operators)} operator(s))")
    try:
        # tell the browser it starts as view-only
        await ws.send(json.dumps({"type": "auth", "ok": False}))
        await ws.send(json.dumps(snapshot()))

        async for raw in ws:
            if DEBUG_WEBSOCKET:
                print("[WS RX]", raw if ws not in operators else "(operator message)")
            try:
                c = json.loads(raw)
            except Exception:
                continue
            if not isinstance(c, dict):
                continue

            # ---- authentication
            if c.get("cmd") == "auth":
                supplied = str(c.get("token", ""))
                if secrets.compare_digest(supplied.encode(), TOKEN.encode()):
                    if ws not in operators:
                        print("[WS] Operator authenticated")
                    operators.add(ws)
                    await ws.send(json.dumps({"type": "auth", "ok": True}))
                else:
                    fails += 1
                    print(f"[WS] Bad operator token ({fails}/{MAX_AUTH_FAILS})")
                    await ws.send(json.dumps({"type": "auth", "ok": False}))
                    await asyncio.sleep(1)          # slow down guessing
                    if fails >= MAX_AUTH_FAILS:
                        await ws.close(code=1008, reason="too many bad tokens")
                        break
                continue

            # ---- commands: operators only
            if ws in operators:
                cmd_q.put(c)
            else:
                await ws.send(json.dumps({
                    "type": "text", "level": "warn",
                    "text": "View-only access — controls are disabled"}))
    except Exception as e:
        print("[WS CLIENT ERROR]", e)
    finally:
        clients.discard(ws)
        operators.discard(ws)
        print(f"[WS] Dashboard disconnected ({len(clients)} client(s), {len(operators)} operator(s))")


async def main():
    global loop
    loop = asyncio.get_running_loop()
    threading.Thread(target=reader, daemon=True).start()

    print("=" * 60)
    if TOKEN_FROM_ENV:
        print("Operator token loaded from GCS_TOKEN")
    else:
        print(f"Operator token (random, changes every start): {TOKEN}")
    print("Operator link: <page-url>#key=<token>")
    print("=" * 60)

    async with websockets.serve(handle_client, WS_HOST, WS_PORT, max_size=MAX_MSG_BYTES):
        print(f"[WS] Server ready: ws://{WS_HOST}:{WS_PORT}\n")
        while True:
            try:
                await broadcast(json.dumps(snapshot()))
                await asyncio.sleep(0.2)
            except Exception as e:
                print("[WS LOOP ERROR]", e)
                await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBridge stopped.")