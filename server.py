#!/usr/bin/env python3
"""Mon nœud Meshtastic — contrôle web d'un nœud branché en USB (port 8089)."""
import base64
import getpass
import hashlib
import hmac
import sys
import collections
import glob
import json
import os
import secrets
import signal
import sqlite3
import threading
import time
import traceback
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import meshtastic.serial_interface
from google.protobuf.json_format import MessageToDict
from meshtastic.protobuf import config_pb2, channel_pb2, portnums_pb2, mesh_pb2
from pubsub import pub

APP = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(APP, "data")
DB = os.path.join(DATA, "node.db")
PORT = int(os.environ.get("NODE_PORT", "8089"))
DEV = os.environ.get("NODE_DEV")  # sinon détection automatique
os.makedirs(DATA, exist_ok=True)


def load_config():
    """config.json (local, non publié) : {"home": {"name": ..., "lat": ..., "lon": ..., "alt": ...}}"""
    try:
        with open(os.path.join(APP, "config.json")) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


CONFIG = load_config()

LoRa = config_pb2.Config.LoRaConfig
Dev = config_pb2.Config.DeviceConfig
Pos = config_pb2.Config.PositionConfig


def todict(m):
    return MessageToDict(m, preserving_proto_field_name=True)


def db():
    con = sqlite3.connect(DB, timeout=10)
    con.row_factory = sqlite3.Row
    return con


db_lock = threading.Lock()


def init_db():
    with db_lock, db() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, pkt_id INTEGER, dir TEXT,
            src TEXT, dst TEXT, channel INTEGER, text TEXT, status TEXT,
            snr REAL, rssi INTEGER, hops INTEGER, reply_id INTEGER, emoji INTEGER);
        CREATE INDEX IF NOT EXISTS ix_pkt ON messages(pkt_id);
        CREATE TABLE IF NOT EXISTS rx(
            ts REAL, node TEXT, pkt_id INTEGER, port TEXT, snr REAL, rssi INTEGER, hops INTEGER, via_mqtt INTEGER);
        CREATE INDEX IF NOT EXISTS ix_rx_node ON rx(node, ts);
        CREATE INDEX IF NOT EXISTS ix_rx_ts ON rx(ts);
        CREATE TABLE IF NOT EXISTS telem(
            ts REAL, node TEXT, battery INTEGER, voltage REAL, ch_util REAL, air_tx REAL);
        CREATE INDEX IF NOT EXISTS ix_telem ON telem(node, ts);
        CREATE TABLE IF NOT EXISTS seen_nodes(id TEXT PRIMARY KEY, first_seen REAL, alerted INTEGER);
        CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
        """)


def get_setting(k, default=None):
    with db() as con:
        r = con.execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default


def put_setting(k, v):
    with db_lock, db() as con:
        con.execute("INSERT OR REPLACE INTO settings(k,v) VALUES(?,?)", (k, json.dumps(v)))


# ---------------------------------------------------------------- notifications
NOTIF_DEFAULT = {"enabled": False, "service": "ntfy", "ntfy_server": "https://ntfy.sh", "ntfy_topic": "",
                 "ntfy_token": "", "tg_token": "", "tg_chat": "",
                 "on_dm": True, "on_channel": False, "on_new_node": True, "on_reaction": False}
SECRET_KEYS = ("ntfy_token", "tg_token")


def notif_conf():
    return {**NOTIF_DEFAULT, **(get_setting("notif", {}) or {})}


def _post_json(url, obj, headers=None):
    req = urllib.request.Request(url, json.dumps(obj).encode(), {"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=15) as r:
        r.read()


def notify_now(title, text, conf=None):
    c = conf or notif_conf()
    if c["service"] == "telegram":
        if not c["tg_token"] or not c["tg_chat"]:
            raise ValueError("jeton du bot et chat_id Telegram requis")
        _post_json("https://api.telegram.org/bot%s/sendMessage" % c["tg_token"],
                   {"chat_id": c["tg_chat"], "text": "%s\n%s" % (title, text)})
    else:
        if not c["ntfy_topic"]:
            raise ValueError("sujet ntfy requis")
        h = {"Authorization": "Bearer " + c["ntfy_token"]} if c["ntfy_token"] else {}
        _post_json(c["ntfy_server"].rstrip("/") + "/",
                   {"topic": c["ntfy_topic"], "title": title, "message": text, "tags": ["satellite_antenna"]}, h)


def notify(kind, title, text):
    c = notif_conf()
    if not c["enabled"] or not c.get("on_" + kind):
        return

    def run():
        try:
            notify_now(title, text, c)
        except Exception as e:
            print("Notification échouée :", e, flush=True)
    threading.Thread(target=run, daemon=True).start()


def send_packet(it, data, dest, port, want_ack=False, on_resp=None, channel=0, reply_id=None, emoji=False, hop_limit=None):
    """Comme sendData, mais permet le champ emoji (réactions)."""
    mp = mesh_pb2.MeshPacket()
    mp.channel = int(channel)
    mp.decoded.payload = data
    mp.decoded.portnum = port
    mp.id = it._generatePacketId()
    if reply_id:
        mp.decoded.reply_id = int(reply_id)
    if emoji:
        mp.decoded.emoji = 1
    if on_resp is not None:
        it._addResponseHandler(mp.id, on_resp, ackPermitted=True)
    return it._sendPacket(mp, dest, wantAck=want_ack, hopLimit=hop_limit)


def find_device():
    if DEV:
        return DEV
    c = sorted(glob.glob("/dev/serial/by-id/*")) + sorted(glob.glob("/dev/ttyUSB*")) + sorted(glob.glob("/dev/ttyACM*"))
    return c[0] if c else None


class Node:
    def __init__(self):
        self.iface = None
        self.lock = threading.RLock()
        self.connected = False
        self.error = None
        self.dev = None
        self.since = None
        self.packets = collections.deque(maxlen=400)
        self.pkt_seq = 0
        self.traces = {}  # dest -> résultat
        self.events = collections.deque(maxlen=100)
        self.ev_seq = 0
        self.rt = {"running": False, "results": []}
        self.boot_done = False
        self.stop = False
        threading.Thread(target=self.sampler, daemon=True).start()
        pub.subscribe(self.on_receive, "meshtastic.receive")
        pub.subscribe(self.on_lost, "meshtastic.connection.lost")
        threading.Thread(target=self.loop, daemon=True).start()

    # ------------------------------------------------------------- connexion
    def loop(self):
        while not self.stop:
            if not self.connected:
                self.connect()
            time.sleep(5)

    def connect(self):
        dev = find_device()
        if not dev:
            self.error = "Aucun appareil USB trouvé (ESP32 débranché ?)"
            return
        try:
            self.dev = dev
            self.error = None
            print("Connexion à", dev, flush=True)
            iface = meshtastic.serial_interface.SerialInterface(devPath=dev)
            with self.lock:
                self.iface = iface
                self.connected = True
                self.since = time.time()
            print("Connecté :", iface.getLongName(), flush=True)
            self.bootstrap_seen()
        except PermissionError:
            self.error = "Accès refusé à %s : lance « bash ~/mesh-node/install.sh » (groupe dialout)" % dev
        except Exception as e:
            msg = str(e) or e.__class__.__name__
            if "Permission denied" in msg:
                msg = "Accès refusé à %s : lance « bash ~/mesh-node/install.sh » (groupe dialout)" % dev
            elif "Resource busy" in msg or "lock" in msg.lower():
                msg = "Port %s déjà utilisé par une autre application" % dev
            self.error = msg
            print("Échec connexion :", msg, flush=True)
            self._close()

    def _close(self):
        with self.lock:
            it, self.iface = self.iface, None
            self.connected = False
        if it:
            try:
                it.close()
            except Exception:
                pass

    def on_lost(self, interface=None, **kw):
        if interface is self.iface:
            print("Connexion perdue", flush=True)
            self.error = "Connexion perdue — reconnexion…"
            threading.Thread(target=self._close, daemon=True).start()

    def reconnect(self):
        self._close()

    def my_id(self):
        it = self.iface
        return "!%08x" % it.myInfo.my_node_num if it and it.myInfo else None

    def event(self, kind, **kw):
        self.ev_seq += 1
        self.events.append({"seq": self.ev_seq, "ts": time.time(), "kind": kind, **kw})

    def bootstrap_seen(self):
        """Premier lancement : les nœuds déjà connus ne déclenchent pas d'alerte."""
        if not get_setting("seen_boot"):
            with db_lock, db() as con:
                for n in (self.iface.nodesByNum or {}).values():
                    con.execute("INSERT OR IGNORE INTO seen_nodes VALUES(?,?,1)",
                                ("!%08x" % n["num"], n.get("lastHeard") or time.time()))
            put_setting("seen_boot", True)
        self.boot_done = True

    def check_new_node(self, nid):
        if not self.boot_done or not nid or nid == self.my_id() or nid == "!ffffffff":
            return
        with db_lock, db() as con:
            if con.execute("SELECT 1 FROM seen_nodes WHERE id=?", (nid,)).fetchone():
                return
            con.execute("INSERT INTO seen_nodes VALUES(?,?,1)", (nid, time.time()))
        # laisser le temps aux infos du nœud d'arriver pour avoir un nom
        def later():
            time.sleep(20)
            name = self.node_name(int(nid[1:], 16))
            self.event("new_node", id=nid, name=name)
            notify("new_node", "Nouveau nœud", "%s (%s) est apparu dans le secteur" % (name, nid))
        threading.Thread(target=later, daemon=True).start()

    def sampler(self):
        """Télémétrie de mon nœud toutes les 5 min + ménage quotidien."""
        last_clean = 0
        while not self.stop:
            try:
                if self.connected and self.iface:
                    m = (self.iface.getMyNodeInfo() or {}).get("deviceMetrics", {})
                    if m:
                        with db_lock, db() as con:
                            con.execute("INSERT INTO telem VALUES(?,?,?,?,?,?)",
                                        (time.time(), self.my_id(), m.get("batteryLevel"), m.get("voltage"),
                                         m.get("channelUtilization"), m.get("airUtilTx")))
                if time.time() - last_clean > 86400:
                    lim = time.time() - 90 * 86400
                    with db_lock, db() as con:
                        con.execute("DELETE FROM rx WHERE ts<?", (lim,))
                        con.execute("DELETE FROM telem WHERE ts<?", (lim,))
                    last_clean = time.time()
            except Exception:
                traceback.print_exc()
            for _ in range(300):
                if self.stop:
                    return
                time.sleep(1)

    # ------------------------------------------------------------- réception
    def on_receive(self, packet, interface=None):
        try:
            d = packet.get("decoded", {}) or {}
            port = d.get("portnum", "CHIFFRÉ" if "encrypted" in packet else "?")
            hops = (packet["hopStart"] - packet.get("hopLimit", 0)) if packet.get("hopStart") else None
            self.pkt_seq += 1
            summary = self.summarize(port, d)
            self.packets.append({
                "seq": self.pkt_seq, "ts": time.time(), "from": packet.get("fromId") or "!%08x" % packet.get("from", 0),
                "to": packet.get("toId"), "port": port, "channel": packet.get("channel", 0),
                "snr": packet.get("rxSnr"), "rssi": packet.get("rxRssi"), "hops": hops,
                "via_mqtt": packet.get("viaMqtt", False), "summary": summary,
            })
            src = packet.get("fromId") or "!%08x" % packet.get("from", 0)
            if src != self.my_id():
                with db_lock, db() as con:
                    con.execute("INSERT INTO rx VALUES(?,?,?,?,?,?,?,?)",
                                (time.time(), src, packet.get("id"), port, packet.get("rxSnr"), packet.get("rxRssi"),
                                 hops, 1 if packet.get("viaMqtt") else 0))
                self.check_new_node(src)
            if port == "TELEMETRY_APP" and "deviceMetrics" in d.get("telemetry", {}):
                m = d["telemetry"]["deviceMetrics"]
                with db_lock, db() as con:
                    con.execute("INSERT INTO telem VALUES(?,?,?,?,?,?)",
                                (time.time(), src, m.get("batteryLevel"), m.get("voltage"),
                                 m.get("channelUtilization"), m.get("airUtilTx")))
            if port == "TEXT_MESSAGE_APP":
                with db_lock, db() as con:
                    con.execute("""INSERT INTO messages(ts,pkt_id,dir,src,dst,channel,text,status,snr,rssi,hops,reply_id,emoji)
                                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (time.time(), packet.get("id"), "in", packet.get("fromId"), packet.get("toId"),
                                 packet.get("channel", 0), d.get("text", ""), "reçu", packet.get("rxSnr"),
                                 packet.get("rxRssi"), hops, d.get("replyId"), d.get("emoji")))
                if src != self.my_id():
                    name, txt = self.node_name(packet.get("from", 0)), d.get("text", "")
                    dm = packet.get("toId") == self.my_id()
                    if d.get("emoji"):
                        notify("reaction", "Réaction de %s" % name, txt)
                    elif dm:
                        notify("dm", "✉️ %s" % name, txt)
                    else:
                        notify("channel", "# canal %s — %s" % (packet.get("channel", 0), name), txt)
            if port == "TRACEROUTE_APP" and "traceroute" in d:
                self.traces[packet.get("fromId")] = {"ts": time.time(), "status": "ok", **self.fmt_trace(packet)}
        except Exception:
            traceback.print_exc()

    def summarize(self, port, d):
        if port == "TEXT_MESSAGE_APP":
            return d.get("text", "")
        if port == "POSITION_APP":
            p = d.get("position", {})
            if "latitude" in p:
                return "Position %.5f, %.5f%s" % (p["latitude"], p["longitude"], " alt %s m" % p["altitude"] if "altitude" in p else "")
            return "Position"
        if port == "NODEINFO_APP":
            u = d.get("user", {})
            return "Infos nœud : %s (%s) %s" % (u.get("longName", ""), u.get("shortName", ""), u.get("hwModel", ""))
        if port == "TELEMETRY_APP":
            t = d.get("telemetry", {})
            if "deviceMetrics" in t:
                m = t["deviceMetrics"]
                b = m.get("batteryLevel")
                return "Télémétrie : %s, %.2f V" % ("secteur" if b and b > 100 else "batterie %s %%" % b, m.get("voltage", 0))
            if "environmentMetrics" in t:
                m = t["environmentMetrics"]
                return "Environnement : " + ", ".join("%s=%s" % (k, round(v, 1) if isinstance(v, float) else v) for k, v in m.items())
            return "Télémétrie"
        if port == "ROUTING_APP":
            return "Routage : " + str(d.get("routing", {}).get("errorReason", "ACK"))
        if port == "TRACEROUTE_APP":
            return "Traceroute"
        return port

    def fmt_trace(self, p):
        t = p["decoded"]["traceroute"]
        name = lambda n: self.node_name(n)
        snr = lambda v: None if v in (None, -128) else v / 4
        me = self.iface.myInfo.my_node_num if self.iface else 0
        fwd = [me] + t.get("route", []) + [p["from"]]
        back = [p["from"]] + t.get("routeBack", []) + [me]
        nid = lambda n: "!%08x" % n
        return {"forward_ids": [nid(n) for n in fwd], "back_ids": [nid(n) for n in back],
                "forward": [name(n) for n in fwd], "snr_forward": [snr(v) for v in t.get("snrTowards", [])],
                "back": [name(n) for n in back], "snr_back": [snr(v) for v in t.get("snrBack", [])]}

    def node_name(self, num):
        if self.iface and self.iface.nodesByNum and num in self.iface.nodesByNum:
            u = self.iface.nodesByNum[num].get("user", {})
            return u.get("longName") or u.get("id") or "!%08x" % num
        return "!%08x" % num

    # ------------------------------------------------------------- lecture
    def need(self):
        if not self.connected or not self.iface:
            raise RuntimeError(self.error or "Nœud non connecté")
        return self.iface

    def status(self):
        out = {"connected": self.connected, "error": self.error, "dev": self.dev or find_device(), "since": self.since,
               "home": CONFIG.get("home"), "sdr_port": SDR_PORT if SDR else None}
        if not self.connected:
            return out
        it = self.iface
        ln = it.localNode
        me = it.getMyNodeInfo() or {}
        cfg = ln.localConfig
        out["me"] = {"num": it.myInfo.my_node_num, "id": me.get("user", {}).get("id"),
                     "long_name": it.getLongName(), "short_name": it.getShortName(),
                     "hw": me.get("user", {}).get("hwModel"), "metrics": me.get("deviceMetrics", {}),
                     "position": me.get("position", {}),
                     "firmware": getattr(getattr(it, "metadata", None), "firmware_version", None)}
        out["lora"] = {"region": LoRa.RegionCode.Name(cfg.lora.region),
                       "modem_preset": LoRa.ModemPreset.Name(cfg.lora.modem_preset),
                       "use_preset": cfg.lora.use_preset, "hop_limit": cfg.lora.hop_limit,
                       "tx_power": cfg.lora.tx_power, "tx_enabled": cfg.lora.tx_enabled,
                       "channel_num": cfg.lora.channel_num}
        out["device"] = {"role": Dev.Role.Name(cfg.device.role)}
        out["position"] = {"fixed_position": cfg.position.fixed_position,
                           "gps_mode": Pos.GpsMode.Name(cfg.position.gps_mode),
                           "broadcast_secs": cfg.position.position_broadcast_secs}
        chans = []
        for c in ln.channels or []:
            if c.role == channel_pb2.Channel.Role.DISABLED:
                continue
            psk = bytes(c.settings.psk)
            chans.append({"index": c.index, "role": channel_pb2.Channel.Role.Name(c.role),
                          "name": c.settings.name, "psk": base64.b64encode(psk).decode(),
                          "psk_kind": "aucun" if psk in (b"", b"\x00") else ("défaut" if len(psk) == 1 else "AES-%d" % (len(psk) * 8)),
                          "uplink": c.settings.uplink_enabled, "downlink": c.settings.downlink_enabled,
                          "precision": c.settings.module_settings.position_precision})
        out["channels"] = chans
        try:
            out["url"] = ln.getURL(includeAll=True)
        except Exception:
            out["url"] = None
        out["options"] = {"regions": [k for k in LoRa.RegionCode.keys()],
                          "presets": [k for k in LoRa.ModemPreset.keys()],
                          "roles": [k for k in Dev.Role.keys()],
                          "gps_modes": [k for k in Pos.GpsMode.keys()]}
        return out

    def nodes(self):
        it = self.need()
        me = it.myInfo.my_node_num
        with db() as con:
            first = dict(con.execute("SELECT id, first_seen FROM seen_nodes").fetchall())
        out = []
        for n in (it.nodesByNum or {}).values():
            u = n.get("user", {})
            p = n.get("position", {})
            m = n.get("deviceMetrics", {})
            out.append({"num": n.get("num"), "id": u.get("id") or "!%08x" % n.get("num", 0),
                        "long_name": u.get("longName"), "short_name": u.get("shortName"),
                        "hw": u.get("hwModel"), "role": u.get("role"),
                        "lat": p.get("latitude"), "lon": p.get("longitude"), "alt": p.get("altitude"),
                        "snr": n.get("snr"), "last_heard": n.get("lastHeard"), "hops": n.get("hopsAway"),
                        "battery": m.get("batteryLevel"), "voltage": m.get("voltage"),
                        "ch_util": m.get("channelUtilization"), "air_tx": m.get("airUtilTx"),
                        "favorite": n.get("isFavorite", False), "via_mqtt": n.get("viaMqtt", False),
                        "pki": bool(u.get("publicKey")), "me": n.get("num") == me,
                        "first_seen": first.get(u.get("id") or "!%08x" % n.get("num", 0))})
        out.sort(key=lambda x: (not x["me"], -(x["last_heard"] or 0)))
        return out

    # ------------------------------------------------------------- actions
    def send_text(self, text, to, channel, reply_id=None, emoji=False):
        it = self.need()
        text = text.strip()
        if not text:
            raise ValueError("message vide")
        if len(text.encode()) > 200:
            raise ValueError("message trop long (200 octets max)")
        dest = to or "^all"
        row = {}

        def on_resp(p):
            r = p.get("decoded", {}).get("routing", {}).get("errorReason", "NONE")
            if r == "NONE":
                st = "remis ✓" if dest != "^all" and p.get("fromId") == dest else "relayé ✓"
            else:
                st = "échec : " + r
            with db_lock, db() as con:
                con.execute("UPDATE messages SET status=? WHERE id=?", (st, row["id"]))

        with self.lock:
            pkt = send_packet(it, text.encode(), dest, portnums_pb2.PortNum.TEXT_MESSAGE_APP, want_ack=True,
                              on_resp=on_resp, channel=channel, reply_id=reply_id, emoji=emoji)
        with db_lock, db() as con:
            cur = con.execute("""INSERT INTO messages(ts,pkt_id,dir,src,dst,channel,text,status,reply_id,emoji)
                                 VALUES(?,?,?,?,?,?,?,?,?,?)""",
                              (time.time(), pkt.id, "out", (it.getMyNodeInfo() or {}).get("user", {}).get("id"),
                               "!ffffffff" if dest == "^all" else dest, int(channel), text, "envoyé…",
                               int(reply_id) if reply_id else None, 1 if emoji else None))
            row["id"] = cur.lastrowid
        threading.Timer(90, self._timeout, args=(row["id"],)).start()

    def _timeout(self, mid):
        with db_lock, db() as con:
            con.execute("UPDATE messages SET status='pas d''accusé' WHERE id=? AND status='envoyé…'", (mid,))

    def traceroute(self, to):
        it = self.need()
        self.traces[to] = {"ts": time.time(), "status": "en cours"}

        def on_resp(p):
            if "traceroute" in p.get("decoded", {}):
                self.traces[to] = {"ts": time.time(), "status": "ok", **self.fmt_trace(p)}
            else:
                r = p.get("decoded", {}).get("routing", {}).get("errorReason")
                self.traces[to] = {"ts": time.time(), "status": "échec : %s" % r}

        with self.lock:
            it.sendData(mesh_pb2.RouteDiscovery().SerializeToString(), destinationId=to,
                        portNum=portnums_pb2.PortNum.TRACEROUTE_APP, wantResponse=True, onResponse=on_resp,
                        channelIndex=0, hopLimit=it.localNode.localConfig.lora.hop_limit or 3)

    def set_owner(self, long_name, short_name):
        it = self.need()
        if not long_name or not short_name:
            raise ValueError("nom long et nom court requis")
        if len(short_name) > 4:
            raise ValueError("nom court : 4 caractères max")
        with self.lock:
            it.localNode.setOwner(long_name=long_name[:39], short_name=short_name)

    def set_lora(self, b):
        it = self.need()
        lc = it.localNode.localConfig.lora
        if "region" in b:
            lc.region = LoRa.RegionCode.Value(b["region"])
        if "modem_preset" in b:
            lc.modem_preset = LoRa.ModemPreset.Value(b["modem_preset"])
            lc.use_preset = True
        if "hop_limit" in b:
            h = int(b["hop_limit"])
            if not 1 <= h <= 7:
                raise ValueError("sauts : 1 à 7")
            lc.hop_limit = h
        if "tx_power" in b:
            lc.tx_power = max(0, min(30, int(b["tx_power"])))
        if "tx_enabled" in b:
            lc.tx_enabled = bool(b["tx_enabled"])
        with self.lock:
            it.localNode.writeConfig("lora")

    def set_device(self, b):
        it = self.need()
        dc = it.localNode.localConfig.device
        dc.role = Dev.Role.Value(b["role"])
        with self.lock:
            it.localNode.writeConfig("device")

    def set_position(self, b):
        it = self.need()
        with self.lock:
            if b.get("clear"):
                it.localNode.removeFixedPosition()
                return
            if "broadcast_secs" in b:
                pc = it.localNode.localConfig.position
                pc.position_broadcast_secs = int(b["broadcast_secs"])
                if "gps_mode" in b:
                    pc.gps_mode = Pos.GpsMode.Value(b["gps_mode"])
                it.localNode.writeConfig("position")
            if b.get("lat") not in (None, "") and b.get("lon") not in (None, ""):
                it.localNode.setFixedPosition(float(b["lat"]), float(b["lon"]), int(float(b.get("alt") or 0)))

    def set_channel(self, b):
        it = self.need()
        ln = it.localNode
        idx = b.get("index")
        if idx is None or idx == "":
            free = [c for c in ln.channels if c.role == channel_pb2.Channel.Role.DISABLED]
            if not free:
                raise ValueError("plus de place (8 canaux max)")
            ch = free[0]
            ch.role = channel_pb2.Channel.Role.SECONDARY
        else:
            ch = ln.channels[int(idx)]
        name = (b.get("name") or "").strip()
        if len(name.encode()) > 11:
            raise ValueError("nom de canal : 11 caractères max")
        if ch.role == channel_pb2.Channel.Role.SECONDARY and not name:
            raise ValueError("un canal secondaire doit avoir un nom")
        ch.settings.name = name
        psk = (b.get("psk") or "").strip()
        if psk == "random":
            ch.settings.psk = secrets.token_bytes(32)
        elif psk == "default":
            ch.settings.psk = b"\x01"
        elif psk == "none":
            ch.settings.psk = b"\x00"
        elif psk:
            k = base64.b64decode(psk, validate=True)
            if len(k) not in (0, 1, 16, 32):
                raise ValueError("clé : 1, 16 ou 32 octets en base64")
            ch.settings.psk = k
        if "uplink" in b:
            ch.settings.uplink_enabled = bool(b["uplink"])
        if "downlink" in b:
            ch.settings.downlink_enabled = bool(b["downlink"])
        if "precision" in b and b["precision"] not in (None, ""):
            ch.settings.module_settings.position_precision = int(b["precision"])
        with self.lock:
            ln.writeChannel(ch.index)

    def delete_channel(self, idx):
        it = self.need()
        idx = int(idx)
        if idx == 0:
            raise ValueError("le canal principal ne peut pas être supprimé")
        with self.lock:
            it.localNode.deleteChannel(idx)

    def set_url(self, url):
        it = self.need()
        with self.lock:
            it.localNode.setURL(url.strip(), addOnly=False)

    def node_action(self, b):
        it = self.need()
        act, nid = b.get("action"), b.get("id")
        with self.lock:
            if act == "favorite":
                it.localNode.setFavorite(nid)
            elif act == "unfavorite":
                it.localNode.removeFavorite(nid)
            elif act == "remove":
                it.localNode.removeNode(nid)
            elif act == "position":
                it.sendPosition(destinationId=nid, wantResponse=True)
            elif act == "telemetry":
                it.sendTelemetry(destinationId=nid, wantResponse=True)
            else:
                raise ValueError("action inconnue")

    # ------------------------------------------------------------- test de portée
    def rangetest_start(self, b):
        self.need()
        if self.rt.get("running"):
            raise ValueError("un test est déjà en cours")
        to = b.get("to")
        if not to or not to.startswith("!"):
            raise ValueError("choisis un nœud destinataire")
        interval = max(30, int(b.get("interval", 60)))
        count = max(1, min(60, int(b.get("count", 10))))
        self.rt = {"running": True, "to": to, "name": self.node_name(int(to[1:], 16)), "interval": interval,
                   "count": count, "sent": 0, "started": time.time(), "results": []}
        threading.Thread(target=self._rt_loop, args=(self.rt,), daemon=True).start()

    def rangetest_stop(self):
        self.rt["running"] = False

    def _rt_loop(self, rt):
        for i in range(1, rt["count"] + 1):
            if not rt["running"] or self.stop:
                break
            res = {"n": i, "ts": time.time(), "status": "en attente…", "snr": None, "rssi": None, "rtt": None}
            rt["results"].append(res)
            rt["sent"] = i

            def on_resp(p, res=res):
                r = p.get("decoded", {}).get("routing", {}).get("errorReason", "NONE")
                if r != "NONE":
                    res["status"] = "échec : " + r
                elif p.get("fromId") == rt["to"]:
                    res["status"] = "remis ✓"
                    res["snr"], res["rssi"] = p.get("rxSnr"), p.get("rxRssi")
                    res["rtt"] = round(time.time() - res["ts"], 1)
                elif res["status"] == "en attente…":
                    res["status"] = "relayé (pas d'accusé final)"
            try:
                with self.lock:
                    send_packet(self.need(), ("Test de portée %d/%d" % (i, rt["count"])).encode(), rt["to"],
                                portnums_pb2.PortNum.TEXT_MESSAGE_APP, want_ack=True, on_resp=on_resp)
            except Exception as e:
                res["status"] = "erreur : %s" % e
            threading.Timer(90, lambda res=res: res.update(status="perdu ✗") if res["status"] in ("en attente…", "relayé (pas d'accusé final)") else None).start()
            for _ in range(rt["interval"]):
                if not rt["running"] or self.stop or i == rt["count"]:
                    break
                time.sleep(1)
        rt["running"] = False

    # ------------------------------------------------------------- sauvegarde
    def backup(self):
        st = self.status()
        if not st.get("connected"):
            raise RuntimeError("Nœud non connecté")
        p = st["me"].get("position", {})
        return {"app": "mesh-node", "version": 1, "date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "node_id": st["me"]["id"], "hw": st["me"]["hw"], "firmware": st["me"]["firmware"],
                "owner": {"long_name": st["me"]["long_name"], "short_name": st["me"]["short_name"]},
                "lora": {k: st["lora"][k] for k in ("region", "modem_preset", "hop_limit", "tx_power", "tx_enabled")},
                "device": st["device"],
                "position": {"broadcast_secs": st["position"]["broadcast_secs"], "gps_mode": st["position"]["gps_mode"],
                             "fixed": st["position"]["fixed_position"],
                             "lat": p.get("latitude"), "lon": p.get("longitude"), "alt": p.get("altitude")},
                "channels_url": st.get("url")}

    def restore(self, b, parts):
        if b.get("app") != "mesh-node":
            raise ValueError("fichier de sauvegarde non reconnu")
        done = []
        if "channels" in parts and b.get("channels_url"):
            self.set_url(b["channels_url"]); done.append("canaux"); time.sleep(2)
        if "owner" in parts and b.get("owner"):
            self.set_owner(b["owner"]["long_name"], b["owner"]["short_name"]); done.append("nom"); time.sleep(2)
        if "position" in parts and b.get("position"):
            pos = b["position"]
            self.set_position({"broadcast_secs": pos["broadcast_secs"], "gps_mode": pos["gps_mode"]})
            if pos.get("fixed") and pos.get("lat") is not None:
                self.set_position({"lat": pos["lat"], "lon": pos["lon"], "alt": pos.get("alt") or 0})
            done.append("position"); time.sleep(2)
        if "device" in parts and b.get("device"):
            self.set_device(b["device"]); done.append("rôle"); time.sleep(2)
        if "lora" in parts and b.get("lora"):
            self.set_lora(b["lora"]); done.append("radio")
        return done

    def reboot(self):
        it = self.need()
        with self.lock:
            it.localNode.reboot(5)


# ---------------------------------------------------------------- statistiques
def history(node, hours):
    since = time.time() - hours * 3600
    with db() as con:
        rx = con.execute("SELECT ts,snr,rssi,hops FROM rx WHERE node=? AND ts>? AND via_mqtt=0 ORDER BY ts",
                         (node, since)).fetchall()
        te = con.execute("SELECT ts,battery,voltage,ch_util,air_tx FROM telem WHERE node=? AND ts>? ORDER BY ts",
                         (node, since)).fetchall()
        first = con.execute("SELECT first_seen FROM seen_nodes WHERE id=?", (node,)).fetchone()
    return {"rx": [list(r) for r in rx], "telem": [list(r) for r in te], "first_seen": first[0] if first else None}


def stats(days):
    now = time.time()
    with db() as con:
        per_day = con.execute("""SELECT date(ts,'unixepoch','localtime') d, count(DISTINCT node) n, count(*) p
                                 FROM rx WHERE ts>? GROUP BY d ORDER BY d""", (now - days * 86400,)).fetchall()
        hours = con.execute("""SELECT CAST(strftime('%H',ts,'unixepoch','localtime') AS INTEGER) h, count(*) p
                               FROM rx WHERE ts>? GROUP BY h""", (now - 7 * 86400,)).fetchall()
        direct = con.execute("""SELECT node, count(*) n, max(snr) best_snr, max(ts) last FROM rx
                                WHERE hops=0 AND via_mqtt=0 GROUP BY node""").fetchall()
        top = con.execute("""SELECT node, count(*) p FROM rx WHERE ts>? GROUP BY node ORDER BY p DESC LIMIT 10""",
                          (now - 7 * 86400,)).fetchall()
        tot = con.execute("SELECT count(*), min(ts) FROM rx").fetchone()
        seen = con.execute("SELECT count(*) FROM seen_nodes").fetchone()[0]
        new = con.execute("SELECT id, first_seen FROM seen_nodes WHERE first_seen>? ORDER BY first_seen DESC",
                          (now - 7 * 86400,)).fetchall()
        msgs = con.execute("SELECT count(*) FROM messages WHERE emoji IS NULL").fetchone()[0]
    hmap = {r["h"]: r["p"] for r in hours}
    return {"per_day": [dict(r) for r in per_day], "hours": [hmap.get(h, 0) for h in range(24)],
            "direct": [dict(r) for r in direct], "top": [dict(r) for r in top],
            "total_packets": tot[0], "since": tot[1], "nodes_seen": seen, "messages": msgs,
            "new_nodes": [dict(r) for r in new]}


SDR = CONFIG.get("lora_mesh") or {}  # appli d'écoute SDR compagnon (facultative)
SDR_DB = os.path.expanduser(SDR.get("db", ""))
SDR_PORT = int(SDR.get("port", 8088))


def compare(hours, me=None):
    since = time.time() - hours * 3600
    out = {"available": bool(SDR) and os.path.exists(SDR_DB), "configured": bool(SDR), "hours": hours}
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/api/status" % SDR_PORT, timeout=3) as r:
            s = json.loads(r.read())
        out["sdr_running"], out["sdr_preset"] = s.get("running"), (s.get("params") or {}).get("preset")
    except Exception:
        out["sdr_running"] = None
    if not out["available"]:
        return out
    c2 = sqlite3.connect("file:%s?mode=ro" % SDR_DB, uri=True, timeout=5)
    sdr = {(r[0], r[1]): (r[2], r[3]) for r in c2.execute(
        "SELECT src,pkt_id,snr,ts FROM packets WHERE crc_ok=1 AND dup_of IS NULL AND pkt_id IS NOT NULL AND ts>?", (since,))}
    c2.close()
    sdr = {k: v for k, v in sdr.items() if k[0] != me}  # mes propres émissions
    with db() as con:
        mine = {(r[0], r[1]): (r[2], r[3]) for r in con.execute(
            "SELECT node,pkt_id,snr,ts FROM rx WHERE via_mqtt=0 AND pkt_id IS NOT NULL AND ts>?", (since,))}
    if sdr:  # ne comparer que pendant la période où le SDR écoutait
        t0, t1 = min(v[1] for v in sdr.values()) - 60, max(v[1] for v in sdr.values()) + 60
        mine = {k: v for k, v in mine.items() if t0 <= v[1] <= t1}
        out["window"] = [t0, t1]
    both = set(sdr) & set(mine)
    per = {}
    for k in set(sdr) | set(mine):
        e = per.setdefault(k[0], {"node": k[0], "heltec": 0, "sdr": 0, "both": 0, "snr_h": [], "snr_s": []})
        if k in mine:
            e["heltec"] += 1
        if k in sdr:
            e["sdr"] += 1
        if k in both:
            e["both"] += 1
            if mine[k][0] is not None and sdr[k][0] is not None:
                e["snr_h"].append(mine[k][0]); e["snr_s"].append(sdr[k][0])
    avg = lambda l: round(sum(l) / len(l), 1) if l else None
    rows = []
    for e in per.values():
        e["snr_h"], e["snr_s"] = avg(e.pop("snr_h")), avg(e.pop("snr_s"))
        rows.append(e)
    rows.sort(key=lambda e: -(e["heltec"] + e["sdr"]))
    pairs = [(mine[k][0], sdr[k][0]) for k in both if mine[k][0] is not None and sdr[k][0] is not None]
    out.update({"heltec": len(mine), "sdr": len(sdr), "both": len(both), "nodes": rows,
                "avg_snr_heltec": avg([a for a, _ in pairs]), "avg_snr_sdr": avg([b for _, b in pairs])})
    return out


# ---------------------------------------------------------------- authentification
AUTH_FILE = os.path.join(DATA, "auth.json")
SESS_FILE = os.path.join(DATA, "sessions.json")
SESS_TTL = 30 * 86400
COOKIE = "mnsess"
auth_lock = threading.Lock()
fails = {}  # ip -> [nb, verrou_jusqu_a]


def _hash(pw, salt, it):
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(salt), it).hex()


def set_password(user, pw):
    salt = secrets.token_hex(16)
    it = 300000
    with open(AUTH_FILE, "w") as f:
        json.dump({"user": user, "salt": salt, "iter": it, "hash": _hash(pw, salt, it)}, f)
    os.chmod(AUTH_FILE, 0o600)
    save_sessions({})  # déconnecte toutes les sessions


def load_auth():
    try:
        with open(AUTH_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def load_sessions():
    try:
        with open(SESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_sessions(s):
    tmp = SESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, SESS_FILE)


SESSIONS = {}


def check_login(ip, user, pw):
    now = time.time()
    with auth_lock:
        n, until = fails.get(ip, [0, 0])
        if until > now:
            return "Trop de tentatives, réessayez dans %d min" % (int((until - now) / 60) + 1)
    a = load_auth()
    ok = bool(a) and hmac.compare_digest(user, a["user"]) & hmac.compare_digest(_hash(pw, a["salt"], a["iter"]), a["hash"])
    with auth_lock:
        if ok:
            fails.pop(ip, None)
            return None
        n += 1
        fails[ip] = [n, now + 300 if n >= 5 else 0]
    time.sleep(1)
    return "Identifiant ou mot de passe incorrect"


def new_session():
    tok = secrets.token_urlsafe(32)
    with auth_lock:
        now = time.time()
        for k in [k for k, v in SESSIONS.items() if v < now]:
            del SESSIONS[k]
        SESSIONS[tok] = now + SESS_TTL
        save_sessions(SESSIONS)
    return tok


def drop_session(tok):
    with auth_lock:
        if SESSIONS.pop(tok, None):
            save_sessions(SESSIONS)


def valid_session(tok):
    with auth_lock:
        return bool(tok) and SESSIONS.get(tok, 0) > time.time()


LOGIN_HTML = """<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Connexion · Mon nœud</title><link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>:root{--bg:#f4f5f7;--card:#fff;--fg:#1d2330;--muted:#6b7385;--line:#e2e5eb;--accent:#0f8b6f;--accent-fg:#fff;--danger:#c8382e}
@media (prefers-color-scheme: dark){:root{--bg:#12151b;--card:#1b1f27;--fg:#e6e9ef;--muted:#949cad;--line:#2b313c;--accent:#2bb594;--accent-fg:#06110e;--danger:#e5574c}}
*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--fg);
font:15px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:16px}
form{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:24px;width:100%;max-width:340px}
h1{font-size:20px;margin:0 0 4px}p{color:var(--muted);margin:0 0 16px;font-size:14px}
label{display:block;font-size:13px;color:var(--muted);margin:12px 0 4px}
input{width:100%;font:inherit;padding:9px 10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--fg)}
button{width:100%;margin-top:18px;font:inherit;font-weight:600;padding:10px;border-radius:8px;border:0;background:var(--accent);color:var(--accent-fg);cursor:pointer}
#err{color:var(--danger);font-size:14px;margin-top:12px;min-height:1.2em}</style></head><body>
<form id="f"><h1>📟 Mon nœud</h1><p>Connexion requise</p>
<label for="u">Identifiant</label><input id="u" autocomplete="username" required autofocus>
<label for="p">Mot de passe</label><input id="p" type="password" autocomplete="current-password" required>
<button>Se connecter</button><div id="err"></div></form>
<script>document.getElementById('f').onsubmit=async e=>{e.preventDefault();const err=document.getElementById('err');err.textContent='';
try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({user:u.value,password:p.value})});const j=await r.json();
if(j.ok)location.replace('/');else err.textContent=j.error||'Erreur';}catch(x){err.textContent=x.message}};</script></body></html>"""


def save_notif(b):
    c = notif_conf()
    for k in NOTIF_DEFAULT:
        if k in b:
            if k in SECRET_KEYS and not b[k]:
                continue  # vide = inchangé
            c[k] = b[k]
    if b.get("clear_secrets"):
        for k in SECRET_KEYS:
            c[k] = ""
    put_setting("notif", c)


NODE = None


class Handler(BaseHTTPRequestHandler):
    server_version = "MeshNode/1.0"

    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        b = json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _token(self):
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == COOKIE:
                return v
        return None

    def _ip(self):
        return self.client_address[0]

    def _authed(self):
        return valid_session(self._token())

    def _deny(self, path):
        if path.startswith("/api/"):
            return self._json({"ok": False, "error": "non connecté", "auth": True}, 401)
        self.send_response(302)
        self.send_header("Location", "/login")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/login":
            b = LOGIN_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            return self.wfile.write(b)
        if u.path in ("/favicon.svg", "/favicon.ico"):
            with open(os.path.join(APP, "static", "favicon.svg"), "rb") as f:
                b = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Cache-Control", "max-age=86400")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            return self.wfile.write(b)
        if not self._authed():
            return self._deny(u.path)
        try:
            if u.path in ("/", "/index.html"):
                with open(os.path.join(APP, "static", "index.html"), "rb") as f:
                    b = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
            elif u.path == "/api/status":
                self._json(NODE.status())
            elif u.path == "/api/nodes":
                self._json(NODE.nodes() if NODE.connected else [])
            elif u.path == "/api/messages":
                after = int(q.get("after", 0))
                with db() as con:
                    if after:
                        rows = con.execute("SELECT * FROM messages WHERE id>? ORDER BY id", (after,)).fetchall()
                    else:
                        rows = con.execute("SELECT * FROM (SELECT * FROM messages ORDER BY id DESC LIMIT 500) ORDER BY id").fetchall()
                    # statuts récents (accusés) : renvoyés à chaque appel
                    upd = con.execute("SELECT id,status FROM messages WHERE dir='out' AND ts>?", (time.time() - 600,)).fetchall()
                self._json({"messages": [dict(r) for r in rows], "status": {r["id"]: r["status"] for r in upd}})
            elif u.path == "/api/packets":
                after = int(q.get("after", 0))
                self._json([p for p in NODE.packets if p["seq"] > after])
            elif u.path == "/api/traces":
                self._json(NODE.traces)
            elif u.path == "/api/events":
                after = int(q.get("after", 0))
                self._json({"seq": NODE.ev_seq, "events": [e for e in NODE.events if e["seq"] > after]})
            elif u.path == "/api/history":
                self._json(history(q["node"], min(float(q.get("hours", 24)), 24 * 90)))
            elif u.path == "/api/stats":
                self._json(stats(min(int(q.get("days", 14)), 90)))
            elif u.path == "/api/compare":
                self._json(compare(min(float(q.get("hours", 24)), 24 * 30), NODE.my_id()))
            elif u.path == "/api/rangetest":
                self._json(NODE.rt)
            elif u.path == "/api/notif":
                c = notif_conf()
                self._json({**{k: v for k, v in c.items() if k not in SECRET_KEYS},
                            **{"has_" + k: bool(c[k]) for k in SECRET_KEYS}})
            elif u.path == "/api/backup":
                bj = json.dumps(NODE.backup(), ensure_ascii=False, indent=2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="noeud-%s.json"' % time.strftime("%Y%m%d"))
                self.send_header("Content-Length", str(len(bj)))
                self.end_headers()
                self.wfile.write(bj)
            else:
                self._json({"error": "introuvable"}, 404)
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def do_POST(self):
        u = urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length") or 0)
            b = json.loads(self.rfile.read(n) or b"{}") if n else {}
            if u.path == "/api/login":
                err = check_login(self._ip(), str(b.get("user", "")), str(b.get("password", "")))
                if err:
                    return self._json({"ok": False, "error": err}, 401)
                tok = new_session()
                bj = json.dumps({"ok": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Set-Cookie", "%s=%s; Path=/; Max-Age=%d; HttpOnly; SameSite=Strict" % (COOKIE, tok, SESS_TTL))
                self.send_header("Content-Length", str(len(bj)))
                self.end_headers()
                return self.wfile.write(bj)
            if u.path == "/api/logout":
                drop_session(self._token())
                bj = b'{"ok": true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Set-Cookie", "%s=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict" % COOKIE)
                self.send_header("Content-Length", str(len(bj)))
                self.end_headers()
                return self.wfile.write(bj)
            if not self._authed():
                return self._deny(u.path)
            r = {
                "/api/send": lambda: NODE.send_text(b.get("text", ""), b.get("to"), b.get("channel", 0),
                                                    b.get("reply_id"), bool(b.get("emoji"))),
                "/api/rangetest/start": lambda: NODE.rangetest_start(b),
                "/api/rangetest/stop": lambda: NODE.rangetest_stop(),
                "/api/notif": lambda: save_notif(b),
                "/api/notif/test": lambda: notify_now("Test", "Les notifications de « Mon nœud » fonctionnent ✓"),
                "/api/traceroute": lambda: NODE.traceroute(b["to"]),
                "/api/owner": lambda: NODE.set_owner(b.get("long_name", "").strip(), b.get("short_name", "").strip()),
                "/api/lora": lambda: NODE.set_lora(b),
                "/api/device": lambda: NODE.set_device(b),
                "/api/position": lambda: NODE.set_position(b),
                "/api/channel": lambda: NODE.set_channel(b),
                "/api/channel/delete": lambda: NODE.delete_channel(b["index"]),
                "/api/seturl": lambda: NODE.set_url(b["url"]),
                "/api/node": lambda: NODE.node_action(b),
                "/api/reboot": lambda: NODE.reboot(),
                "/api/reconnect": lambda: NODE.reconnect(),
            }.get(u.path)
            if u.path == "/api/restore":
                return self._json({"ok": True, "done": NODE.restore(b.get("backup") or {}, b.get("parts") or [])})
            if not r:
                return self._json({"error": "introuvable"}, 404)
            r()
            self._json({"ok": True})
        except Exception as e:
            traceback.print_exc()
            self._json({"ok": False, "error": str(e)}, 400)


def cli_passwd():
    user = input("Identifiant [admin] : ").strip() or "admin"
    pw = getpass.getpass("Nouveau mot de passe : ")
    if len(pw) < 8 or pw != getpass.getpass("Confirmer : "):
        sys.exit("Mot de passe trop court (8 min.) ou différent.")
    set_password(user, pw)
    print("Mot de passe enregistré. Redémarrez : sudo systemctl restart mesh-node")


def main():
    global NODE, SESSIONS
    if len(sys.argv) > 1 and sys.argv[1] == "passwd":
        return cli_passwd()
    if not load_auth():
        pw = secrets.token_urlsafe(9)
        set_password("admin", pw)
        with open(os.path.join(DATA, "mot-de-passe-initial.txt"), "w") as f:
            f.write("identifiant: admin\nmot de passe: %s\n" % pw)
        os.chmod(os.path.join(DATA, "mot-de-passe-initial.txt"), 0o600)
        print("Identifiants initiaux : admin / %s" % pw, flush=True)
    SESSIONS = {k: v for k, v in load_sessions().items() if v > time.time()}
    init_db()
    NODE = Node()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)

    def bye(*_):
        NODE.stop = True
        NODE._close()
        os._exit(0)
    signal.signal(signal.SIGTERM, bye)
    signal.signal(signal.SIGINT, bye)
    print("Mon nœud Meshtastic sur http://0.0.0.0:%d" % PORT, flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
