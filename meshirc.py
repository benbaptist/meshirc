#!/usr/bin/env python3
"""
MeshIRC — MeshCore ↔ IRC Bridge

Connects to a MeshCore node via TCP and bridges messages to/from an IRC channel.
Automatically sets up the target channel on the MeshCore node on startup.
"""

import json
import logging
import os
import socket
import ssl
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import irc.bot
import irc.client
import irc.connection
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class ChannelType(Enum):
    PUBLIC = "public"
    PRIVATE = "private"


@dataclass
class Config:
    meshcore_host: str = "192.168.1.100"
    meshcore_port: int = 5000
    meshcore_node_name: str = "meshirc-bridge"
    irc_host: str = "irc.libera.chat"
    irc_port: int = 6667
    irc_nick: str = "MeshBridge"
    irc_realname: str = "MeshCore IRC Bridge"
    irc_use_tls: bool = False
    irc_channel: str = "#meshnet"
    meshcore_channel: str = "irc-bridge"
    meshcore_channel_type: ChannelType = ChannelType.PUBLIC
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        return cls(
            meshcore_host=os.getenv("MESHCORE_HOST", cls.meshcore_host),
            meshcore_port=int(os.getenv("MESHCORE_PORT", str(cls.meshcore_port))),
            meshcore_node_name=os.getenv("MESHCORE_NODE_NAME", cls.meshcore_node_name),
            irc_host=os.getenv("IRC_HOST", cls.irc_host),
            irc_port=int(os.getenv("IRC_PORT", str(cls.irc_port))),
            irc_nick=os.getenv("IRC_NICK", cls.irc_nick),
            irc_realname=os.getenv("IRC_REALNAME", cls.irc_realname),
            irc_use_tls=os.getenv("IRC_USE_TLS", "false").lower() == "true",
            irc_channel=os.getenv("IRC_CHANNEL", cls.irc_channel),
            meshcore_channel=os.getenv("MESHCORE_CHANNEL", cls.meshcore_channel),
            meshcore_channel_type=ChannelType(
                os.getenv("MESHCORE_CHANNEL_TYPE", cls.meshcore_channel_type.value)
            ),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )


# ---------------------------------------------------------------------------
# MeshCore TCP Client
# ---------------------------------------------------------------------------

class MeshCoreClient:
    """
    TCP client for a MeshCore node.

    Assumes a JSON-line protocol:
      - One JSON object per line, delimited by \n.
      - Commands sent:  {"cmd": "...", ...}
      - Events received: {"event": "...", ...}
      - Responses:       {"id": <seq>, "ok": true/false, ...}

    If the actual MeshCore protocol differs, this class is the single place
    to adapt.
    """

    RECONNECT_DELAY = 5.0
    PING_INTERVAL = 30.0

    def __init__(self, config: Config, on_message, on_event):
        self._config = config
        self._on_message = on_message       # (channel, sender, text)
        self._on_event = on_event           # (event_type, data)
        self._sock: Optional[socket.socket] = None
        self._buffer = b""
        self._seq = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._log = logging.getLogger("meshcore")

    # -- public API ----------------------------------------------------------

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="meshcore-tcp")
        self._thread.start()

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass

    def send_message(self, channel: str, text: str):
        """Send a text message to a MeshCore channel."""
        self._send_cmd({
            "cmd": "send_message",
            "channel": channel,
            "text": text,
        })

    def create_channel(self, channel: str, channel_type: ChannelType):
        """Create (or ensure) a channel on the MeshCore node."""
        self._send_cmd({
            "cmd": "create_channel",
            "channel": channel,
            "type": channel_type.value,
        })

    def join_channel(self, channel: str):
        """Subscribe to messages from a channel."""
        self._send_cmd({
            "cmd": "join_channel",
            "channel": channel,
        })

    # -- internals -----------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _send_cmd(self, obj: dict):
        with self._lock:
            if self._sock is None:
                self._log.warning("Cannot send — not connected")
                return
            obj["id"] = self._next_seq()
            line = json.dumps(obj, ensure_ascii=False) + "\n"
            try:
                self._sock.sendall(line.encode("utf-8"))
            except OSError as exc:
                self._log.error("Send failed: %s", exc)

    def _connect(self) -> Optional[socket.socket]:
        self._log.info(
            "Connecting to MeshCore node at %s:%d ...",
            self._config.meshcore_host,
            self._config.meshcore_port,
        )
        sock = socket.create_connection(
            (self._config.meshcore_host, self._config.meshcore_port),
            timeout=10,
        )
        sock.settimeout(1.0)  # non-blocking-ish for the read loop
        # Send handshake / identity
        handshake = {
            "cmd": "identify",
            "node_name": self._config.meshcore_node_name,
            "client": "meshirc",
        }
        sock.sendall((json.dumps(handshake) + "\n").encode("utf-8"))
        return sock

    def _run(self):
        last_ping = time.monotonic()
        while self._running:
            if self._sock is None:
                try:
                    self._sock = self._connect()
                    self._buffer = b""
                    self._log.info("Connected to MeshCore node.")
                    # Set up the target channel
                    self.create_channel(
                        self._config.meshcore_channel,
                        self._config.meshcore_channel_type,
                    )
                    self.join_channel(self._config.meshcore_channel)
                except OSError as exc:
                    self._log.error("Connection failed: %s — retrying in %ds", exc, self.RECONNECT_DELAY)
                    time.sleep(self.RECONNECT_DELAY)
                    continue

            try:
                data = self._sock.recv(4096)
                if not data:
                    self._log.warning("MeshCore node closed connection.")
                    self._close_sock()
                    continue
                self._buffer += data
                self._process_buffer()
            except socket.timeout:
                pass  # expected — lets us check _running
            except OSError as exc:
                self._log.error("Read error: %s", exc)
                self._close_sock()
                time.sleep(self.RECONNECT_DELAY)

            # Periodic ping
            now = time.monotonic()
            if now - last_ping >= self.PING_INTERVAL:
                self._send_cmd({"cmd": "ping"})
                last_ping = now

    def _close_sock(self):
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _process_buffer(self):
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                self._log.warning("Invalid JSON from node: %s", line[:200])
                continue
            self._dispatch(obj)

    def _dispatch(self, obj: dict):
        event_type = obj.get("event")
        if event_type == "message":
            self._on_message(
                obj.get("channel", ""),
                obj.get("from", "unknown"),
                obj.get("text", ""),
            )
        elif event_type:
            self._on_event(event_type, obj)
        # Responses to commands are logged but otherwise ignored for now
        elif "id" in obj:
            if not obj.get("ok", True):
                self._log.warning("Command failed: %s", obj.get("error", "unknown error"))


# ---------------------------------------------------------------------------
# IRC Bot
# ---------------------------------------------------------------------------

class IrcBridgeBot(irc.bot.SingleServerIRCBot):
    """
    IRC bot that bridges messages to/from a MeshCore node.
    """

    def __init__(self, config: Config, meshcore: MeshCoreClient):
        self._config = config
        self._meshcore = meshcore
        self._log = logging.getLogger("irc")

        factory = irc.connection.Factory(
            wrapper=ssl.SSLContext().wrap_socket if config.irc_use_tls else None,
        )
        server = irc.bot.ServerSpec(
            config.irc_host,
            config.irc_port,
        )
        kwargs = {}
        if config.irc_use_tls:
            kwargs["connect_factory"] = factory
        super().__init__(
            [server],
            config.irc_nick,
            config.irc_realname,
            **kwargs,
        )

    # -- IRC event handlers --------------------------------------------------

    def on_welcome(self, conn: irc.client.ServerConnection, event):
        self._log.info("Connected to IRC server %s", self._config.irc_host)
        conn.join(self._config.irc_channel)
        self._log.info("Joined IRC channel %s", self._config.irc_channel)

    def on_pubmsg(self, conn: irc.client.ServerConnection, event):
        """Public message in the channel — relay to mesh."""
        nick = irc.client.NickMask(event.source).nick
        text = event.arguments[0]
        if nick == self._config.irc_nick:
            return  # don't echo our own messages
        self._meshcore.send_message(self._config.meshcore_channel, f"<{nick}> {text}")

    def on_privmsg(self, conn: irc.client.ServerConnection, event):
        """Private message — treat as mesh relay if from our channel context."""
        nick = irc.client.NickMask(event.source).nick
        text = event.arguments[0]
        if nick == self._config.irc_nick:
            return
        self._meshcore.send_message(self._config.meshcore_channel, f"[PM] <{nick}> {text}")

    def on_disconnect(self, conn: irc.client.ServerConnection, event):
        self._log.warning("Disconnected from IRC — will auto-reconnect")

    # -- send to IRC ---------------------------------------------------------

    def relay_to_irc(self, sender: str, text: str):
        """Called by the bridge when a mesh message arrives."""
        conn = self.connection
        if conn and conn.is_connected():
            formatted = f"\x0303[{sender}]\x03 {text}"
            conn.privmsg(self._config.irc_channel, formatted)


# ---------------------------------------------------------------------------
# Bridge Orchestrator
# ---------------------------------------------------------------------------

class Bridge:
    """
    Wires MeshCore ↔ IRC together.
    """

    def __init__(self, config: Config):
        self._config = config
        self._meshcore = MeshCoreClient(
            config,
            on_message=self._on_mesh_message,
            on_event=self._on_mesh_event,
        )
        self._irc = IrcBridgeBot(config, self._meshcore)

    def start(self):
        self._meshcore.start()
        # IRC bot runs in its own thread via irc.client internals
        self._log.info("Bridge starting — meshcore → IRC (%s) ↔ mesh (%s)",
                        self._config.irc_channel, self._config.meshcore_channel)
        self._irc.start()

    def _on_mesh_message(self, channel: str, sender: str, text: str):
        if channel != self._config.meshcore_channel:
            return
        self._log.info("[mesh → irc] %s: %s", sender, text)
        self._irc.relay_to_irc(sender, text)

    def _on_mesh_event(self, event_type: str, data: dict):
        self._log.debug("MeshCore event '%s': %s", event_type, data)

    @property
    def _log(self):
        return logging.getLogger("bridge")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    config = Config.from_env()
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)-8s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Squelch noisy IRC protocol debug
    logging.getLogger("irc.client").setLevel(logging.WARNING)

    bridge = Bridge(config)
    bridge.start()

    # Keep the main thread alive; IRC library handles its own loop in a daemon
    # thread, and MeshCore runs in a daemon thread — so we just block on signal.
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Shutting down...")
        bridge._meshcore.stop()


if __name__ == "__main__":
    main()
