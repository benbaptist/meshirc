#!/usr/bin/env python3
"""
MeshIRC — MeshCore ↔ IRC Bridge

Connects to a MeshCore node via TCP (using the official meshcore library) and
bridges messages to/from an IRC channel.  Automatically configures the target
channel on the MeshCore node on startup.

Protocol: uses the meshcore Python library's binary protocol over TCP.
"""

import asyncio
import logging
import os
import secrets
import ssl
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import irc.bot
import irc.client
import irc.connection
from dotenv import load_dotenv
from meshcore import EventType, MeshCore

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CHANNEL_INDEX = 0
MAX_CHANNELS = 40


class ChannelType(Enum):
    PUBLIC = "public"
    PRIVATE = "private"


@dataclass
class Config:
    meshcore_host: str = "192.168.1.100"
    meshcore_port: int = 5000
    irc_host: str = "irc.libera.chat"
    irc_port: int = 6667
    irc_nick: str = "MeshBridge"
    irc_realname: str = "MeshCore IRC Bridge"
    irc_use_tls: bool = False
    irc_channel: str = "#meshnet"
    meshcore_channel: str = "irc-bridge"
    meshcore_channel_type: ChannelType = ChannelType.PUBLIC
    meshcore_channel_index: int = DEFAULT_CHANNEL_INDEX
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        return cls(
            meshcore_host=os.getenv("MESHCORE_HOST", cls.meshcore_host),
            meshcore_port=int(os.getenv("MESHCORE_PORT", str(cls.meshcore_port))),
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
            meshcore_channel_index=int(
                os.getenv("MESHCORE_CHANNEL_INDEX", str(cls.meshcore_channel_index))
            ),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )


# ---------------------------------------------------------------------------
# Bridge
# ---------------------------------------------------------------------------

class MeshIrcBridge:
    """Wires a MeshCore node to an IRC channel bidirectionally."""

    def __init__(self, config: Config):
        self._cfg = config
        self._log = logging.getLogger("bridge")
        self._mc: Optional[MeshCore] = None
        self._irc_bot: Optional["_IrcBot"] = None
        self._irc_to_mesh: asyncio.Queue = asyncio.Queue()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def run(self):
        """Connect both sides and run forever."""
        self._loop = asyncio.get_running_loop()
        self._log.info(
            "Bridge starting — meshcore(%s:%d) chan[%d]=%s ↔ IRC(%s:%d) %s",
            self._cfg.meshcore_host, self._cfg.meshcore_port,
            self._cfg.meshcore_channel_index, self._cfg.meshcore_channel,
            self._cfg.irc_host, self._cfg.irc_port, self._cfg.irc_channel,
        )

        await self._connect_meshcore()
        self._start_irc()

        try:
            while True:
                text = await self._irc_to_mesh.get()
                await self._send_to_mesh(text)
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _connect_meshcore(self):
        """Connect to the MeshCore node and set up the channel."""
        self._log.info("Connecting to MeshCore node at %s:%d ...",
                        self._cfg.meshcore_host, self._cfg.meshcore_port)
        self._mc = await MeshCore.create_tcp(
            self._cfg.meshcore_host, self._cfg.meshcore_port,
            auto_reconnect=True, max_reconnect_attempts=10,
        )
        self._log.info("Connected to MeshCore node.")
        if self._mc.self_info:
            name = self._mc.self_info.get("adv_name", "unknown")
            self._log.info("Node name: %s", name)

        await self._setup_channel()

        self._mc.subscribe(
            EventType.CHANNEL_MSG_RECV,
            self._on_mesh_message,
            attribute_filters={"channel_idx": self._cfg.meshcore_channel_index},
        )
        await self._mc.start_auto_message_fetching()

    async def _setup_channel(self):
        """Create or verify the target channel on the node."""
        idx = self._cfg.meshcore_channel_index
        ch_type = self._cfg.meshcore_channel_type
        ch_name = self._cfg.meshcore_channel

        if ch_type == ChannelType.PUBLIC:
            if not ch_name.startswith("#"):
                ch_name = "#" + ch_name
            self._log.info("Setting up public channel [%d]: %s", idx, ch_name)
            result = await self._mc.commands.set_channel(idx, ch_name)
            if result.type == EventType.ERROR:
                self._log.error("Failed to set channel: %s", result.payload)
                raise RuntimeError(f"Channel setup failed: {result.payload}")
            self._log.info("Public channel configured (key derived from name hash).")
        else:
            secret = secrets.token_bytes(16)
            self._log.info("Setting up private channel [%d]: %s", idx, ch_name)
            result = await self._mc.commands.set_channel(idx, ch_name, secret)
            if result.type == EventType.ERROR:
                self._log.error("Failed to set channel: %s", result.payload)
                raise RuntimeError(f"Channel setup failed: {result.payload}")
            secret_hex = secret.hex()
            banner = (
                f"\n{'='*60}\n"
                f"PRIVATE CHANNEL CREATED\n"
                f"  Name:   {ch_name}\n"
                f"  Secret: {secret_hex}\n"
                f"{'='*60}\n"
            )
            print(banner, flush=True)
            self._log.info("Private channel configured. Secret: %s", secret_hex)

    async def _send_to_mesh(self, text: str):
        """Send a message to the MeshCore channel."""
        if self._mc is None or not self._mc.is_connected:
            self._log.warning("Cannot send to mesh — not connected")
            return
        result = await self._mc.commands.send_chan_msg(
            self._cfg.meshcore_channel_index, text
        )
        if result.type == EventType.ERROR:
            self._log.error("Failed to send mesh message: %s", result.payload)

    async def _on_mesh_message(self, event):
        """Handle incoming CHANNEL_MSG_RECV from the mesh."""
        payload = event.payload
        text = payload.get("text", "")
        sender = payload.get("sender_name") or payload.get("pubkey_prefix", "unknown")
        self._log.info("[mesh → irc] %s: %s", sender, text)
        if self._irc_bot:
            self._irc_bot.queue_to_irc(sender, text)

    def _on_irc_message(self, sender: str, text: str):
        """Called from IRC thread — schedule send on asyncio loop."""
        if self._loop:
            self._loop.call_soon_threadsafe(
                self._irc_to_mesh.put_nowait, f"<{sender}> {text}"
            )

    def _start_irc(self):
        self._irc_bot = _IrcBot(self._cfg, self._on_irc_message)
        self._irc_bot.start()

    async def _shutdown(self):
        self._log.info("Shutting down...")
        if self._irc_bot:
            self._irc_bot.stop()
        if self._mc:
            try:
                await self._mc.stop_auto_message_fetching()
            except Exception:
                pass
            try:
                await self._mc.disconnect()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# IRC Bot (runs in its own thread)
# ---------------------------------------------------------------------------

class _IrcBot(irc.bot.SingleServerIRCBot):
    """IRC bot that bridges to MeshCore."""

    def __init__(self, config: Config, on_message_callback):
        self._cfg = config
        self._on_message = on_message_callback
        self._log = logging.getLogger("irc")

        kwargs = {}
        if config.irc_use_tls:
            kwargs["connect_factory"] = irc.connection.Factory(
                wrapper=ssl.SSLContext().wrap_socket
            )
        super().__init__(
            [irc.bot.ServerSpec(config.irc_host, config.irc_port)],
            config.irc_nick,
            config.irc_realname,
            **kwargs,
        )

    def on_welcome(self, conn, event):
        self._log.info("Connected to IRC server %s", self._cfg.irc_host)
        conn.join(self._cfg.irc_channel)
        self._log.info("Joined IRC channel %s", self._cfg.irc_channel)

    def on_pubmsg(self, conn, event):
        nick = irc.client.NickMask(event.source).nick
        text = event.arguments[0]
        if nick == self._cfg.irc_nick:
            return
        self._on_message(nick, text)

    def on_privmsg(self, conn, event):
        nick = irc.client.NickMask(event.source).nick
        text = event.arguments[0]
        if nick == self._cfg.irc_nick:
            return
        self._on_message(nick, f"[PM] {text}")

    def on_disconnect(self, conn, event):
        self._log.warning("Disconnected from IRC — will auto-reconnect")

    def queue_to_irc(self, sender: str, text: str):
        """Thread-safe: relay a mesh message to IRC."""
        conn = self.connection
        if conn and conn.is_connected():
            formatted = f"\x0303[{sender}]\x03 {text}"
            conn.privmsg(self._cfg.irc_channel, formatted)

    def stop(self):
        try:
            self.disconnect("Bridge shutting down")
        except Exception:
            pass


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
    logging.getLogger("irc.client").setLevel(logging.WARNING)
    logging.getLogger("meshcore").setLevel(logging.WARNING)

    bridge = MeshIrcBridge(config)
    try:
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        logging.info("Interrupted.")


if __name__ == "__main__":
    main()
