#!/usr/bin/env python3
"""
MeshIRC — MeshCore ↔ IRC Bridge

Connects to a MeshCore node via TCP (using the official meshcore library) and
bridges messages to/from an IRC channel.  Configures the target channel on the
MeshCore node on first run; on subsequent restarts it reuses the existing
channel without overwriting it.

Protocol: uses the meshcore Python library's binary protocol over TCP.
"""

import asyncio
import logging
import os
import secrets
import ssl
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
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
SECRET_FILE = Path("/data/channel_secret")


class ChannelType(Enum):
    PUBLIC = "public"
    PRIVATE = "private"


# MeshCore protocol overhead per message: command(1) + subcmd(1) +
# chan_idx(1) + timestamp(4) = 7 bytes.  Frame max is 300 bytes, and
# the firmware payload limit is ~228 bytes, so ~221 bytes remain for
# text.  160 chars is a safe default that works for most UTF-8 text.
DEFAULT_MAX_MSG_LENGTH = 160
DEFAULT_THROTTLE_SECS = 3.0


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
    meshcore_channel_secret: Optional[str] = None
    meshcore_msg_max_len: int = DEFAULT_MAX_MSG_LENGTH
    meshcore_msg_throttle_secs: float = DEFAULT_THROTTLE_SECS
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
            meshcore_channel_secret=os.getenv("MESHCORE_CHANNEL_SECRET"),
            meshcore_msg_max_len=int(os.getenv(
                "MESHCORE_MSG_MAX_LEN", str(cls.meshcore_msg_max_len)
            )),
            meshcore_msg_throttle_secs=float(os.getenv(
                "MESHCORE_MSG_THROTTLE_SECS", str(cls.meshcore_msg_throttle_secs)
            )),
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
        self._throttle_lock = asyncio.Lock()
        self._last_send_time = 0.0

    # -- run -----------------------------------------------------------------

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._log.info(
            "Bridge starting — meshcore(%s:%d) chan[%d]=%s ↔ IRC(%s:%d) %s",
            self._cfg.meshcore_host, self._cfg.meshcore_port,
            self._cfg.meshcore_channel_index, self._cfg.meshcore_channel,
            self._cfg.irc_host, self._cfg.irc_port, self._cfg.irc_channel,
        )

        await self._connect_meshcore()
        self._start_irc()

        # Run both loops concurrently
        poller = asyncio.create_task(self._poll_mesh_loop())
        try:
            while True:
                text = await self._irc_to_mesh.get()
                await self._send_to_mesh(text)
        finally:
            poller.cancel()
            try:
                await poller
            except asyncio.CancelledError:
                pass
            await self._shutdown()

    # -- meshcore connection -------------------------------------------------

    async def _connect_meshcore(self):
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

        self._mc.subscribe(EventType.CONNECTED, self._on_connected)
        self._mc.subscribe(EventType.DISCONNECTED, self._on_disconnected)

        await self._setup_channel()

        self._mc.subscribe(
            EventType.CHANNEL_MSG_RECV,
            self._on_mesh_message,
            attribute_filters={"channel_idx": self._cfg.meshcore_channel_index},
        )

    # -- channel setup -------------------------------------------------------

    async def _setup_channel(self):
        """Create or verify the target channel without clobbering on restart."""
        idx = self._cfg.meshcore_channel_index
        ch_type = self._cfg.meshcore_channel_type
        ch_name = self._cfg.meshcore_channel

        if ch_type == ChannelType.PUBLIC and not ch_name.startswith("#"):
            ch_name = "#" + ch_name

        existing = await self._mc.commands.get_channel(idx)
        if existing.type == EventType.CHANNEL_INFO:
            existing_name = existing.payload.get("name", "")
            if existing_name.rstrip("\x00") == ch_name:
                self._log.info("Channel [%d] %s already configured — reusing.", idx, ch_name)
                return
            self._log.info("Slot [%d] holds '%s' — replacing with '%s'.",
                            idx, existing_name.rstrip("\x00"), ch_name)

        if ch_type == ChannelType.PUBLIC:
            self._log.info("Creating public channel [%d]: %s", idx, ch_name)
            result = await self._mc.commands.set_channel(idx, ch_name)
        else:
            secret = self._resolve_private_secret()
            self._log.info("Creating private channel [%d]: %s", idx, ch_name)
            result = await self._mc.commands.set_channel(idx, ch_name, secret)
            secret_hex = secret.hex()
            banner = (
                f"\n{'='*60}\n"
                f"PRIVATE CHANNEL CREATED\n"
                f"  Name:   {ch_name}\n"
                f"  Secret: {secret_hex}\n"
                f"{'='*60}\n"
            )
            print(banner, flush=True)
            self._log.info("Private channel secret: %s", secret_hex)

        if result.type == EventType.ERROR:
            self._log.error("Channel setup failed: %s", result.payload)
            raise RuntimeError(f"Channel setup failed: {result.payload}")

    def _resolve_private_secret(self) -> bytes:
        """Return 16-byte secret: env var > persisted file > generate new."""
        env_hex = self._cfg.meshcore_channel_secret
        if env_hex:
            try:
                secret = bytes.fromhex(env_hex)
                if len(secret) == 16:
                    self._log.info("Using secret from MESHCORE_CHANNEL_SECRET.")
                    return secret
            except ValueError:
                pass
            self._log.warning("MESHCORE_CHANNEL_SECRET invalid — ignoring.")

        try:
            if SECRET_FILE.exists():
                secret = bytes.fromhex(SECRET_FILE.read_text().strip())
                if len(secret) == 16:
                    self._log.info("Reusing persisted secret from %s.", SECRET_FILE)
                    return secret
        except Exception:
            pass

        secret = secrets.token_bytes(16)
        try:
            SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
            SECRET_FILE.write_text(secret.hex())
            self._log.info("New secret persisted to %s.", SECRET_FILE)
        except OSError as exc:
            self._log.warning("Could not persist secret: %s", exc)
        return secret

    # -- mesh message polling ------------------------------------------------

    async def _poll_mesh_loop(self):
        """Continuously poll for messages instead of relying on push events."""
        self._log.info("Poll loop started.")
        while True:
            try:
                await asyncio.sleep(1)
                if self._mc is None or not self._mc.is_connected:
                    self._log.debug("Poll skip: not connected")
                    continue
                result = await self._mc.commands.get_msg(timeout=3.0)
                if result.type != EventType.NO_MORE_MSGS:
                    self._log.info("Poll result: %s", result.type.value)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.warning("Poll error: %s", exc)

    # -- mesh → IRC ----------------------------------------------------------

    async def _on_mesh_message(self, event):
        payload = event.payload
        text = payload.get("text", "")
        sender = payload.get("sender_name") or payload.get("pubkey_prefix", "unknown")
        self._log.info("[mesh → irc] %s: %s", sender, text)
        if self._irc_bot:
            self._irc_bot.queue_to_irc(sender, text)

    async def _on_connected(self, event):
        self._log.info("[meshcore] connected: %s", event.payload)

    async def _on_disconnected(self, event):
        self._log.warning("[meshcore] disconnected: %s", event.payload)

    # -- IRC → mesh ----------------------------------------------------------

    def _split_message(self, text: str) -> list[str]:
        """Split *text* into chunks that fit within the configured max length.

        Each chunk is annotated with ``[i/n]`` so the receiver can reassemble.
        If the text fits in a single message it is returned unchanged (no
        annotation overhead added).
        """
        max_len = self._cfg.meshcore_msg_max_len
        if len(text) <= max_len:
            return [text]

        # Reserve room for " [i/n]" suffix: 4 chars for " [", up to 3 digits
        # for i, "/", up to 3 digits for n, "]"
        # Worst case: " [999/999]" = 11 chars.  But we compute dynamically.
        # First determine how many chunks we need, then use that to know the
        # exact suffix width.
        def _chunk_count(suffix_len: int) -> int:
            payload = max_len - suffix_len
            if payload <= 0:
                return len(text)  # degenerate — each char gets its own chunk
            return (len(text) + payload - 1) // payload

        # Estimate n conservatively to pick suffix format.
        rough_n = _chunk_count(7)  # " [xx/xx]" ≈ 7 chars
        suffix_fmt = f" [{{i}}/{{n}}]" if rough_n <= 99 else f" [{{i}}/{{n}}]"
        suffix_len = len(suffix_fmt.format(i=rough_n, n=rough_n))

        # Now compute real n with correct suffix length.
        n = _chunk_count(suffix_len)
        # Recompute suffix_len with actual n — the length of the last chunk's
        # suffix may differ if n is wider.  We handle that per-chunk below.
        chunks: list[str] = []
        pos = 0
        for i in range(1, n + 1):
            suffix = suffix_fmt.format(i=i, n=n)
            remaining = len(text) - pos
            chunk_payload = max_len - len(suffix)
            if chunk_payload <= 0:
                # Defensive: just emit the suffix alone if no room for text.
                chunks.append(suffix)
                continue
            take = min(chunk_payload, remaining)
            chunk = text[pos:pos + take] + suffix
            chunks.append(chunk)
            pos += take

        return chunks

    async def _throttled_send_to_mesh(self, text: str):
        """Send a single message to MeshCore, respecting the throttle."""
        async with self._throttle_lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_send_time
            min_gap = self._cfg.meshcore_msg_throttle_secs
            if elapsed < min_gap:
                wait = min_gap - elapsed
                self._log.debug("Throttling send — waiting %.1fs", wait)
                await asyncio.sleep(wait)
            await self._send_one(text)
            self._last_send_time = asyncio.get_event_loop().time()

    async def _send_one(self, text: str):
        """Actually transmit *text* to the MeshCore channel (no throttling)."""
        self._log.info("[irc → mesh] sending: %s", text)
        if self._mc is None or not self._mc.is_connected:
            self._log.warning("Cannot send to mesh — not connected")
            return
        result = await self._mc.commands.send_chan_msg(
            self._cfg.meshcore_channel_index, text
        )
        if result.type == EventType.ERROR:
            self._log.error("send_chan_msg failed: %s", result.payload)
        else:
            self._log.info("[irc → mesh] sent OK")

    async def _send_to_mesh(self, text: str):
        """Split and throttle the message, then send all chunks."""
        chunks = self._split_message(text)
        for chunk in chunks:
            await self._throttled_send_to_mesh(chunk)

    def _on_irc_message(self, sender: str, text: str):
        self._log.info("[irc → mesh] from IRC: <%s> %s", sender, text)
        if self._loop:
            self._loop.call_soon_threadsafe(
                self._irc_to_mesh.put_nowait, f"<{sender}> {text}"
            )

    # -- IRC -----------------------------------------------------------------

    def _start_irc(self):
        self._irc_bot = _IrcBot(self._cfg, self._on_irc_message)
        # IRC bot's start() calls reactor.process_forever() which blocks,
        # so it MUST run in its own thread.
        thread = threading.Thread(target=self._irc_bot.start, daemon=True, name="irc-bot")
        thread.start()

    # -- shutdown ------------------------------------------------------------

    async def _shutdown(self):
        self._log.info("Shutting down...")
        if self._irc_bot:
            self._irc_bot.stop()
        if self._mc:
            try:
                await self._mc.disconnect()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# IRC Bot
# ---------------------------------------------------------------------------

class _IrcBot(irc.bot.SingleServerIRCBot):

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
