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
import itertools
import logging
import os
import random
import secrets
import signal
import ssl
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import irc.bot
import irc.client
import irc.connection
from dotenv import load_dotenv
from meshcore import EventType, MeshCore, TCPConnection

from commands import (
    CONTACT_TYPE_REPEATER,
    find_infra_contacts,
    is_hex,
    pack_lines,
    paginate,
    parse_irc_command,
    parse_mesh_command,
    path_hash_hex,
    split_sender,
)
from mentions import MentionRewriter

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
# Cap on the IRC → mesh backlog held while the mesh node is unreachable.
MAX_PENDING_MESSAGES = 200
# The meshcore library treats max_reconnect_attempts as a *lifetime* cap on
# consecutive failed reconnect attempts.  A bridge must never permanently
# give up on the mesh, so use an effectively unbounded value.
MESH_RECONNECT_ATTEMPTS = 2**31 - 1
# Delay between initial-connection attempts while the mesh node is down.
MESH_RETRY_SECS = 5
# Entries per page of the IRC "list" command.
LIST_PAGE_SIZE = 3
# Trace hash width (bytes) used when pinging a repeater/room chosen by name.
PING_NAME_HASH_BYTES = 1
# Lower bound on how long a ping waits for its trace reply.
MIN_PING_TIMEOUT_SECS = 5.0


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
    irc_command_prefix: str = "&"
    mesh_command_prefix: str = "!"
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
            irc_command_prefix=os.getenv("IRC_COMMAND_PREFIX", cls.irc_command_prefix),
            mesh_command_prefix=os.getenv("MESH_COMMAND_PREFIX", cls.mesh_command_prefix),
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
        # IRC → mesh messages queued while the mesh node is disconnected,
        # flushed on reconnect.  Bounded to avoid unbounded memory growth.
        self._mesh_pending: list[str] = []
        # True once the mesh channel is configured and ready for sends.
        self._mesh_ready = False
        # Sender names seen in mesh channel chat → last-seen unix time.
        self._seen_nodes: dict[str, float] = {}
        self._tasks: set[asyncio.Task] = set()
        # Command handlers: (sender, args) → reply lines.
        self._irc_commands = {
            "repeaters": self._irc_cmd_repeaters,
            "ping": self._irc_cmd_ping,
            "list": self._irc_cmd_list,
        }
        self._mesh_commands = {
            "list": self._mesh_cmd_list,
            "topic": self._mesh_cmd_topic,
            "raw": self._mesh_cmd_raw,
        }

    # -- run -----------------------------------------------------------------

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._install_signal_handlers()
        self._log.info(
            "Bridge starting — meshcore(%s:%d) chan[%d]=%s ↔ IRC(%s:%d) %s",
            self._cfg.meshcore_host, self._cfg.meshcore_port,
            self._cfg.meshcore_channel_index, self._cfg.meshcore_channel,
            self._cfg.irc_host, self._cfg.irc_port, self._cfg.irc_channel,
        )

        # IRC runs in its own thread and is independent of mesh availability.
        self._start_irc()

        # The mesh connection retries forever in the background; IRC → mesh
        # traffic is buffered until it is up.
        mesh_task = asyncio.create_task(self._connect_meshcore())
        poll_task = asyncio.create_task(self._poll_mesh_loop())

        try:
            while True:
                text = await self._irc_to_mesh.get()
                await self._send_to_mesh(text)
        finally:
            for task in (mesh_task, poll_task):
                task.cancel()
            for task in (mesh_task, poll_task):
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    self._log.error("Background task failed: %s", exc)
            await self._shutdown()

    def _install_signal_handlers(self):
        """Exit cleanly on SIGINT/SIGTERM instead of hanging as PID 1.

        Docker runs this process as PID 1, where the kernel ignores any
        signal whose default action is "terminate" unless a handler is
        installed.  Cancelling the main task triggers the ``finally``
        block above, which stops the IRC bot and disconnects the mesh
        node before the process exits.
        """
        main_task = asyncio.current_task()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                self._loop.add_signal_handler(signum, main_task.cancel)
            except (NotImplementedError, RuntimeError, ValueError):
                # Non-main thread, unsupported on this platform, or a
                # signal handler is already installed.
                pass

    # -- meshcore connection -------------------------------------------------

    async def _connect_meshcore(self):
        while True:
            self._log.info("Connecting to MeshCore node at %s:%d ...",
                            self._cfg.meshcore_host, self._cfg.meshcore_port)
            try:
                self._mc = await self._create_meshcore()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._mc = None
                self._log.warning(
                    "MeshCore connection failed: %s — retrying in %ds",
                    exc, MESH_RETRY_SECS,
                )
                await asyncio.sleep(MESH_RETRY_SECS)
                continue
            if self._mc is None:
                self._log.warning(
                    "MeshCore node not responding — retrying in %ds",
                    MESH_RETRY_SECS,
                )
                await asyncio.sleep(MESH_RETRY_SECS)
                continue

            self._log.info("Connected to MeshCore node.")
            if self._mc.self_info:
                name = self._mc.self_info.get("adv_name", "unknown")
                self._log.info("Node name: %s", name)

            try:
                await self._setup_channel()
                self._mc.subscribe(EventType.CONNECTED, self._on_connected)
                self._mc.subscribe(EventType.DISCONNECTED, self._on_disconnected)
                self._mc.subscribe(
                    EventType.CHANNEL_MSG_RECV,
                    self._on_mesh_message,
                    attribute_filters={"channel_idx": self._cfg.meshcore_channel_index},
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.error(
                    "MeshCore channel setup failed: %s — reconnecting in %ds",
                    exc, MESH_RETRY_SECS,
                )
                await self._disconnect_meshcore()
                await asyncio.sleep(MESH_RETRY_SECS)
                continue

            self._mesh_ready = True
            self._log.info("MeshCore channel ready.")
            # The initial CONNECTED event fired before we subscribed, so flush
            # any messages queued while the node was unreachable.
            await self._flush_pending_to_mesh()
            break

    async def _create_meshcore(self) -> Optional[MeshCore]:
        """Create and connect a MeshCore instance, cleaning up on failure.

        ``MeshCore.create_tcp`` leaks its event-dispatcher task when the TCP
        connect raises (it never returns the half-built instance), so build
        the instance here and always disconnect it on failure.
        """
        connection = TCPConnection(
            self._cfg.meshcore_host, self._cfg.meshcore_port
        )
        mc = MeshCore(
            connection,
            auto_reconnect=True,
            max_reconnect_attempts=MESH_RECONNECT_ATTEMPTS,
        )
        try:
            result = await mc.connect()
        except BaseException:
            await mc.disconnect()
            raise
        if result is None:
            await mc.disconnect()
            return None
        return mc

    async def _disconnect_meshcore(self):
        if self._mc is None:
            return
        mc, self._mc = self._mc, None
        try:
            await mc.disconnect()
        except Exception:
            pass

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
        # Channel messages embed the sender name in the text itself
        # (e.g. "DeviceName (ShortName): message") — no separate sender
        # field exists in the payload (no sender_name or pubkey_prefix).
        sender, body = split_sender(text)
        if sender:
            self._seen_nodes[sender] = time.time()

        parsed = parse_mesh_command(body, self._cfg.mesh_command_prefix)
        if parsed and parsed[0] in self._mesh_commands:
            self._log.info("[mesh command] %s: %s", sender, body)
            self._spawn(self._run_mesh_command(sender, *parsed))
            return

        self._mentions.learn(text)
        self._log.info("[mesh → irc] %s", text)
        if self._irc_bot:
            self._irc_bot.send_to_channel(text)

    async def _on_connected(self, event):
        self._log.info("[meshcore] connected: %s", event.payload)
        await self._flush_pending_to_mesh()

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

    def _buffer_for_mesh(self, text: str):
        """Queue *text* for delivery once the mesh node reconnects."""
        if len(self._mesh_pending) >= MAX_PENDING_MESSAGES:
            dropped = self._mesh_pending.pop(0)
            self._log.warning(
                "Mesh backlog full (%d) — dropping oldest message: %s",
                MAX_PENDING_MESSAGES, dropped,
            )
        was_empty = not self._mesh_pending
        self._mesh_pending.append(text)
        if was_empty:
            self._log.warning(
                "Mesh not connected — queueing IRC messages until it returns"
            )
        else:
            self._log.debug(
                "Mesh not connected — queued message (%d pending)",
                len(self._mesh_pending),
            )

    def _mesh_can_send(self) -> bool:
        """True once the mesh channel is configured and the node is connected."""
        return (
            self._mesh_ready
            and self._mc is not None
            and self._mc.is_connected
        )

    async def _send_one(self, text: str):
        """Actually transmit *text* to the MeshCore channel (no throttling)."""
        if not self._mesh_can_send():
            self._buffer_for_mesh(text)
            return
        self._log.info("[irc → mesh] sending: %s", text)
        try:
            result = await self._mc.commands.send_chan_msg(
                self._cfg.meshcore_channel_index, text
            )
        except Exception as exc:
            self._log.error("send_chan_msg raised: %s", exc)
            return
        if result.type == EventType.ERROR:
            self._log.error("send_chan_msg failed: %s", result.payload)
        else:
            self._log.info("[irc → mesh] sent OK")

    async def _send_to_mesh(self, text: str):
        """Split and throttle the message, then send all chunks.

        While the mesh node is disconnected the chunks are queued instead, so
        no IRC traffic is lost during an outage.
        """
        chunks = self._split_message(text)
        if not self._mesh_can_send():
            for chunk in chunks:
                self._buffer_for_mesh(chunk)
            return
        for chunk in chunks:
            await self._throttled_send_to_mesh(chunk)

    async def _flush_pending_to_mesh(self):
        """Deliver any queued IRC → mesh messages after a reconnect."""
        if not self._mesh_pending or not self._mesh_can_send():
            return
        pending, self._mesh_pending = self._mesh_pending, []
        self._log.info("Flushing %d queued message(s) to mesh", len(pending))
        for text in pending:
            await self._throttled_send_to_mesh(text)

    def _on_irc_message(self, sender: str, text: str, reply_to: str, private: bool):
        """Handle an IRC line (called from the IRC thread)."""
        parsed = parse_irc_command(
            text, self._irc_bot.nickname, self._cfg.irc_command_prefix, private
        )
        if parsed and parsed[0] in self._irc_commands and self._loop:
            self._log.info("[irc command] %s: %s", sender, text)
            self._loop.call_soon_threadsafe(
                self._spawn, self._run_irc_command(sender, reply_to, *parsed)
            )
            return

        if private:
            text = f"[PM] {text}"
        self._log.info("[irc → mesh] from IRC: <%s> %s", sender, text)
        if self._loop:
            self._loop.call_soon_threadsafe(
                self._irc_to_mesh.put_nowait,
                f"<{sender}> {self._mentions.rewrite(text)}",
            )

    # -- commands ------------------------------------------------------------

    def _spawn(self, coro):
        """Run *coro* in the background, keeping a reference until it ends."""
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_irc_command(self, sender: str, reply_to: str, cmd: str, args: str):
        try:
            lines = await self._irc_commands[cmd](sender, args)
        except Exception as exc:
            self._log.exception("IRC command %r failed", cmd)
            lines = [f"{cmd} failed: {exc}"]
        for line in lines:
            self._irc_bot.send(reply_to, line)

    async def _run_mesh_command(self, sender: Optional[str], cmd: str, args: str):
        try:
            lines = await self._mesh_commands[cmd](sender, args)
        except Exception as exc:
            self._log.exception("Mesh command %r failed", cmd)
            lines = [f"{cmd} failed: {exc}"]
        for line in lines:
            await self._send_to_mesh(line)

    def _mesh_connected(self) -> bool:
        return self._mc is not None and self._mc.is_connected

    # IRC-side commands

    async def _irc_cmd_repeaters(self, sender: str, args: str) -> list[str]:
        if not self._mesh_connected():
            return ["MeshCore node is not connected."]
        await self._mc.ensure_contacts(follow=True)
        repeaters = sorted(
            (c for c in self._mc.contacts.values()
             if c.get("type") == CONTACT_TYPE_REPEATER),
            key=lambda c: c["adv_name"].lower(),
        )
        if not repeaters:
            return ["No repeaters known."]
        items = [f"{c['adv_name']} [{c['public_key'][:8]}]" for c in repeaters]
        return [f"{len(repeaters)} repeater(s):"] + pack_lines(items)

    async def _irc_cmd_ping(self, sender: str, args: str) -> list[str]:
        query = args.strip()
        if not query:
            return ["Usage: ping <name or hex hash of a repeater/room>"]
        if not self._mesh_connected():
            return ["MeshCore node is not connected."]
        await self._mc.ensure_contacts(follow=True)

        matches = find_infra_contacts(self._mc.contacts.values(), query)
        if len(matches) > 1:
            names = ", ".join(
                f"{c['adv_name']} [{c['public_key'][:4]}]" for c in matches[:5]
            )
            more = f" (+{len(matches) - 5} more)" if len(matches) > 5 else ""
            return [f"'{query}' is ambiguous: {names}{more}"]

        if matches:
            contact = matches[0]
            key = contact["public_key"].lower()
            if is_hex(query) and key.startswith(query.lower()):
                hash_hex = path_hash_hex(query)
            else:
                hash_hex = key[: PING_NAME_HASH_BYTES * 2]
            label = f"{contact['adv_name']} [{hash_hex}]"
        elif is_hex(query) and len(query) >= 2:
            # Unknown node: still worth tracing a bare hash.
            hash_hex = path_hash_hex(query)
            label = f"[{hash_hex}]"
        else:
            return [f"No repeater or room matching '{query}'."]

        result = await self._trace(hash_hex)
        if result is None:
            return [f"No response from {label}."]
        rtt_ms, path = result
        snr = ""
        if path:
            snr = f", SNR out {path[0]['snr']:+.1f} / back {path[-1]['snr']:+.1f} dB"
        return [f"Pong from {label}: {rtt_ms:.0f} ms{snr}"]

    async def _trace(self, hash_hex: str) -> Optional[tuple[float, list]]:
        """Trace through *hash_hex*; return ``(rtt_ms, path)`` or None on timeout."""
        tag = random.randint(1, 0xFFFFFFFF)
        reply: asyncio.Future = asyncio.get_running_loop().create_future()

        async def on_trace(event):
            if not reply.done():
                reply.set_result(event.payload)

        sub = self._mc.subscribe(
            EventType.TRACE_DATA, on_trace, attribute_filters={"tag": tag}
        )
        try:
            started = time.monotonic()
            sent = await self._mc.commands.send_trace(tag=tag, path=hash_hex)
            if sent.type == EventType.ERROR:
                raise RuntimeError(f"trace send failed: {sent.payload}")
            timeout = max(
                sent.payload["suggested_timeout"] / 1000, MIN_PING_TIMEOUT_SECS
            )
            try:
                payload = await asyncio.wait_for(reply, timeout)
            except asyncio.TimeoutError:
                return None
            return (time.monotonic() - started) * 1000, payload.get("path", [])
        finally:
            sub.unsubscribe()

    async def _irc_cmd_list(self, sender: str, args: str) -> list[str]:
        page = int(args) if args.isdigit() else 1
        if not self._seen_nodes:
            return ["No nodes seen in chat yet."]
        if self._mesh_connected():
            await self._mc.ensure_contacts(follow=True)
        names = sorted(self._seen_nodes, key=self._seen_nodes.get, reverse=True)
        shown, page, total = paginate(names, page, LIST_PAGE_SIZE)
        lines = []
        for name in shown:
            contact = self._mc.get_contact_by_name(name) if self._mc else None
            key = contact["public_key"] if contact else "unknown key"
            lines.append(f"{name}  {key}")
        if total > 1:
            lines.append(f"Page {page}/{total} — 'list <page>' for more")
        return lines

    # Mesh-side commands

    async def _mesh_cmd_list(self, sender: Optional[str], args: str) -> list[str]:
        groups = self._irc_bot.channel_members()
        if not groups:
            return ["Not in the IRC channel yet."]
        return [f"{label} ({len(nicks)}): {', '.join(nicks)}" for label, nicks in groups]

    async def _mesh_cmd_topic(self, sender: Optional[str], args: str) -> list[str]:
        topic = self._irc_bot.topic
        return [f"Topic: {topic}" if topic else "No topic set."]

    async def _mesh_cmd_raw(self, sender: Optional[str], args: str) -> list[str]:
        prefix = self._cfg.mesh_command_prefix
        if not args:
            return [f"Usage: {prefix}raw <text>"]
        # Callout first so the raw line is the last thing other bots see.
        self._irc_bot.send_to_channel(
            f"[MeshCore] {sender or 'unknown node'} used {prefix}raw"
        )
        self._irc_bot.send_to_channel(args)
        return []

    # -- IRC -----------------------------------------------------------------

    def _start_irc(self):
        self._irc_bot = _IrcBot(self._cfg, self._on_irc_message)
        # IRC bot's start() calls reactor.process_forever() which blocks,
        # so it MUST run in its own thread.
        thread = threading.Thread(target=self._run_irc_bot, daemon=True, name="irc-bot")
        thread.start()

    def _run_irc_bot(self):
        """Run the IRC bot, logging any crash instead of dying silently."""
        try:
            self._irc_bot.start()
        except Exception:
            self._log.exception("IRC bot thread crashed — IRC bridge is down")

    # -- shutdown ------------------------------------------------------------

    async def _shutdown(self):
        self._log.info("Shutting down...")
        if self._irc_bot:
            self._irc_bot.stop()
        await self._disconnect_meshcore()


# ---------------------------------------------------------------------------
# IRC Bot
# ---------------------------------------------------------------------------

class _SafeServerConnection(irc.client.ServerConnection):
    """ServerConnection that can't crash the reactor thread when disconnected.

    Also decodes incoming lines leniently: the stock ``DecodingLineBuffer``
    uses ``errors='strict'``, so a message truncated mid-UTF-8-character
    (e.g. a long line cut at a byte boundary by another bridge) raises
    ``UnicodeDecodeError`` inside the buffer iterator and kills the reactor
    thread.  ``LenientDecodingLineBuffer`` decodes with ``errors='replace'``,
    substituting U+FFFD for malformed bytes instead of crashing.

    The stock library's ``set_keepalive`` schedules a recurring PING with
    ``reactor.scheduler.execute_every``; that command outlives the connection,
    so after a disconnect it fires ``ping()`` against a closed socket and
    raises ``ServerNotConnectedError`` — killing the reactor thread and, with
    it, every future reconnect.  The same applies to the library's automatic
    PONG replies on incoming server PINGs.

    This subclass makes sends on a dead connection no-ops, and replaces the
    library keepalive with a self-arming one-shot that can be cancelled on
    disconnect via ``clear_keepalive``.
    """

    buffer_class = irc.client.buffer.LenientDecodingLineBuffer

    def ping(self, target, target2=""):
        if self.is_connected():
            super().ping(target, target2)

    def pong(self, target, target2=""):
        if self.is_connected():
            super().pong(target, target2)

    def set_keepalive(self, interval):
        self._keepalive_interval = interval
        # Each keepalive call invalidates any previously scheduled chain, so
        # reconnects don't stack duplicate pingers.
        gen = getattr(self, "_keepalive_gen", 0) + 1
        self._keepalive_gen = gen

        def tick():
            if getattr(self, "_keepalive_gen", 0) != gen:
                return  # superseded by a newer keepalive or a disconnect
            self.ping("keep-alive")  # guarded no-op when disconnected
            self.reactor.scheduler.execute_after(self._keepalive_interval, tick)

        self.reactor.scheduler.execute_after(self._keepalive_interval, tick)

    def clear_keepalive(self):
        """Stop the keepalive chain (call on disconnect)."""
        self._keepalive_gen = getattr(self, "_keepalive_gen", 0) + 1


class _SafeReactor(irc.client.Reactor):
    connection_class = _SafeServerConnection


class _IrcBot(irc.bot.SingleServerIRCBot):

    reactor_class = _SafeReactor

    # Keepalive: send a PING every KEEPALIVE_SECS to keep the socket (and any
    # intervening NAT/firewall mapping) alive.  15s is aggressive enough to
    # stop idle timeouts from both the ircd and middleboxes.
    KEEPALIVE_SECS = 15

    def __init__(self, config: Config, on_message_callback):
        self._cfg = config
        self._on_message = on_message_callback
        self._log = logging.getLogger("irc")
        self.topic: Optional[str] = None

        kwargs = {}
        if config.irc_use_tls:
            kwargs["connect_factory"] = irc.connection.Factory(
                wrapper=ssl.SSLContext().wrap_socket
            )

        # Use exponential backoff with shorter, saner intervals so
        # reconnection happens quickly (5-60 s) instead of the library
        # default 60-300 s.  The attempt counter is reset in on_welcome
        # so the backoff doesn't grow unboundedly over the container's
        # lifetime.
        recon = irc.bot.ExponentialBackoff(min_interval=5, max_interval=60)

        super().__init__(
            [irc.bot.ServerSpec(config.irc_host, config.irc_port)],
            config.irc_nick,
            config.irc_realname,
            recon=recon,
            **kwargs,
        )

    def on_welcome(self, conn, event):
        self._log.info("Connected to IRC server %s", self._cfg.irc_host)
        conn.join(self._cfg.irc_channel)
        self._log.info("Joined IRC channel %s", self._cfg.irc_channel)

        # Start client-side keepalive PINGs so the IRC server doesn't
        # drop us for idling.
        conn.set_keepalive(self.KEEPALIVE_SECS)

        # Reset the exponential-backoff attempt counter so that a
        # successful connection always restores the shortest delay.
        self.recon.attempt_count = itertools.count(1)

    def on_pubmsg(self, conn, event):
        nick = irc.client.NickMask(event.source).nick
        text = event.arguments[0]
        if nick == self._cfg.irc_nick:
            return
        self._on_message(nick, text, self._cfg.irc_channel, False)

    def on_privmsg(self, conn, event):
        nick = irc.client.NickMask(event.source).nick
        text = event.arguments[0]
        if nick == self._cfg.irc_nick:
            return
        self._on_message(nick, text, nick, True)

    def on_currenttopic(self, conn, event):
        channel, topic = event.arguments
        if channel.lower() == self._cfg.irc_channel.lower():
            self.topic = topic

    def on_notopic(self, conn, event):
        self.topic = None

    def on_topic(self, conn, event):
        if event.target.lower() == self._cfg.irc_channel.lower():
            self.topic = event.arguments[0] or None

    def on_disconnect(self, conn, event):
        self.topic = None
        conn.clear_keepalive()
        self._log.warning("Disconnected from IRC — will auto-reconnect")

    @property
    def nickname(self) -> str:
        """Our current nick (may differ from the configured one after a clash)."""
        return self.connection.get_nickname()

    def send(self, target: str, text: str):
        conn = self.connection
        if not conn.is_connected():
            return
        try:
            conn.privmsg(target, text)
        except irc.client.ServerNotConnectedError:
            self._log.debug("Dropped message to IRC — not connected")

    def send_to_channel(self, text: str):
        self.send(self._cfg.irc_channel, text)

    def channel_members(self) -> list[tuple[str, list[str]]]:
        """Channel members grouped by their highest status, highest first."""
        channel = self.channels.get(self._cfg.irc_channel)
        if channel is None:
            return []
        tiers = [
            ("Owners", channel.owners()),
            ("Admins", channel.admins()),
            ("Ops", channel.opers()),
            ("Halfops", channel.halfops()),
            ("Voice", channel.voiced()),
        ]
        remaining = set(channel.users())
        groups = []
        for label, nicks in tiers:
            members = sorted(remaining.intersection(nicks), key=str.lower)
            remaining.difference_update(members)
            if members:
                groups.append((label, members))
        if remaining:
            groups.append(("Users", sorted(remaining, key=str.lower)))
        return groups

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
    except asyncio.CancelledError:
        logging.info("Shutdown complete.")


if __name__ == "__main__":
    main()
