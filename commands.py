"""Pure helpers for the bridge's chat commands (no I/O, no network state)."""

import re
import string
from typing import Iterable, Optional, Sequence, TypeVar

T = TypeVar("T")

# MeshCore contact "type" values.
CONTACT_TYPE_REPEATER = 2
CONTACT_TYPE_ROOM = 3

# Path hash sizes (bytes) supported by MeshCore trace packets.
PATH_HASH_SIZES = (1, 2, 4, 8)


def parse_irc_command(
    text: str, nick: str, prefix: str, private: bool = False
) -> Optional[tuple[str, str]]:
    """Return ``(command, args)`` if an IRC line addresses the bot, else None.

    Accepted triggers: ``<prefix>cmd``, ``nick: cmd``, ``nick, cmd``,
    ``nick cmd`` — and a bare ``cmd`` in a private message.
    """
    text = text.strip()
    if prefix and text.startswith(prefix):
        rest = text[len(prefix):]
    else:
        match = re.match(rf"{re.escape(nick)}(?:\s*[:,]\s*|\s+)(.+)$", text, re.I)
        if match:
            rest = match.group(1).strip()
            if prefix and rest.startswith(prefix):
                rest = rest[len(prefix):]
        elif private:
            rest = text
        else:
            return None
    return _split_command(rest)


def parse_mesh_command(body: str, prefix: str) -> Optional[tuple[str, str]]:
    """Return ``(command, args)`` if a mesh message body starts with *prefix*."""
    body = body.strip()
    if not prefix or not body.startswith(prefix):
        return None
    return _split_command(body[len(prefix):])


def _split_command(rest: str) -> Optional[tuple[str, str]]:
    parts = rest.strip().split(None, 1)
    if not parts:
        return None
    return parts[0].lower(), parts[1].strip() if len(parts) > 1 else ""


def split_sender(text: str) -> tuple[Optional[str], str]:
    """Split a mesh channel message ``"Name: body"`` into ``(Name, body)``."""
    sender, sep, body = text.partition(": ")
    if not sep or not sender:
        return None, text
    return sender, body


def is_hex(text: str) -> bool:
    return bool(text) and all(c in string.hexdigits for c in text)


def path_hash_hex(hex_prefix: str) -> str:
    """Trim *hex_prefix* to the largest supported trace hash size it covers.

    The more of a hash the user supplies, the wider the hash used for the
    trace: 2 hex chars → 1 byte, 4 → 2 bytes, 8 → 4 bytes, 16+ → 8 bytes.
    """
    nbytes = len(hex_prefix) // 2
    size = max(s for s in PATH_HASH_SIZES if s <= max(nbytes, 1))
    return hex_prefix[: size * 2].lower()


def find_infra_contacts(contacts: Iterable[dict], query: str) -> list[dict]:
    """Repeaters/rooms whose public key (hex query) or name matches *query*.

    A hex query matches by public-key prefix first, then by name.  Names match
    exactly (case-insensitive) if possible, otherwise by substring.
    """
    infra = [
        c for c in contacts
        if c.get("type") in (CONTACT_TYPE_REPEATER, CONTACT_TYPE_ROOM)
    ]
    q = query.lower()
    if is_hex(q) and len(q) >= 2:
        by_key = [c for c in infra if c["public_key"].lower().startswith(q)]
        if by_key:
            return by_key
    exact = [c for c in infra if c["adv_name"].lower() == q]
    if exact:
        return exact
    return [c for c in infra if q in c["adv_name"].lower()]


def pack_lines(items: Sequence[str], max_len: int = 400, sep: str = " | ") -> list[str]:
    """Greedily join *items* into lines no longer than *max_len*."""
    lines: list[str] = []
    current = ""
    for item in items:
        if current and len(current) + len(sep) + len(item) > max_len:
            lines.append(current)
            current = item
        else:
            current = f"{current}{sep}{item}" if current else item
    if current:
        lines.append(current)
    return lines


def paginate(items: Sequence[T], page: int, size: int) -> tuple[Sequence[T], int, int]:
    """Return ``(items_on_page, page, total_pages)`` with *page* clamped (1-based)."""
    total = max(1, -(-len(items) // size))
    page = min(max(page, 1), total)
    return items[(page - 1) * size: page * size], page, total
