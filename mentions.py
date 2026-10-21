"""Pure helpers for converting IRC nick mentions into MeshCore mentions.

The MeshCore app only registers ``@[Full Node Name]`` as a mention, e.g.
``@[Ben (@benbaptist)]``.  Mesh senders are learned
from the sender names of incoming channel messages.  The bridge's own IRC
nick stands for whoever spoke last on the mesh.
"""

import re

_HANDLE_RE = re.compile(r"\(@([^()\s]+)\)\s*$")
_EXISTING_MENTION = r"@\[[^\]]*\]"


class MentionRewriter:
    def __init__(self):
        # lowercase alias (full name, "@handle", "handle") -> full node name
        self._aliases: dict[str, str] = {}
        self._last_sender: str | None = None
        self._pattern: re.Pattern | None = None
        self._pattern_nick: str | None = None

    def learn(self, name: str) -> None:
        """Record a mesh sender's full node name as the latest speaker."""
        self._last_sender = name
        aliases = [name]
        handle = _HANDLE_RE.search(name)
        if handle:
            aliases += [f"@{handle.group(1)}", handle.group(1)]
        changed = False
        for alias in aliases:
            if self._aliases.get(alias.lower()) != name:
                self._aliases[alias.lower()] = name
                changed = True
        if changed:
            self._pattern = None

    def rewrite(self, text: str, bridge_nick: str) -> str:
        """Replace mentions of known mesh users with ``@[Full Name]``.

        *bridge_nick* is rewritten to the most recent mesh sender, if any.
        """
        if not self._aliases:
            return text
        nick = bridge_nick.lower()
        if self._pattern is None or self._pattern_nick != nick:
            self._pattern_nick = nick
            aliases = sorted({*self._aliases, nick}, key=len, reverse=True)
            alternation = "|".join(re.escape(a) for a in aliases)
            # Existing @[...] spans are matched first so they stay untouched.
            self._pattern = re.compile(
                rf"({_EXISTING_MENTION})|(?<![\w@\[])({alternation})(?!\w)",
                re.IGNORECASE,
            )

        def _sub(match: re.Match) -> str:
            if match.group(1):
                return match.group(1)
            alias = match.group(2).lower()
            if alias == nick:
                return f"@[{self._last_sender}]"
            return f"@[{self._aliases[alias]}]"

        return self._pattern.sub(_sub, text)
