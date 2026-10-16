"""Pure helpers for converting IRC nick mentions into MeshCore mentions.

The MeshCore app only registers ``@[Full Node Name]`` as a mention, e.g.
``@[Ben (@benbaptist)]``.  Mesh senders are learned from incoming channel
messages, which are formatted ``Full Node Name: text``.
"""

import re

_HANDLE_RE = re.compile(r"\(@([^()\s]+)\)\s*$")
_EXISTING_MENTION = r"@\[[^\]]*\]"


class MentionRewriter:
    def __init__(self):
        # lowercase alias (full name, "@handle", "handle") -> full node name
        self._aliases: dict[str, str] = {}
        self._pattern: re.Pattern | None = None

    def learn(self, mesh_text: str) -> None:
        """Record the sender of a mesh channel message (``Name: text``)."""
        name, sep, _ = mesh_text.partition(": ")
        name = name.strip()
        if not sep or not name:
            return
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

    def rewrite(self, text: str) -> str:
        """Replace mentions of known mesh users with ``@[Full Name]``."""
        if not self._aliases:
            return text
        if self._pattern is None:
            aliases = sorted(self._aliases, key=len, reverse=True)
            alternation = "|".join(re.escape(a) for a in aliases)
            # Existing @[...] spans are matched first so they stay untouched.
            self._pattern = re.compile(
                rf"({_EXISTING_MENTION})|(?<![\w@\[])({alternation})(?!\w)",
                re.IGNORECASE,
            )

        def _sub(match: re.Match) -> str:
            if match.group(1):
                return match.group(1)
            return f"@[{self._aliases[match.group(2).lower()]}]"

        return self._pattern.sub(_sub, text)
