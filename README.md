# MeshIRC - MeshCore ↔ IRC Bridge

Bridges a MeshCore mesh network node to an IRC channel.
Messages flow bidirectionally between the mesh and IRC.

## Setup

1. Copy `.env.example` to `.env` and edit values:
   ```bash
   cp .env.example .env
   ```

2. Run with Docker Compose:
   ```bash
   docker compose up -d
   ```

3. Or run directly:
   ```bash
   pip install -r requirements.txt
   python meshirc.py
   ```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `MESHCORE_HOST` | MeshCore node IP/hostname | `192.168.1.100` |
| `MESHCORE_PORT` | MeshCore node TCP port | `5000` |
| `MESHCORE_NODE_NAME` | Name reported to the mesh | `meshirc-bridge` |
| `IRC_HOST` | IRC server hostname | `irc.libera.chat` |
| `IRC_PORT` | IRC server port | `6667` |
| `IRC_NICK` | IRC nickname | `MeshBridge` |
| `IRC_REALNAME` | IRC realname | `MeshCore IRC Bridge` |
| `IRC_USE_TLS` | Use TLS for IRC | `false` |
| `IRC_CHANNEL` | IRC channel to join | `#meshnet` |
| `MESHCORE_CHANNEL` | MeshCore channel name | `irc-bridge` |
| `MESHCORE_CHANNEL_TYPE` | `public` (hashtag) or `private` | `public` |
| `IRC_COMMAND_PREFIX` | Prefix for IRC-side commands | `&` |
| `MESH_COMMAND_PREFIX` | Prefix for mesh-side commands | `!` |
| `LOG_LEVEL` | Logging level | `INFO` |

## Commands

Commands are not bridged; the bridge answers them directly.

**IRC** — trigger with the prefix (`&ping foo`), a nick mention (`MeshBridge: ping foo`, `MeshBridge, ping foo`, `MeshBridge ping foo`), or in a PM:

| Command | Description |
|---|---|
| `repeaters` | List known repeaters with the start of their public key |
| `ping <name\|hash>` | Trace a repeater/room. Partial names work; a hex hash matches by public-key prefix and the trace hash width grows with the hash given (2 hex chars = 1 byte, 4 = 2, 8 = 4, 16+ = 8) |
| `help [command]` | Show available commands, or usage for one |
| `list [page]` | Node names seen in mesh chat with their public keys, 3 per page |

**MeshCore** — send in the bridged channel:

| Command | Description |
|---|---|
| `!list` | IRC channel members grouped by status (ops, voice, ...) |
| `!topic` | IRC channel topic |
| `!raw <text>` | Send `<text>` to IRC without the sender prefix, preceded by a `<node> said:` message naming the calling node |
