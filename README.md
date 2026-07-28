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
| `LOG_LEVEL` | Logging level | `INFO` |
