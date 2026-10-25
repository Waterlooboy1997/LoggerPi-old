# DietPi TCP forwarding server

Headless migration of the original GUI, now archived in `legacy/app.py`. The active entry point is `main.py`. Python asyncio TCP listeners, Flask/Waitress LAN dashboard, systemd management. Supports Python 3.9+ (DietPi's actual Python version should be checked with `python3 --version`).

## Original behaviour retained

- Data ports: 300–379, 6785, 623, 724, 721, 723 (85 total); command port 622.
- Maximum two accepted clients per data port. A new connection from the same IP evicts existing connections from that IP **on that port** before the capacity check.
- Binary data is forwarded in both directions between the two clients, without modification or buffering for clients that are absent.
- Remote commands remain `RESET=<port>` with the original response text.
- Reset closes connected clients but leaves listening sockets open.
- Per-port history holds up to 500 events; chunk/payload logging disabled by default. Reject/duplicate counters persist until process restart.
- Client traffic counters disappear when a client disconnects; new cumulative traffic counters remain until process restart.

## Deliberate reliability changes

- Admission lock per port prevents simultaneous connections bypassing capacity or duplicate-IP policy.
- Writer drain/close operations have two-second deadlines; a stalled peer cannot indefinitely block cleanup.
- Client removal happens before awaiting network shutdown, preventing further forwarding to a closing client.
- **No automatic disconnect.** The original dropped every client at minute 30 each hour; that is removed. Connections stay up until a peer closes, keepalive finds them dead, or you reset a port from the dashboard or with `RESET=<port>` on port 622.
- Port-bind failures abort startup and close previously opened listeners; shutdown cancels tracked handlers.
- Remote command connections time out after ten seconds without data; source IP must match `TRUSTED_NETWORKS`.
- A separate Disconnect action closes clients without writing the `Port X reset complete` event.
- **No text is ever written to a data connection.** The original sent disconnect reasons such as `Duplicate IP detected.` into the socket, putting stray bytes into the PakBus stream. Reasons now go to the event log only and the socket is simply closed.
- **TCP keepalive** is on for every accepted data socket, so a modem connection that vanishes without closing is dropped in about two minutes instead of holding a slot (see *TCP keepalive* below).

## Installation on DietPi

Choose your Pi's static LAN address and subnet. Example below uses `192.168.1.20` and `192.168.1.0/24`; **replace both** with your actual values. If connecting via VPN, include the specific VPN subnet only if trusted.

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
sudo useradd --system --home /opt/tcp-server --shell /usr/sbin/nologin tcpserver
sudo mkdir -p /opt/tcp-server
# Copy all contents of the extracted project directory to /opt/tcp-server (scp/rsync from your computer).
sudo chown -R root:root /opt/tcp-server
sudo python3 -m venv /opt/tcp-server/.venv
sudo /opt/tcp-server/.venv/bin/pip install -r /opt/tcp-server/requirements.txt
sudo tee /etc/default/tcp-server >/dev/null <<'ENV'
TCP_HOST=0.0.0.0
WEB_HOST=192.168.1.20
WEB_PORT=8888
TRUSTED_NETWORKS=127.0.0.1/32,192.168.1.0/24
# Optional keepalive tuning (seconds / probes), see "TCP keepalive":
# KEEPALIVE_IDLE=60
# KEEPALIVE_INTERVAL=15
# KEEPALIVE_COUNT=4
ENV
sudo chmod 644 /etc/default/tcp-server
sudo cp /opt/tcp-server/systemd/tcp-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tcp-server
```

**IMPORTANT:** A source-IP allowlist is not a substitute for a host firewall. Set firewall rules so TCP/8888 and TCP/622 are accepted **only** from your trusted LAN. Block access from other interfaces, including routed/VPN networks you do not trust. If using UFW, configure your SSH allow rule *before* enabling UFW. Example for a Pi with `eth0`, after checking `ip -br addr`:

```bash
sudo apt install -y ufw
sudo ufw allow in on eth0 from 192.168.1.0/24 to any port 22 proto tcp
sudo ufw allow in on eth0 from 192.168.1.0/24 to any port 8888 proto tcp
sudo ufw allow in on eth0 from 192.168.1.0/24 to any port 622 proto tcp
# Review any existing rules and ensure there are no broader permits for 622/8888.
sudo ufw status numbered
# Only enable after confirming SSH access is allowed and reviewing data-port requirements.
sudo ufw enable
```

The firewall's default incoming deny policy would also block the 85 data ports. **Add the required ingress rules for data ports before enabling the firewall**, considering which modem networks need access. For example, to allow only your trusted LAN for the 80 contiguous ports: `sudo ufw allow in on eth0 from 192.168.1.0/24 to any port 300:379 proto tcp`. Add separate rules for 623, 6785, 721, 723 and 724 as appropriate. If the data ports need public inbound access, restrict and document that independently; never broadly expose 622 or 8888. Check router port forwarding and IPv6 exposure separately. The dashboard also rejects requests outside `TRUSTED_NETWORKS` at the application layer.

## Service commands

```bash
sudo systemctl start tcp-server
sudo systemctl stop tcp-server
sudo systemctl restart tcp-server
sudo systemctl status tcp-server
sudo systemctl enable tcp-server
sudo systemctl disable tcp-server
sudo journalctl -u tcp-server -f
sudo journalctl -u tcp-server -n 200 --no-pager
```

Open `http://192.168.1.20:8888` from a browser on the trusted LAN. No authentication is configured. Browser actions require a same-session control token and a confirmation dialog; this is **CSRF protection, not authentication**. Any LAN user with dashboard access can operate controls.

## Checks

```bash
python3 --version
ip -br addr
sudo ss -ltnp | grep -E ':(300|379|622|623|6785|8888)\b'
curl -i http://192.168.1.20:8888/api/status
printf 'RESET=300\n' | nc -w 3 192.168.1.20 622
sudo journalctl -u tcp-server -n 100 --no-pager
```

For a forwarding test, connect two clients from **different source IPs** to the same data port. Two connections from the same IP intentionally trigger duplicate-IP eviction. Transmit bytes from each client and confirm receipt by the other. The supplied `tests/test_backend.py` runs an automated localhost test using distinct loopback source addresses.

## Troubleshooting

- **Service fails to start:** `journalctl -u tcp-server -e`; check port collisions with `ss -ltnp`, configuration IP, dependencies and capabilities. Startup requires all 86 TCP listeners plus the dashboard.
- **Dashboard 403:** browser source IP not in `TRUSTED_NETWORKS`; inspect `/etc/default/tcp-server`, including VPN/subnet configuration.
- **Dashboard inaccessible:** verify `WEB_HOST` is assigned to the Pi; verify firewall and routing; test locally with `curl`.
- **RESET command gets no reply:** command source IP not trusted, firewall restriction or timeout. The command port intentionally does not authenticate beyond IP restriction.
- **One client immediately disconnects:** both clients may share the same public/NAT IP; this is the original duplicate-IP policy.
- **`Dead connection reaped` in the event log:** the peer stopped answering TCP keepalive (usually a modem that lost coverage or power) and its slot was freed.
- **CPU temperature N/A:** DietPi kernel may not expose `/sys/class/thermal/thermal_zone0/temp`.

## Notes

- HTTP and TCP share one process but use different threads. The dashboard is not needed for forwarding to continue; a fatal process crash still affects both.
- Data port bindings remain `0.0.0.0` by default. The dashboard should bind to the Pi's actual LAN address and use firewall isolation.
- Per-client `bytes_out` counts bytes accepted by the asyncio transport after `drain`, not remote application acknowledgement.
- This migration includes concurrency fixes, so under unusual simultaneous-connection races the outcome may differ from the original buggy behaviour while preserving the intended rules.

## Development and tests

Create an isolated environment and install the headless app dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

On DietPi, launch the active app with `python main.py` using the configured environment and the permissions needed to bind its low-numbered ports, or use the systemd service described above. Tests use temporary, unprivileged localhost ports and do not require a Pi or a connected logger.

Python automatically regenerates `__pycache__/` and `.pyc` files. They are not source files and are ignored by Git, along with local virtual environments, test caches, and generated logs.

## Archived desktop GUI

`legacy/app.py` preserves the original standalone Qt GUI for reference and optional use. It is not imported by the headless app or used by its systemd service. Its optional dependency, PySide6, is intentionally excluded from `requirements.txt`.

To run it on a machine with a graphical desktop, create a separate virtual environment, install PySide6, and launch the archived script:

```bash
python3 -m venv .venv-gui
. .venv-gui/bin/activate
python -m pip install PySide6
python legacy/app.py
```

The original GUI binds the same TCP ports as the headless app, so stop the headless service before launching it. Low-numbered ports require suitable OS permissions. The GUI is retained as historical source and does not include the migrated backend's reliability changes.

## Site names

The **Site** column identifies the configured site for each callback port. Select a port,
edit **Site name** in its details, then choose **Save** (or **Cancel** to discard edits).
An empty name clears the label. Names are searchable alongside port numbers and IPs.
Saving changes only the label; connected clients and forwarding continue normally.
Any trusted dashboard user can edit names using the existing control-token protection.

`sites.json` contains the initial assignments and optional PakBus metadata. Callback
ports determine labels; for example, HUL uses callback port 310 and PakBus 305.
UMR remains unassigned until its callback port is confirmed.

For a direct `python main.py` launch, the app reads and updates `sites.json` beside
its script. Set `SITE_CONFIG_PATH` to use another path; its parent directory must
exist and be writable. If that file is missing, the bundled seed is copied there.
Invalid configuration stops startup with an error and is never automatically replaced.

The supplied systemd unit creates `/var/lib/tcp-server` with `StateDirectory` and
sets `SITE_CONFIG_PATH=/var/lib/tcp-server/sites.json`, allowing saves under
`ProtectSystem=strict`. Existing saved files are retained on restart and upgrade.
When upgrading, copy the updated unit to `/etc/systemd/system/tcp-server.service`,
run `sudo systemctl daemon-reload`, then `sudo systemctl restart tcp-server`.
An explicit `SITE_CONFIG_PATH` in `/etc/default/tcp-server` overrides the unit default;
custom locations also need an appropriate writable-path service override.

For manual edits, stop the service, edit the active JSON file, then restart it.
The format is a list of objects with `site`, `callback_port` (integer or null), and
`pakbus` (integer or null). Port assignments must be unique. Browser renames preserve
PakBus values and other metadata. Names are trimmed, limited to 100 characters,
and cannot contain control characters. Changes persist across browser and server restarts.

Developer checks: `python -m unittest discover -s tests -v` covers forwarding,
site persistence, validation, and API access controls. With Node.js available,
`node tests/test_dashboard.js` checks site rendering, filtering, and editor behavior
using a lightweight DOM stub.

## Traffic monitor

The dashboard's **Traffic monitor** panel shows what crosses the selected data port, live.
Choose a port, pick a duration, and press **Start capture**. Each chunk read from a client is
listed with its time, the gap since the previous chunk, source peer, the peers it was forwarded
to (or `NOT FORWARDED` when no other client is connected), length, hex and ASCII. Connects,
disconnects and rejects appear in the same timeline.

PakBus frames are decoded where possible (sync `0xBD`, `0xBC` quoting, header, message type,
transaction number, signature check) following PyCampbellCR1000's `pakbus.py`. Decoding is
informational; non-PakBus data is simply shown as bytes. Runs of three or more `0xBD` are
reported as an attention/sync burst.

Capture is **off by default**, per port, stops after the chosen time (maximum 60 minutes), is
limited to 8 ports at a time and keeps the newest 3000 entries per port (512 bytes stored per
chunk; the true length is always shown). The tap only copies data after it has been read: it
never writes to a socket, and any failure inside it is swallowed so forwarding is unaffected.
**Download .txt** saves the capture as plain text. Captured packets can contain PakBus security
codes (for example in clock and file commands), so treat downloads as sensitive.

API: `GET /api/ports/<port>/tap?since=<id>`, `GET /api/ports/<port>/tap.txt`, and
`POST /api/ports/<port>/tap/{start,stop,clear}` (control token required, like other actions).

## Updating the Pi

From the app folder on the Pi: `./deploy/update.sh`. It fetches GitHub, fast-forwards only, installs
dependencies if `requirements.txt` changed, runs the tests, and only then restarts `tcp-server`.
A failing test leaves the running service untouched. A restart disconnects every client, so the
script refuses to run during minutes :00-:09 unless given `--force`.

## TCP keepalive

Every accepted data socket gets TCP keepalive so the relay notices a peer that disappeared without
closing (a modem that lost power or coverage). Defaults, settable in `/etc/default/tcp-server`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `KEEPALIVE_IDLE` | `60` | Seconds of silence before the first probe. |
| `KEEPALIVE_INTERVAL` | `15` | Seconds between probes. |
| `KEEPALIVE_COUNT` | `4` | Unanswered probes before the connection is dropped. |

`TCP_USER_TIMEOUT` is set to `IDLE + INTERVAL x COUNT` (120 s by default), which also drops a connection
whose forwarded data stays unacknowledged that long. The event log then shows
`Dead connection reaped on port N: <peer> stopped answering TCP keepalive` with the disconnect reason
`Dead connection (keepalive timeout)`. Probes are empty TCP segments handled by the kernel and never
appear in the PakBus stream or the traffic monitor. They use a little cellular data on idle connections
(one small probe and reply per minute with the defaults).
