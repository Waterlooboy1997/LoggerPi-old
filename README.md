# DietPi TCP forwarding server

Headless migration of `app.py`. Python asyncio TCP listeners, Flask/Waitress LAN dashboard, systemd management. Supports Python 3.9+ (DietPi's actual Python version should be checked with `python3 --version`).

## Original behaviour retained

- Data ports: 300–379, 6785, 623, 724, 721, 723 (85 total); command port 622.
- Maximum two accepted clients per data port. A new connection from the same IP evicts existing connections from that IP **on that port** before the capacity check.
- Binary data is forwarded in both directions between the two clients, without modification or buffering for clients that are absent.
- Disconnect reason messages remain newline-terminated; remote commands remain `RESET=<port>` with the original response text.
- Reset closes connected clients but leaves listening sockets open. Scheduled reset disconnects at minute 30 each hour.
- Per-port history holds up to 500 events; chunk/payload logging disabled by default. Reject/duplicate counters persist until process restart.
- Client traffic counters disappear when a client disconnects; new cumulative traffic counters remain until process restart.

## Deliberate reliability changes

- Admission lock per port prevents simultaneous connections bypassing capacity or duplicate-IP policy.
- Writer drain/close operations have two-second deadlines; a stalled peer cannot indefinitely block cleanup.
- Client removal happens before awaiting network shutdown, preventing further forwarding to a closing client.
- Timed reset uses the next actual minute-30 deadline, avoiding the original narrow polling window.
- Port-bind failures abort startup and close previously opened listeners; shutdown cancels tracked handlers.
- Remote command connections time out after ten seconds without data; source IP must match `TRUSTED_NETWORKS`.
- A separate Disconnect action closes clients without writing the `Port X reset complete` event.

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
- **CPU temperature N/A:** DietPi kernel may not expose `/sys/class/thermal/thermal_zone0/temp`.
- **All clients disconnect at minute 30:** intentional inherited policy.

## Notes

- HTTP and TCP share one process but use different threads. The dashboard is not needed for forwarding to continue; a fatal process crash still affects both.
- Data port bindings remain `0.0.0.0` by default. The dashboard should bind to the Pi's actual LAN address and use firewall isolation.
- Per-client `bytes_out` counts bytes accepted by the asyncio transport after `drain`, not remote application acknowledgement.
- This migration includes concurrency fixes, so under unusual simultaneous-connection races the outcome may differ from the original buggy behaviour while preserving the intended rules.
