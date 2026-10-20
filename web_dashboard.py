import asyncio
import hmac
import ipaddress
import logging
import secrets
import threading
from flask import Flask, jsonify, render_template, request, abort
from waitress import create_server

logger = logging.getLogger('dashboard')

def create_app(server, state, monitor, trusted_networks, sites):
    app = Flask(__name__)
    networks = [ipaddress.ip_network(x, strict=False) for x in trusted_networks]
    token = secrets.token_urlsafe(32)

    @app.before_request
    def access_control():
        # Never trust proxy-provided IP headers. No reverse proxy is configured.
        try:
            allowed = any(ipaddress.ip_address(request.remote_addr) in n for n in networks)
        except (ValueError, TypeError):
            allowed = False
        if not allowed:
            abort(403)
        if request.method == 'POST':
            # A non-simple custom header + token prevents cross-site form requests.
            if request.headers.get('Origin'):
                from urllib.parse import urlsplit
                origin = urlsplit(request.headers['Origin'])
                if origin.netloc != request.host or origin.scheme != request.scheme:
                    abort(403)
            if not hmac.compare_digest(request.headers.get('X-Control-Token', ''), token):
                abort(403)

    @app.after_request
    def security_headers(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
        return response

    @app.get('/')
    def index():
        return render_template('dashboard.html', control_token=token)

    @app.get('/api/status')
    def status():
        ports = state.snapshot()
        names = sites.names()
        for port in ports:
            port['site'] = names.get(port['port'], '')
        counts = [len(p['clients']) for p in ports]
        return jsonify({'ports': ports, 'summary': {'total': len(ports), 'empty': counts.count(0),
                       'one': counts.count(1), 'two': counts.count(2),
                       'rejections': sum(p['reject_count'] for p in ports),
                       'duplicates': sum(p['duplicate_ip_count'] for p in ports)},
                       'traffic': state.totals(), 'system': monitor.snapshot()})

    def validated_port(port):
        if port not in server.clients:
            abort(404)
        return port

    @app.get('/api/ports/<int:port>')
    def detail(port):
        result = state.snapshot(validated_port(port))
        result['site'] = sites.names().get(port, '')
        return jsonify(result)

    @app.post('/api/ports/<int:port>/site')
    def update_site(port):
        validated_port(port)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict) or 'site' not in payload:
            return jsonify({'error': 'Expected a JSON object with a site string'}), 400
        try:
            name = sites.set_site(port, payload['site'])
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
        except OSError:
            logger.exception('Could not save site configuration')
            return jsonify({'error': 'Could not save site configuration; check file permissions and disk space'}), 500
        return jsonify({'ok': True, 'port': port, 'site': name})

    @app.get('/api/events')
    def events():
        try:
            since = max(0, int(request.args.get('since', 0)))
            limit = max(1, min(500, int(request.args.get('limit', 150))))
            port = request.args.get('port', '')
            port = validated_port(int(port)) if port else None
        except ValueError:
            abort(400)
        return jsonify({'events': state.get_events(port, since, limit)})

    @app.post('/api/ports/<int:port>/<action>')
    def control(port, action):
        validated_port(port)
        if action not in ('reset', 'disconnect', 'clear'):
            abort(404)
        if action == 'clear':
            state.clear_events(port)
            logger.info('Event history cleared for port %s', port)
        else:
            if not server.loop or not server.loop.is_running():
                return jsonify({'error': 'TCP backend unavailable'}), 503
            reason = 'Disconnected by GUI reset action.' if action == 'reset' else 'Disconnected by dashboard action.'
            future = asyncio.run_coroutine_threadsafe(server.reset_port(port, reason, record_reset=(action == 'reset')), server.loop)
            try:
                future.result(timeout=15)
            except TimeoutError:
                return jsonify({'error': 'Operation still pending; check port state'}), 504
            except Exception:
                logger.exception('Port control failed')
                return jsonify({'error': 'Port operation failed'}), 500
        return jsonify({'ok': True, 'port': port, 'action': action})
    return app

class DashboardThread(threading.Thread):
    def __init__(self, app, host, port):
        super().__init__(name='http-dashboard', daemon=True)
        self.http = create_server(app, host=host, port=port, threads=3, connection_limit=30)

    def run(self):
        self.http.run()

    def stop(self):
        self.http.close()
