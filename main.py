import asyncio
import logging
import signal
from config import HOST, PORTS, COMMAND_PORT, WEB_HOST, WEB_PORT, TRUSTED_NETWORKS
from shared_state import SharedState
from site_config import SiteConfig
from tcp_server import TcpDashboardServer
from system_monitor import SystemMonitor
from web_dashboard import create_app, DashboardThread

async def main():
    sites = SiteConfig()
    state = SharedState(PORTS)
    backend = TcpDashboardServer(HOST, PORTS, COMMAND_PORT, state, TRUSTED_NETWORKS)
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)
    await backend.start()
    dashboard = None
    try:
        app = create_app(backend, state, SystemMonitor(), TRUSTED_NETWORKS, sites)
        dashboard = DashboardThread(app, WEB_HOST, WEB_PORT)
        dashboard.start()
        logging.info('Dashboard listening on %s:%s', WEB_HOST, WEB_PORT)
        await shutdown.wait()
    finally:
        if dashboard:
            dashboard.stop()
            dashboard.join(timeout=5)
        await backend.stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
