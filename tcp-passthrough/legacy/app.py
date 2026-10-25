import asyncio
import logging
import signal
import sys
import threading
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QColor, QAction
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

# -----------------------
# Configuration
# -----------------------
HOST = "0.0.0.0"
BASE_PORTS = list(range(300, 380))
ADDITIONAL_PORTS = [6785, 623, 724, 721, 723]
COMMAND_PORT = 622
PORTS = sorted(set(BASE_PORTS + ADDITIONAL_PORTS))

LOG_TO_FILE = False
LOG_FILE_NAME = "tcp_server.log"
MAX_EVENTS_PER_PORT = 500
TABLE_REFRESH_MS = 1000
AUTOSCROLL_DEBUG = True
READ_CHUNK_SIZE = 65536
LOG_PAYLOADS = False
# Disable per-chunk logging by default for high-throughput ports.
LOG_CHUNK_EVENTS = False


# -----------------------
# Logging Setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("tcp_callback_server_gui")

if LOG_TO_FILE:
    file_handler = logging.FileHandler(LOG_FILE_NAME)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    logger.addHandler(file_handler)


# -----------------------
# State Models
# -----------------------
@dataclass
class ClientInfo:
    writer: asyncio.StreamWriter
    peer: str
    ip: str
    port: int
    connected_at: datetime
    last_activity: datetime
    bytes_in: int = 0
    bytes_out: int = 0


@dataclass
class PortState:
    port: int
    clients: List[ClientInfo] = field(default_factory=list)
    reject_count: int = 0
    duplicate_ip_count: int = 0
    last_event: str = ""
    last_event_time: Optional[datetime] = None
    recent_events: deque = field(default_factory=lambda: deque(maxlen=MAX_EVENTS_PER_PORT))
    listening: bool = False


class SharedState:
    def __init__(self, ports: List[int]):
        self._lock = threading.Lock()
        self._ports: Dict[int, PortState] = {port: PortState(port=port) for port in ports}

    def add_event(self, port: int, level: str, message: str):
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._ports[port]
            text = f"{now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} [{level}] {message}"
            state.recent_events.append(text)
            state.last_event = message
            state.last_event_time = now

    def set_listening(self, port: int, listening: bool):
        with self._lock:
            self._ports[port].listening = listening

    def increment_reject(self, port: int):
        with self._lock:
            self._ports[port].reject_count += 1

    def increment_duplicate_ip(self, port: int):
        with self._lock:
            self._ports[port].duplicate_ip_count += 1

    def upsert_client(self, port: int, client: ClientInfo):
        with self._lock:
            state = self._ports[port]
            for idx, existing in enumerate(state.clients):
                if existing.writer == client.writer:
                    state.clients[idx] = client
                    return
            state.clients.append(client)

    def remove_client(self, port: int, writer: asyncio.StreamWriter):
        with self._lock:
            state = self._ports[port]
            state.clients = [c for c in state.clients if c.writer != writer]

    def update_client_activity(
        self,
        port: int,
        writer: asyncio.StreamWriter,
        *,
        bytes_in: int = 0,
        bytes_out: int = 0,
    ):
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._ports[port]
            for client in state.clients:
                if client.writer == writer:
                    client.last_activity = now
                    client.bytes_in += bytes_in
                    client.bytes_out += bytes_out
                    return

    def clear_events(self, port: int):
        with self._lock:
            self._ports[port].recent_events.clear()
            self._ports[port].last_event = ""
            self._ports[port].last_event_time = None

    def snapshot(self):
        with self._lock:
            data = {}
            for port, state in self._ports.items():
                data[port] = {
                    "port": port,
                    "listening": state.listening,
                    "reject_count": state.reject_count,
                    "duplicate_ip_count": state.duplicate_ip_count,
                    "last_event": state.last_event,
                    "last_event_time": state.last_event_time,
                    "clients": [
                        {
                            "peer": c.peer,
                            "ip": c.ip,
                            "connected_at": c.connected_at,
                            "last_activity": c.last_activity,
                            "bytes_in": c.bytes_in,
                            "bytes_out": c.bytes_out,
                        }
                        for c in state.clients
                    ],
                    "recent_events": list(state.recent_events),
                }
            return data


# -----------------------
# TCP Server Backend
# -----------------------
class TcpDashboardServer:
    def __init__(self, host: str, ports: List[int], command_port: int, shared_state: SharedState):
        self.host = host
        self.ports = ports
        self.command_port = command_port
        self.shared_state = shared_state
        self.clients: Dict[int, List[asyncio.StreamWriter]] = {port: [] for port in ports}
        self.servers = []
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self.stop_event: Optional[asyncio.Event] = None

    @staticmethod
    def client_id(writer: asyncio.StreamWriter) -> str:
        peer = writer.get_extra_info("peername")
        if isinstance(peer, tuple) and len(peer) >= 2:
            return f"{peer[0]}:{peer[1]}"
        return "unknown"

    @staticmethod
    def client_ip(writer: asyncio.StreamWriter) -> str:
        peer = writer.get_extra_info("peername")
        if isinstance(peer, tuple) and len(peer) >= 1:
            return str(peer[0])
        return "unknown"

    def log_event(self, port: int, level: str, message: str):
        self.shared_state.add_event(port, level, message)
        log_fn = getattr(logger, level.lower(), logger.info)
        log_fn(message)

    async def disconnect_client(self, port: int, writer: asyncio.StreamWriter, reason: str):
        peer = self.client_id(writer)
        try:
            writer.write(reason.encode() + b"\n")
            await writer.drain()
        except Exception:
            pass
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        self.clients[port] = [c for c in self.clients.get(port, []) if c != writer]
        self.shared_state.remove_client(port, writer)
        self.log_event(port, "INFO", f"Disconnected {peer} on port {port}. Reason: {reason}")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int):
        ip = self.client_ip(writer)
        peer = self.client_id(writer)

        duplicates = [c for c in self.clients[port] if self.client_ip(c) == ip]
        if duplicates:
            self.shared_state.increment_duplicate_ip(port)
            self.log_event(port, "WARNING", f"Duplicate IP detected: {ip} on port {port}. Disconnecting existing clients from this IP.")
            for dup in list(duplicates):
                await self.disconnect_client(port, dup, "Duplicate IP detected.")

        if len(self.clients[port]) >= 2:
            self.shared_state.increment_reject(port)
            self.log_event(port, "WARNING", f"Connection limit reached on port {port}. Rejecting {peer}")
            await self.disconnect_client(port, writer, "Connection limit reached (max 2).")
            return

        self.clients[port].append(writer)
        now = datetime.now(timezone.utc)
        client = ClientInfo(
            writer=writer,
            peer=peer,
            ip=ip,
            port=port,
            connected_at=now,
            last_activity=now,
        )
        self.shared_state.upsert_client(port, client)
        self.log_event(port, "INFO", f"Client connected on port {port}: {peer} (total {len(self.clients[port])})")

        try:
            while not reader.at_eof():
                data = await reader.read(READ_CHUNK_SIZE)
                if not data:
                    break

                self.shared_state.update_client_activity(port, writer, bytes_in=len(data))
                if LOG_CHUNK_EVENTS:
                    if LOG_PAYLOADS:
                        decoded = data.decode(errors="ignore").strip()
                        self.log_event(
                            port,
                            "INFO",
                            f"Data received on port {port} from {peer} -> {decoded or '<binary/empty>'}",
                        )
                    else:
                        self.log_event(
                            port,
                            "INFO",
                            f"Received {len(data)} bytes on port {port} from {peer}",
                        )

                forwarded = False
                for other in list(self.clients[port]):
                    if other != writer:
                        try:
                            other.write(data)
                            await other.drain()
                            self.shared_state.update_client_activity(port, other, bytes_out=len(data))
                            if LOG_CHUNK_EVENTS:
                                self.log_event(
                                    port,
                                    "INFO",
                                    f"Forwarded {len(data)} bytes from {peer} to {self.client_id(other)} on port {port}",
                                )
                            forwarded = True
                        except Exception as exc:
                            self.log_event(port, "ERROR", f"Forwarding error on port {port} to {self.client_id(other)}: {exc}")
                            await self.disconnect_client(port, other, "Forwarding error")

                if not forwarded and LOG_CHUNK_EVENTS:
                    self.log_event(port, "INFO", f"No other client connected on port {port}. Data not forwarded.")

        except Exception as exc:
            self.log_event(port, "ERROR", f"Error on port {port} with {peer}: {exc}")
        finally:
            await self.disconnect_client(port, writer, "Client disconnected")

    async def handle_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = self.client_id(writer)
        try:
            data = await reader.read(1024)
            if not data:
                return
            message = data.decode(errors="ignore").strip()
            if message.startswith("RESET="):
                try:
                    port_to_reset = int(message.split("=", 1)[1])
                except ValueError:
                    writer.write(b"Invalid port number.\n")
                    await writer.drain()
                    return
                if port_to_reset in self.ports:
                    self.log_event(port_to_reset, "INFO", f"Command server request from {peer} to reset port {port_to_reset}")
                    await self.reset_port(port_to_reset, reason="Disconnected by RESET command.")
                    writer.write(f"Port {port_to_reset} reset successfully.\n".encode())
                else:
                    writer.write(f"Invalid port: {port_to_reset}\n".encode())
            else:
                writer.write(b"Invalid command. Use RESET=[port]\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def timed_disconnect(self):
        while not self.stop_event.is_set():
            now = datetime.now(timezone.utc)
            if now.minute == 30 and now.second < 5:
                logger.info("30-minute mark reached. Disconnecting all clients.")
                for port in self.ports:
                    await self.reset_port(port, reason="Disconnected at 30-minute mark by server policy.")
                    self.log_event(port, "INFO", "30-minute policy disconnect executed.")
            await asyncio.sleep(5)

    async def reset_port(self, port: int, reason: str = "Port reset"):
        for client in list(self.clients.get(port, [])):
            await self.disconnect_client(port, client, reason)
        self.clients[port] = []
        self.log_event(port, "INFO", f"Port {port} reset complete")

    async def start_servers(self):
        for port in self.ports:
            server = await asyncio.start_server(
                lambda r, w, p=port: self.handle_client(r, w, p),
                self.host,
                port,
            )
            self.servers.append(server)
            self.shared_state.set_listening(port, True)
            self.log_event(port, "INFO", f"TCP server is listening on {self.host}:{port}")

        cmd_server = await asyncio.start_server(self.handle_command, self.host, self.command_port)
        self.servers.append(cmd_server)
        logger.info(f"Command server is listening on {self.host}:{self.command_port}")

    async def stop_servers(self):
        for port in self.ports:
            await self.reset_port(port, reason="Server shutdown")
            self.shared_state.set_listening(port, False)
        for server in self.servers:
            server.close()
            await server.wait_closed()
        self.servers.clear()
        logger.info("All servers shut down.")

    async def run_async(self):
        self.stop_event = asyncio.Event()
        await self.start_servers()
        asyncio.create_task(self.timed_disconnect())
        await self.stop_event.wait()
        await self.stop_servers()

    def _thread_target(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.run_async())
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            try:
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            self.loop.close()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._thread_target, daemon=True)
        self.thread.start()

    def stop(self):
        if self.loop and self.stop_event:
            self.loop.call_soon_threadsafe(self.stop_event.set)
        if self.thread:
            self.thread.join(timeout=5)

    def request_reset_port(self, port: int):
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(
            self.reset_port(port, reason="Disconnected by GUI reset action."),
            self.loop,
        )


# -----------------------
# GUI
# -----------------------
class MainWindow(QMainWindow):
    def __init__(self, server: TcpDashboardServer, shared_state: SharedState):
        super().__init__()
        self.server = server
        self.shared_state = shared_state
        self.selected_port: Optional[int] = PORTS[0] if PORTS else None
        self.last_debug_text = ""
        self.autoscroll_debug = AUTOSCROLL_DEBUG

        self.setWindowTitle("TCP Port Dashboard")
        self.resize(1500, 850)

        self._build_ui()
        self._build_menu()
        self._connect_signals()

        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.refresh_ui)
        self.refresh_timer.start(TABLE_REFRESH_MS)

        self.refresh_ui()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QVBoxLayout(central)
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)

        # Left panel
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        self.summary_label = QLabel()
        self.summary_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        left_layout.addWidget(self.summary_label)

        self.table = QTableWidget(len(PORTS), 9)
        self.table.setHorizontalHeaderLabels([
            "Port",
            "Listening",
            "Clients",
            "Client 1",
            "Client 1 Time",
            "Client 2",
            "Client 2 Time",
            "Rejects",
            "Duplicate IPs",
        ])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        left_layout.addWidget(self.table)

        # Right panel
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

        detail_group = QGroupBox("Selected Port Details")
        detail_layout = QGridLayout(detail_group)
        self.detail_port = QLabel("-")
        self.detail_listening = QLabel("-")
        self.detail_clients = QLabel("-")
        self.detail_last_event = QLabel("-")
        self.detail_last_event.setWordWrap(True)

        detail_layout.addWidget(QLabel("Port:"), 0, 0)
        detail_layout.addWidget(self.detail_port, 0, 1)
        detail_layout.addWidget(QLabel("Listening:"), 1, 0)
        detail_layout.addWidget(self.detail_listening, 1, 1)
        detail_layout.addWidget(QLabel("Clients:"), 2, 0)
        detail_layout.addWidget(self.detail_clients, 2, 1)
        detail_layout.addWidget(QLabel("Last event:"), 3, 0)
        detail_layout.addWidget(self.detail_last_event, 3, 1)
        right_layout.addWidget(detail_group)

        button_row = QHBoxLayout()
        self.reset_button = QPushButton("Reset Selected Port")
        self.clear_debug_button = QPushButton("Clear Debug")
        self.autoscroll_checkbox = QCheckBox("Auto-scroll debug")
        self.autoscroll_checkbox.setChecked(self.autoscroll_debug)
        button_row.addWidget(self.reset_button)
        button_row.addWidget(self.clear_debug_button)
        button_row.addWidget(self.autoscroll_checkbox)
        button_row.addStretch()
        right_layout.addLayout(button_row)

        self.debug_text = QTextEdit()
        self.debug_text.setReadOnly(True)
        right_layout.addWidget(self.debug_text)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([950, 550])

    def _build_menu(self):
        file_menu = self.menuBar().addMenu("File")
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

    def _connect_signals(self):
        self.table.itemSelectionChanged.connect(self._on_row_selected)
        self.reset_button.clicked.connect(self._reset_selected_port)
        self.clear_debug_button.clicked.connect(self._clear_selected_debug)
        self.autoscroll_checkbox.stateChanged.connect(self._set_autoscroll)

    def _set_autoscroll(self, state: int):
        self.autoscroll_debug = state == Qt.Checked

    def _on_row_selected(self):
        row = self.table.currentRow()
        if row < 0 or row >= len(PORTS):
            return
        item = self.table.item(row, 0)
        if item is None:
            return
        self.selected_port = int(item.text())
        self.refresh_ui()

    def _reset_selected_port(self):
        if self.selected_port is None:
            return
        self.server.request_reset_port(self.selected_port)

    def _clear_selected_debug(self):
        if self.selected_port is None:
            return
        self.shared_state.clear_events(self.selected_port)
        self.debug_text.clear()

    @staticmethod
    def _format_duration(start: Optional[datetime]) -> str:
        if not start:
            return "-"
        delta = datetime.now(timezone.utc) - start
        total = int(delta.total_seconds())
        hours, remainder = divmod(total, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def refresh_ui(self):
        snapshot = self.shared_state.snapshot()

        ports_with_zero = 0
        ports_with_one = 0
        ports_with_two = 0
        total_rejects = 0
        total_duplicates = 0

        for row, port in enumerate(PORTS):
            state = snapshot[port]
            clients = state["clients"]
            client_count = len(clients)
            total_rejects += state["reject_count"]
            total_duplicates += state["duplicate_ip_count"]

            if client_count == 0:
                ports_with_zero += 1
            elif client_count == 1:
                ports_with_one += 1
            else:
                ports_with_two += 1

            values = [
                str(port),
                "Yes" if state["listening"] else "No",
                str(client_count),
                clients[0]["peer"] if len(clients) >= 1 else "-",
                self._format_duration(clients[0]["connected_at"]) if len(clients) >= 1 else "-",
                clients[1]["peer"] if len(clients) >= 2 else "-",
                self._format_duration(clients[1]["connected_at"]) if len(clients) >= 2 else "-",
                str(state["reject_count"]),
                str(state["duplicate_ip_count"]),
            ]

            for col, value in enumerate(values):
                item = self.table.item(row, col)
                if item is None:
                    item = QTableWidgetItem()
                    self.table.setItem(row, col, item)
                item.setText(value)
                item.setTextAlignment(Qt.AlignCenter)

            row_color = None
            if client_count == 0:
                row_color = QColor(245, 245, 245)
            elif client_count == 1:
                row_color = QColor(255, 244, 204)
            elif client_count == 2:
                row_color = QColor(212, 237, 218)

            if state["reject_count"] > 0 or state["duplicate_ip_count"] > 0:
                row_color = QColor(255, 230, 230)

            for col in range(self.table.columnCount()):
                self.table.item(row, col).setBackground(row_color)

        self.table.resizeColumnsToContents()

        self.summary_label.setText(
            f"Ports: {len(PORTS)} | Empty: {ports_with_zero} | One client: {ports_with_one} | "
            f"Two clients: {ports_with_two} | Rejected connections: {total_rejects} | Duplicate IP events: {total_duplicates}"
        )

        if self.selected_port is None and PORTS:
            self.selected_port = PORTS[0]

        if self.selected_port is not None and self.selected_port in snapshot:
            state = snapshot[self.selected_port]
            self.detail_port.setText(str(self.selected_port))
            self.detail_listening.setText("Yes" if state["listening"] else "No")

            if state["clients"]:
                client_lines = []
                for idx, client in enumerate(state["clients"], start=1):
                    client_lines.append(
                        f"Client {idx}: {client['peer']} | Connected: {self._format_duration(client['connected_at'])} | "
                        f"Last activity: {self._format_duration(client['last_activity'])} ago? | "
                        f"Bytes in: {client['bytes_in']} | Bytes out: {client['bytes_out']}"
                    )
                self.detail_clients.setText("\n".join(client_lines))
            else:
                self.detail_clients.setText("No clients connected")

            self.detail_last_event.setText(state["last_event"] or "-")
            debug_text = "\n".join(state["recent_events"])
            if debug_text != self.last_debug_text:
                self.debug_text.setPlainText(debug_text)
                self.last_debug_text = debug_text
                if self.autoscroll_debug:
                    scrollbar = self.debug_text.verticalScrollBar()
                    scrollbar.setValue(scrollbar.maximum())

        # Ensure some row is selected
        if self.table.currentRow() < 0 and PORTS:
            self.table.selectRow(0)

    def closeEvent(self, event):
        reply = QMessageBox.question(
            self,
            "Exit",
            "Stop the TCP server and close the dashboard?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if reply == QMessageBox.Yes:
            self.server.stop()
            event.accept()
        else:
            event.ignore()


# -----------------------
# Entry Point
# -----------------------
def main():
    shared_state = SharedState(PORTS)
    server = TcpDashboardServer(HOST, PORTS, COMMAND_PORT, shared_state)
    server.start()

    app = QApplication(sys.argv)
    window = MainWindow(server, shared_state)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
