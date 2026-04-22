import asyncio
import html
import json
import logging
import signal
import threading
from datetime import datetime, timezone
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import yaml
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext, ModbusSlaveContext
from pymodbus.server import StartAsyncTcpServer


_PURPLE = "\033[95m"
_RESET  = "\033[0m"

_HUAWEI_STATUS_MAP: dict[int, str] = {
    0x0000: "Standby: initializing",
    0x0001: "Standby: detecting insulation resistance",
    0x0002: "Standby: detecting irradiation",
    0x0003: "Standby: grid detecting",
    0x0100: "Starting",
    0x0200: "On-grid (running)",
    0x0201: "Grid connection: power limited",
    0x0202: "Grid connection: self-derating",
    0x0203: "Off-grid running",
    0x0300: "Shutdown: fault",
    0x0301: "Shutdown: command",
    0x0302: "Shutdown: OVGR",
    0x0303: "Shutdown: communication disconnected",
    0x0304: "Shutdown: power limited",
    0x0305: "Shutdown: manual startup required",
    0x0306: "Shutdown: DC switches disconnected",
    0x0307: "Shutdown: rapid cutoff",
    0x0308: "Shutdown: input under-power",
    0x0401: "Grid scheduling: cosphi-P curve",
    0x0402: "Grid scheduling: Q-U curve",
    0x0403: "Grid scheduling: PF-U curve",
    0x0404: "Grid scheduling: dry contact",
    0x0405: "Grid scheduling: Q-P curve",
    0x0500: "Spot-check ready",
    0x0501: "Spot-checking",
    0x0600: "Inspecting",
    0x0700: "AFCI self-check",
    0x0800: "I-V scanning",
    0x0900: "DC input detection",
    0x0A00: "Running: off-grid charging",
    0xA000: "Standby: no irradiation",
}


class _LoggingSlaveContext(ModbusSlaveContext):
    """Wraps ModbusSlaveContext to log every read/write the PCS makes via Modbus TCP.

    Internal writes (our own heartbeat/control loop calling setValues directly) are
    logged at DEBUG so they don't flood the log.  Reads triggered by an external client
    (i.e. the PCS polling the EMS server) are logged at INFO so they stand out clearly.
    """

    # Maps pymodbus internal register-type codes to human names.
    _REG_NAMES = {1: "Coil", 2: "DiscreteInput", 3: "HoldingReg", 4: "InputReg"}

    def __init__(
        self,
        *args: Any,
        register_labels: dict[int, dict[int, str]] | None = None,
        on_external_read: Any = None,
        on_external_write: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._register_labels = register_labels or {}
        self._on_external_read = on_external_read
        self._on_external_write = on_external_write

    def _label_range(self, reg_type: int, address: int, count: int) -> str:
        labels = self._register_labels.get(reg_type, {})
        parts: list[str] = []
        for addr in range(address, address + max(1, count)):
            label = labels.get(addr)
            if label:
                parts.append(f"{addr}:{label}")
        return ", ".join(parts) if parts else "unmapped"

    def getValues(self, fc_as_hex: int, address: int, count: int = 1) -> list:
        values = super().getValues(fc_as_hex, address, count)
        reg = self._REG_NAMES.get(fc_as_hex, f"reg{fc_as_hex}")
        reg_labels = self._label_range(fc_as_hex, address, count)
        action = "PCS-CMD-READ" if fc_as_hex == 3 else "PCS-INPUT-READ" if fc_as_hex == 4 else "PCS-READ"
        logging.getLogger("pibems").info(
            "%sEMS-SERVER  %s  %s  addr=%s count=%s labels=[%s]  -> %s%s",
            _PURPLE, action, reg, address, count, reg_labels, values, _RESET,
        )
        if self._on_external_read is not None:
            self._on_external_read(fc_as_hex, address, count, values)
        return values

    def setValues(self, fc_as_hex: int, address: int, values: list, *, _internal: bool = False) -> None:  # type: ignore[override]
        reg = self._REG_NAMES.get(fc_as_hex, f"reg{fc_as_hex}")
        reg_labels = self._label_range(fc_as_hex, address, len(values))
        if _internal:
            logging.getLogger("pibems").debug(
                "%sEMS-SERVER  INTERNAL-WRITE  %s  addr=%s labels=[%s] values=%s%s",
                _PURPLE, reg, address, reg_labels, values, _RESET,
            )
        else:
            logging.getLogger("pibems").info(
                "%sEMS-SERVER  PCS-WRITE  %s  addr=%s labels=[%s] values=%s%s",
                _PURPLE, reg, address, reg_labels, values, _RESET,
            )
            if self._on_external_write is not None:
                self._on_external_write(fc_as_hex, address, values)
        super().setValues(fc_as_hex, address, values)

    def snapshot_addresses(self, reg_type: int, addresses: list[int]) -> dict[int, int]:
        values: dict[int, int] = {}
        for address in sorted(set(addresses)):
            try:
                raw = super().getValues(reg_type, address, 1)
            except Exception:
                continue
            if raw:
                values[address] = int(raw[0])
        return values

_LOG = logging.getLogger("pibems")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
# Show pymodbus server connection events (client connect/disconnect)
logging.getLogger("pymodbus.server").setLevel(logging.DEBUG)
logging.getLogger("pymodbus").setLevel(logging.WARNING)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Options:
    huawei_host: str = "192.168.1.20"
    huawei_port: int = 502
    huawei_unit_id: int = 1
    huawei_address_offset: int = 0
    huawei_max_power_kw: float = 50.0

    pcs_host: str = "192.168.1.100"
    pcs_port: int = 502
    pcs_unit_id: int = 1
    pcs_address_offset: int = 0

    ems_bind_host: str = "0.0.0.0"
    ems_bind_port: int = 1502
    ems_unit_id: int = 1

    api_bind_host: str = "0.0.0.0"
    api_bind_port: int = 8080

    enable_huawei: bool = True
    enable_pcs_direct: bool = True
    enable_ems_server: bool = True

    poll_interval_sec: float = 1.0
    control_interval_sec: float = 1.0
    heartbeat_interval_sec: float = 1.0

    target_power_kw: float = 0.0
    huawei_derate_percent: float = 100.0

    max_charge_kw: float = 50.0
    max_discharge_kw: float = 50.0
    min_soc: int = 15
    max_soc: int = 95
    operation_mode: str = "control"

    grid_voltage_present_threshold_v: float = 180.0
    outage_reserve_soc: int = 25
    outage_max_soc_from_huawei: int = 85
    outage_shutdown_soc_no_sun: int = 15
    solar_present_threshold_kw: float = 1.0
    charge_from_grid_on_return: bool = True
    auto_start_on_grid_return: bool = True

    dynamic_charge_limit_enable: bool = True
    dynamic_charge_limit_margin: float = 0.9

    pcs_start_command: int = 0x5555
    pcs_stop_command: int = 0xAAAA

    register_map_file: str = "/app/config/register_map.yaml"

class EMSService:
    def __init__(self, opts: Options, register_map: dict[str, Any]) -> None:
        self.opts = opts
        self.map = register_map
        self.stop_event = asyncio.Event()
        self.heartbeat_counter = 0
        self.loop: asyncio.AbstractEventLoop | None = None  # Will be set in run()

        self.state: dict[str, Any] = {
            "status": {
                "huawei_connected": False,
                "pcs_connected": False,
                "huawei_last_error": None,
                "pcs_last_error": None,
                "huawei_last_error_time": None,
                "pcs_last_error_time": None,
            },
            "huawei": {},
            "pcs": {},
            "control": {
                "target_power_kw": float(opts.target_power_kw),
                "huawei_derate_percent": float(opts.huawei_derate_percent),
                "auto_mode_enabled": True,
                "operation_mode": opts.operation_mode,
                "last_written_pcs_power_kw": 0.0,
                "effective_target_power_kw": 0.0,
                "dynamic_charge_limit_kw": opts.max_charge_kw,
                "policy_reason": "init",
            },
            "server": {
                "enabled": opts.enable_ems_server,
                "unit_id": opts.ems_unit_id,
                "port": opts.ems_bind_port,
                "running": False,
                "connected_clients": 0,
                "last_input_read": None,
                "last_holding_read": None,
                "last_write": None,
                "input_read_count": 0,
                "holding_read_count": 0,
                "write_count": 0,
                "address_view": {
                    "input": [],
                    "holding": [],
                },
            },
            "pcs_direct_probe": {
                "enabled": opts.enable_pcs_direct,
                "last_ok": None,
                "last_error": None,
                "last_heartbeat": None,
            },
            "grid": {
                "is_available": True,
                "last_transition": "init",
            },
            "errors": [],
        }
        self._last_grid_available: bool | None = None

        self.huawei_client = AsyncModbusTcpClient(host=opts.huawei_host, port=opts.huawei_port)
        self.pcs_client = AsyncModbusTcpClient(host=opts.pcs_host, port=opts.pcs_port)

        self.server_ctx: ModbusServerContext | None = None
        self.server_store: _LoggingSlaveContext | None = None
        self.server_register_labels = self._build_ems_register_labels()
        self.httpd: ThreadingHTTPServer | None = None
        self.http_thread: threading.Thread | None = None

    def _build_ems_register_labels(self) -> dict[int, dict[int, str]]:
        labels: dict[int, dict[int, str]] = {3: {}, 4: {}}

        pcs_map = self.map.get("pcs", {})
        for section_name in ("status", "limits", "control"):
            section = pcs_map.get(section_name, {})
            if not isinstance(section, dict):
                continue
            for point_name, point in section.items():
                if not isinstance(point, dict):
                    continue
                ptype = str(point.get("type", "")).strip().lower()
                address = point.get("address")
                quantity = int(point.get("quantity", 1))
                if not isinstance(address, int):
                    continue

                reg_type = 4 if ptype == "input" else 3 if ptype == "holding" else None
                if reg_type is None:
                    continue

                for i in range(max(1, quantity)):
                    addr = address + i
                    suffix = f"[{i}]" if quantity > 1 else ""
                    labels[reg_type][addr] = f"pcs.{section_name}.{point_name}{suffix}"

        ems_defaults = self.map.get("ems_server", {})
        input_defaults = ems_defaults.get("input_defaults", {}) if isinstance(ems_defaults, dict) else {}
        holding_defaults = ems_defaults.get("holding_defaults", {}) if isinstance(ems_defaults, dict) else {}
        for raw_addr in input_defaults:
            try:
                addr = int(raw_addr)
            except (TypeError, ValueError):
                continue
            labels[4].setdefault(addr, f"ems_server.input_defaults.{addr}")
        for raw_addr in holding_defaults:
            try:
                addr = int(raw_addr)
            except (TypeError, ValueError):
                continue
            labels[3].setdefault(addr, f"ems_server.holding_defaults.{addr}")

        return labels

    def _refresh_server_address_view(self) -> None:
        if self.server_store is None:
            return

        def build_entries(reg_type: int) -> list[dict[str, Any]]:
            labels = self.server_register_labels.get(reg_type, {})
            values = self.server_store.snapshot_addresses(reg_type, list(labels.keys()))
            entries: list[dict[str, Any]] = []
            for address in sorted(labels):
                value = values.get(address)
                entries.append({
                    "address": address,
                    "label": labels[address],
                    "value": value,
                })
            return entries

        self.state["server"]["address_view"] = {
            "input": build_entries(4),
            "holding": build_entries(3),
        }

    def _on_pcs_ems_contact(self, reg_type: int, address: int, count: int, values: list[int]) -> None:
        """Called whenever the PCS successfully reads from our EMS Modbus server."""
        self.state["status"]["pcs_connected"] = True
        self.state["status"]["pcs_last_error"] = None
        key = "last_holding_read" if reg_type == 3 else "last_input_read"
        count_key = "holding_read_count" if reg_type == 3 else "input_read_count"
        self.state["server"][count_key] = int(self.state["server"].get(count_key, 0)) + 1
        self.state["server"][key] = {
            "address": address,
            "count": count,
            "values": list(values),
            "time": _now_iso(),
        }
        self._refresh_server_address_view()

    def _on_pcs_ems_write(self, reg_type: int, address: int, values: list[int]) -> None:
        self.state["server"]["write_count"] = int(self.state["server"].get("write_count", 0)) + 1
        self.state["server"]["last_write"] = {
            "reg_type": reg_type,
            "address": address,
            "count": len(values),
            "values": list(values),
            "time": _now_iso(),
        }
        self._refresh_server_address_view()

    def _health_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "huawei_enabled": self.opts.enable_huawei,
            "huawei_connected": self.state["status"].get("huawei_connected", False),
            "pcs_enabled": self.opts.enable_pcs_direct,
            "pcs_connected": self.state["status"].get("pcs_connected", False),
            "ems_server_enabled": self.opts.enable_ems_server,
            "operation_mode": self.state["control"].get("operation_mode", "control"),
            "last_huawei_error": self.state["status"].get("huawei_last_error"),
            "last_pcs_error": self.state["status"].get("pcs_last_error"),
        }

    def _decode_huawei_status(self, code: int) -> str:
        return _HUAWEI_STATUS_MAP.get(code, f"Unknown ({code})")

    def _get_scanner_html(self) -> str:
        """Generate HTML for Modbus register scanner UI."""
        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PIBEMS Modbus Scanner</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: white;
            margin-bottom: 20px;
            text-align: center;
            font-size: 2em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .back-link {
            color: white;
            text-decoration: none;
            font-size: 14px;
            margin-bottom: 10px;
            display: inline-block;
        }
        .back-link:hover {
            text-decoration: underline;
        }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .form-group {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr 1fr auto;
            gap: 10px;
            align-items: flex-end;
            margin-bottom: 20px;
        }
        label {
            display: block;
            font-weight: 600;
            margin-bottom: 5px;
            font-size: 14px;
        }
        input, select {
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        input:focus, select:focus {
            outline: none;
            border-color: #1e3c72;
            box-shadow: 0 0 0 2px rgba(30,60,114,0.1);
        }
        button {
            background: #2a5298;
            color: white;
            padding: 8px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 600;
            font-size: 14px;
        }
        button:hover {
            background: #1e3c72;
        }
        button:disabled {
            background: #ccc;
            cursor: not-allowed;
        }
        .loading {
            text-align: center;
            padding: 20px;
            color: #666;
        }
        .spinner {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid #f3f3f3;
            border-top: 3px solid #2a5298;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .results {
            margin-top: 20px;
        }
        .results-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }
        .results-table thead {
            background: #f5f5f5;
            font-weight: 600;
        }
        .results-table th {
            padding: 10px;
            text-align: left;
            border-bottom: 2px solid #ddd;
        }
        .results-table td {
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }
        .results-table tr:hover {
            background: #fafafa;
        }
        .success {
            background: #d4edda;
            color: #155724;
            font-weight: 600;
        }
        .failed {
            background: #f8d7da;
            color: #721c24;
            font-weight: 600;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 20px;
        }
        .stat-box {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            text-align: center;
            border-left: 4px solid #2a5298;
        }
        .stat-label {
            font-size: 12px;
            color: #666;
            margin-bottom: 5px;
        }
        .stat-value {
            font-size: 24px;
            font-weight: 700;
            color: #2a5298;
        }
        .error {
            background: #fff3cd;
            color: #856404;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 10px;
        }
        .success-msg {
            background: #d4edda;
            color: #155724;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <a href="/" class="back-link">← Back to Dashboard</a>
        <h1>Modbus Register Scanner</h1>
        
        <div class="card">
            <div class="form-group">
                <div>
                    <label for="device">Device:</label>
                    <select id="device">
                        <option value="pcs">PCS</option>
                        <option value="huawei">Huawei</option>
                    </select>
                </div>
                <div>
                    <label for="regType">Register Type:</label>
                    <select id="regType">
                        <option value="input">Input</option>
                        <option value="holding">Holding</option>
                    </select>
                </div>
                <div>
                    <label for="startAddr">Start Address:</label>
                    <input type="number" id="startAddr" value="2100" min="0" max="65535">
                </div>
                <div>
                    <label for="endAddr">End Address:</label>
                    <input type="number" id="endAddr" value="2500" min="0" max="65535">
                </div>
                <button onclick="startScan()">Scan</button>
            </div>
            
            <div id="statusArea"></div>
            
            <div id="results"></div>
        </div>
    </div>

    <script>
        let scanInProgress = false;

        function startScan() {
            const device = document.getElementById('device').value;
            const regType = document.getElementById('regType').value;
            const startAddr = parseInt(document.getElementById('startAddr').value);
            const endAddr = parseInt(document.getElementById('endAddr').value);
            
            if (!device || startAddr === null || endAddr === null) {
                showError('Please fill in all fields');
                return;
            }
            
            if (startAddr > endAddr) {
                showError('Start address must be less than or equal to end address');
                return;
            }
            
            if (endAddr - startAddr > 1000) {
                showError('Scan range too large (max 1000 registers)');
                return;
            }
            
            scanInProgress = true;
            clearResults();
            showLoading('Scanning ' + (endAddr - startAddr + 1) + ' registers...');
            
            // Try both /scan and /api/scan paths for compatibility with HA ingress
            const scanPath = window.location.pathname.includes('/api/') ? '/api/scan' : '/scan';
            
            fetch(scanPath, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    device: device,
                    start_addr: startAddr,
                    end_addr: endAddr,
                    reg_type: regType
                })
            })
            .then(res => res.json())
            .then(data => {
                if (data.error) {
                    showError(data.error);
                } else {
                    displayResults(data);
                }
            })
            .catch(err => showError('Scan failed: ' + err))
            .finally(() => {
                scanInProgress = false;
            });
        }

        function displayResults(data) {
            const results = data.results || {};
            const addresses = Object.keys(results).map(Number).sort((a, b) => a - b);
            
            const successful = addresses.filter(addr => results[addr].success).length;
            const failed = addresses.length - successful;
            
            let html = '';
            html += '<div class="stats">';
            html += '<div class="stat-box"><div class="stat-label">Total</div><div class="stat-value">' + addresses.length + '</div></div>';
            html += '<div class="stat-box"><div class="stat-label">✓ Success</div><div class="stat-value" style="color: #28a745;">' + successful + '</div></div>';
            html += '<div class="stat-box"><div class="stat-label">✗ Failed</div><div class="stat-value" style="color: #dc3545;">' + failed + '</div></div>';
            html += '</div>';
            
            html += '<div class="success-msg">Scan completed for ' + data.device.toUpperCase() + ' (' + data.reg_type + ' registers) | Offset: ' + data.address_offset + '</div>';
            
            html += '<table class="results-table">';
            html += '<thead><tr><th>Address</th><th>Status</th><th>Value</th></tr></thead>';
            html += '<tbody>';
            
            addresses.forEach(addr => {
                const result = results[addr];
                const statusClass = result.success ? 'success' : 'failed';
                const statusText = result.success ? '✓ Success' : '✗ Failed';
                const valueText = result.success ? result.value : result.error;
                
                html += '<tr>';
                html += '<td><strong>' + addr + '</strong></td>';
                html += '<td class="' + statusClass + '">' + statusText + '</td>';
                html += '<td>' + valueText + '</td>';
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            
            if (successful > 0) {
                html += '<div style="margin-top: 15px; padding: 10px; background: #f0f0f0; border-radius: 4px;">';
                html += '<p><strong>Working addresses (copy to register_map.yaml):</strong></p>';
                html += '<code style="display: block; padding: 10px; background: white; border-radius: 4px; overflow-x: auto;">';
                const workingAddrs = addresses.filter(addr => results[addr].success);
                html += workingAddrs.join(', ');
                html += '</code></div>';
            }
            
            document.getElementById('results').innerHTML = html;
        }

        function showLoading(message) {
            let html = '<div class="loading">';
            html += '<div class="spinner"></div>';
            html += '<p>' + message + '</p>';
            html += '</div>';
            document.getElementById('statusArea').innerHTML = html;
        }

        function showError(message) {
            document.getElementById('statusArea').innerHTML = '<div class="error">' + message + '</div>';
        }

        function clearResults() {
            document.getElementById('results').innerHTML = '';
            document.getElementById('statusArea').innerHTML = '';
        }
    </script>
</body>
</html>"""

    def _get_ui_html(self) -> str:
        def esc(value: Any) -> str:
            return html.escape(str(value), quote=True)

        def fmt(value: Any, suffix: str = "") -> str:
            if value is None:
                return f"-{suffix}"
            if isinstance(value, float):
                return f"{value:.2f}{suffix}"
            return f"{value}{suffix}"

        def indicator(connected: bool, text: str) -> str:
            cls = "connected" if connected else "disconnected"
            return f'<span><span class="status-indicator {cls}"></span>{esc(text)}</span>'

        def stat_row(label: str, value: str) -> str:
            return (
                '<div class="stat-row">'
                f'<span class="stat-label">{esc(label)}:</span>'
                f'<span class="stat-value">{value}</span>'
                '</div>'
            )

        def card(title: str, rows: list[str], error_text: str | None = None, full: bool = False) -> str:
            full_class = " full" if full else ""
            error_html = f'<div class="error-msg">{esc(error_text)}</div>' if error_text else ""
            return (
                f'<div class="card{full_class}">'
                f'<h2>{esc(title)}</h2>'
                + "".join(rows)
                + error_html
                + '</div>'
            )

        def address_lines(entries: list[dict[str, Any]]) -> list[str]:
            lines: list[str] = []
            for entry in entries:
                address = int(entry.get("address", 0))
                label = str(entry.get("label", ""))
                value = entry.get("value")
                raw = "-" if value is None else str(value)
                lines.append(f"{address:<6} = {raw:<8} {label}")
            return lines

        status = self.state.get("status", {})
        huawei = self.state.get("huawei", {})
        pcs = self.state.get("pcs", {})
        control = self.state.get("control", {})
        server = self.state.get("server", {})
        grid = self.state.get("grid", {})
        probe = self.state.get("pcs_direct_probe", {})
        now = datetime.now().strftime("%H:%M:%S")

        debug_json = esc(json.dumps(self.state, indent=2, default=str))
        address_view = server.get("address_view", {}) if isinstance(server, dict) else {}
        address_text = "\n".join(
            ["INPUT REGISTERS"]
            + address_lines(address_view.get("input", []))
            + ["", "HOLDING REGISTERS"]
            + address_lines(address_view.get("holding", []))
            + [
                "",
                f"READ COUNTS: input={server.get('input_read_count', 0)} holding={server.get('holding_read_count', 0)} writes={server.get('write_count', 0)}",
                f"LAST HOLDING READ: {server.get('last_holding_read')}",
                f"LAST INPUT READ: {server.get('last_input_read')}",
                f"LAST WRITE: {server.get('last_write')}",
                f"DIRECT PROBE LAST OK: {probe.get('last_ok')}",
                f"DIRECT PROBE LAST HEARTBEAT: {probe.get('last_heartbeat')}",
                f"DIRECT PROBE LAST ERROR: {probe.get('last_error')}",
            ]
        )

        cards = [
            card(
                "Huawei Inverter",
                [
                    stat_row("Connection", indicator(bool(status.get("huawei_connected")), "Connected" if status.get("huawei_connected") else "Disconnected")),
                    stat_row("Status", esc(fmt(huawei.get("device_status_text") or huawei.get("device_status")))),
                    stat_row("Active Power", esc(fmt(huawei.get("active_power_kw"), " kW"))),
                    stat_row("PV Target", esc(fmt(huawei.get("pv_target_value")))),
                    stat_row("Derate %", esc(fmt(control.get("huawei_derate_percent"), "%"))),
                ],
                status.get("huawei_last_error"),
            ),
            card(
                "PCS Battery",
                [
                    stat_row("Connection", indicator(bool(status.get("pcs_connected")), "Connected" if status.get("pcs_connected") else "Disconnected")),
                    stat_row("SOC", esc(fmt(pcs.get("soc"), " %"))),
                    stat_row("Active Power", esc(fmt(pcs.get("load_active_power_kw"), " kW"))),
                    stat_row("Grid Power", esc(fmt(pcs.get("total_power_meter_kw"), " kW"))),
                ],
                status.get("pcs_last_error"),
            ),
            card(
                "Grid Status",
                [
                    stat_row("Available", indicator(bool(grid.get("is_available")), "Online" if grid.get("is_available") else "Offline")),
                    stat_row("Avg Voltage", esc(fmt(grid.get("avg_voltage_v"), " V"))),
                    stat_row("EMS Comm Fail", esc("Yes" if pcs.get("ems_comm_failure") else "No" if pcs.get("ems_comm_failure") is not None else "-")),
                    stat_row("Operation Mode", esc(fmt(control.get("operation_mode")))),
                ],
            ),
            card(
                "Control Settings",
                [
                    stat_row("Target Power", esc(fmt(control.get("target_power_kw"), " kW"))),
                    stat_row("Effective Power", esc(fmt(control.get("effective_target_power_kw"), " kW"))),
                    stat_row("Dynamic Charge Limit", esc(fmt(control.get("dynamic_charge_limit_kw"), " kW"))),
                    stat_row("Policy Reason", esc(fmt(control.get("policy_reason")))),
                ],
            ),
            card(
                "Battery Limits",
                [
                    stat_row("Charge V Limit", esc(fmt(pcs.get("bms_charge_voltage_limit_v"), " V"))),
                    stat_row("Charge I Limit", esc(fmt(pcs.get("bms_charge_current_limit_a"), " A"))),
                    stat_row("Auto Mode", esc("Enabled" if control.get("auto_mode_enabled") else "Disabled")),
                ],
            ),
            card(
                "EMS Modbus Server",
                [
                    stat_row("Enabled", esc("Yes" if server.get("enabled") else "No")),
                    stat_row("Status", indicator(bool(server.get("running")), "Running" if server.get("running") else "Stopped")),
                    stat_row("Port", esc(fmt(server.get("port")))),
                    stat_row("Unit ID", esc(fmt(server.get("unit_id")))),
                    stat_row("Clients", esc(fmt(server.get("connected_clients")))),
                ],
            ),
            card(
                "PCS Direct Probe",
                [
                    stat_row("Enabled", esc("Yes" if probe.get("enabled") else "No")),
                    stat_row("Last OK", esc(fmt(probe.get("last_ok")))),
                    stat_row("Last Heartbeat", esc(fmt(probe.get("last_heartbeat")))),
                    stat_row("Last Error", esc(fmt(probe.get("last_error")))),
                ],
            ),
            card("Raw Diagnostics JSON", [f'<div class="debug-panel">{debug_json}</div>'], full=True),
            card("EMS Address Viewer", [f'<div class="debug-panel">{esc(address_text)}</div>'], full=True),
        ]

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="3">
    <title>PIBEMS Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        h1 {{
            color: white;
            margin-bottom: 18px;
            text-align: center;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        .refresh-note {{
            text-align: center;
            color: rgba(255,255,255,0.9);
            margin-bottom: 20px;
        }}
        .content {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        .card.full {{
            grid-column: 1 / -1;
        }}
        .card h2 {{
            font-size: 1.3em;
            margin-bottom: 15px;
            color: #2a5298;
            border-bottom: 2px solid #2a5298;
            padding-bottom: 10px;
        }}
        .status-indicator {{
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
            vertical-align: middle;
        }}
        .status-indicator.connected {{
            background: #4CAF50;
            box-shadow: 0 0 10px rgba(76, 175, 80, 0.5);
        }}
        .status-indicator.disconnected {{
            background: #f44336;
            box-shadow: 0 0 10px rgba(244, 67, 54, 0.5);
        }}
        .stat-row {{
            display: flex;
            justify-content: space-between;
            gap: 16px;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }}
        .stat-row:last-child {{
            border-bottom: none;
        }}
        .stat-label {{
            font-weight: 600;
            color: #666;
        }}
        .stat-value {{
            color: #2a5298;
            font-weight: 500;
            font-family: 'Courier New', monospace;
            text-align: right;
        }}
        .error-msg {{
            color: #f44336;
            font-size: 0.9em;
            margin-top: 8px;
        }}
        .debug-panel {{
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 15px;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            max-height: 500px;
            overflow-y: auto;
            white-space: pre-wrap;
            word-break: break-word;
        }}
        .last-update {{
            text-align: center;
            font-size: 0.9em;
            color: rgba(255,255,255,0.9);
            margin-bottom: 20px;
        }}
        @media (max-width: 768px) {{
            .content {{
                grid-template-columns: 1fr;
            }}
            h1 {{
                font-size: 1.8em;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>PIBEMS Dashboard</h1>
        <div class="refresh-note">Server-rendered view. Auto-refresh every 3 seconds. | <a href="scanner" style="color: white; text-decoration: underline;">Modbus Scanner</a></div>
        <div class="last-update">Last update: {esc(now)}</div>
        <div class="content">
            {''.join(cards)}
        </div>
    </div>
</body>
</html>
"""

    def _apply_control_payload(self, payload: dict[str, Any]) -> tuple[bool, str | None]:
        if "target_power_kw" in payload and payload["target_power_kw"] is not None:
            self.state["control"]["target_power_kw"] = float(payload["target_power_kw"])
        if "huawei_derate_percent" in payload and payload["huawei_derate_percent"] is not None:
            value = max(0.0, min(100.0, float(payload["huawei_derate_percent"])))
            self.state["control"]["huawei_derate_percent"] = value
        if "auto_mode_enabled" in payload and payload["auto_mode_enabled"] is not None:
            self.state["control"]["auto_mode_enabled"] = bool(payload["auto_mode_enabled"])
        if "operation_mode" in payload and payload["operation_mode"] is not None:
            mode = str(payload["operation_mode"]).strip().lower()
            if mode not in ("control", "read_only"):
                return False, "operation_mode must be 'control' or 'read_only'"
            self.state["control"]["operation_mode"] = mode
        return True, None

    def _build_handler_class(self):
        service = self

        class APIHandler(BaseHTTPRequestHandler):
            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                _LOG.info("api | " + format, *args)

            def do_GET(self) -> None:  # noqa: N802
                path = self.path.split("?")[0].rstrip("/") or "/"
                if path == "/health":
                    self._send_json(200, service._health_payload())
                    return
                if path == "/api/diagnostics":
                    self._send_json(200, service.state)
                    return
                if path in ("/api/scanner", "/scanner"):
                    html = service._get_scanner_html()
                    body = html.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if path in ("/ui", "/"):
                    html = service._get_ui_html()
                    body = html.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self._send_json(404, {"ok": False, "error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                path = self.path.split("?")[0].rstrip("/") or "/"
                if path in ("/api/scan", "/scan"):
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        raw = self.rfile.read(length).decode("utf-8")
                        payload = json.loads(raw) if raw else {}
                    except Exception as exc:  # noqa: BLE001
                        self._send_json(400, {"ok": False, "error": f"invalid json: {exc}"})
                        return
                    
                    device = payload.get("device")
                    start_addr = payload.get("start_addr")
                    end_addr = payload.get("end_addr")
                    reg_type = payload.get("reg_type", "input")
                    
                    if not device or start_addr is None or end_addr is None:
                        self._send_json(400, {"ok": False, "error": "Missing device, start_addr, or end_addr"})
                        return
                    
                    try:
                        if service.loop is None:
                            self._send_json(500, {"ok": False, "error": "Event loop not initialized"})
                            return
                        result = asyncio.run_coroutine_threadsafe(
                            service.scan_registers(device, int(start_addr), int(end_addr), reg_type),
                            service.loop
                        ).result(timeout=60)
                        self._send_json(200, result)
                    except Exception as exc:  # noqa: BLE001
                        self._send_json(500, {"ok": False, "error": f"Scan failed: {exc}"})
                    return
                
                if path != "/api/control":
                    self._send_json(404, {"ok": False, "error": "not found"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    raw = self.rfile.read(length).decode("utf-8")
                    payload = json.loads(raw) if raw else {}
                except Exception as exc:  # noqa: BLE001
                    self._send_json(400, {"ok": False, "error": f"invalid json: {exc}"})
                    return

                ok, err = service._apply_control_payload(payload)
                if not ok:
                    self._send_json(400, {"ok": False, "error": err})
                    return
                self._send_json(200, {"ok": True, "control": service.state["control"]})

        return APIHandler

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        tasks = [
            asyncio.create_task(self._run_api(), name="api"),
            asyncio.create_task(self._poll_loop(), name="poll"),
            asyncio.create_task(self._control_loop(), name="control"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
        ]
        if self.opts.enable_ems_server:
            tasks.append(asyncio.create_task(self._run_modbus_server(), name="ems_server"))

        try:
            await self.stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.huawei_client.close()
            await self.pcs_client.close()

    async def _run_api(self) -> None:
        handler = self._build_handler_class()
        self.httpd = ThreadingHTTPServer((self.opts.api_bind_host, self.opts.api_bind_port), handler)
        self.http_thread = threading.Thread(target=self.httpd.serve_forever, name="pibems-http", daemon=True)
        self.http_thread.start()
        _LOG.info("HTTP API listening on %s:%s", self.opts.api_bind_host, self.opts.api_bind_port)

        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.5)
        finally:
            if self.httpd is not None:
                self.httpd.shutdown()
                self.httpd.server_close()
            if self.http_thread is not None:
                self.http_thread.join(timeout=3)

    async def _run_modbus_server(self) -> None:
        defaults = self.map.get("ems_server", {})
        input_defaults: dict[int, int] = {
            int(k): int(v) for k, v in defaults.get("input_defaults", {}).items()
        }
        holding_defaults: dict[int, int] = {
            int(k): int(v) for k, v in defaults.get("holding_defaults", {}).items()
        }

        # Large register spaces allow directly mirroring documented addresses.
        ir_block = ModbusSequentialDataBlock(0, [0] * 12000)
        hr_block = ModbusSequentialDataBlock(0, [0] * 12000)
        register_labels = self.server_register_labels
        # single=True makes the server respond to ANY Modbus unit_id the PCS sends.
        # This is correct EMS server behaviour - a real EMS accepts all slaves.
        # single=False with a keyed dict would silently reject unit_ids that don't match.
        store = _LoggingSlaveContext(
            ir=ir_block,
            hr=hr_block,
            register_labels=register_labels,
            on_external_read=self._on_pcs_ems_contact,
            on_external_write=self._on_pcs_ems_write,
        )
        self.server_ctx = ModbusServerContext(slaves={0: store}, single=True)
        self.server_store = store

        for addr, value in input_defaults.items():
            store.setValues(4, addr, [value], _internal=True)
        for addr, value in holding_defaults.items():
            store.setValues(3, addr, [value], _internal=True)
        self._refresh_server_address_view()

        _LOG.info(
            "Starting EMS Modbus server on %s:%s unit=%s",
            self.opts.ems_bind_host,
            self.opts.ems_bind_port,
            self.opts.ems_unit_id,
        )
        self.state["server"]["running"] = True
        try:
            await StartAsyncTcpServer(
                context=self.server_ctx,
                address=(self.opts.ems_bind_host, self.opts.ems_bind_port),
            )
        finally:
            self.state["server"]["running"] = False

    async def _poll_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if self.opts.enable_huawei:
                    await self._poll_huawei()
                if self.opts.enable_pcs_direct and self.server_ctx is None:
                    await self._poll_pcs()
                elif self.opts.enable_pcs_direct and self.server_ctx is not None:
                    await self._probe_pcs_heartbeat()
                self.state["errors"] = self.state["errors"][-20:]
            except Exception as exc:  # noqa: BLE001
                self.state["errors"].append(f"poll_loop: {exc}")
                _LOG.exception("poll loop error")
            await asyncio.sleep(self.opts.poll_interval_sec)

    async def _control_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if self.opts.enable_huawei:
                    await self._control_huawei()
                # Run PCS control in both direct-TCP mode and EMS-server mode.
                if self.opts.enable_pcs_direct or self.server_ctx is not None:
                    await self._control_pcs()
            except Exception as exc:  # noqa: BLE001
                self.state["errors"].append(f"control_loop: {exc}")
                _LOG.exception("control loop error")
            await asyncio.sleep(self.opts.control_interval_sec)

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            self.heartbeat_counter = (self.heartbeat_counter + 1) & 0xFFFF
            self.state["pcs"]["synthetic_heartbeat"] = self.heartbeat_counter

            if self.server_ctx is not None:
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2219, [self.heartbeat_counter], _internal=True)

                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2218, [0], _internal=True)
                # Load power registers kept as zero until a separate load meter is wired in.
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2326, [0], _internal=True)
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2327, [0], _internal=True)
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2328, [0], _internal=True)
                self._refresh_server_address_view()

            await asyncio.sleep(self.opts.heartbeat_interval_sec)

    async def _poll_huawei(self) -> None:
        try:
            points = self.map["huawei"]["status"]
            status = await self._read_u16(self.huawei_client, points["device_status"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)
            pwr = await self._read_i32(self.huawei_client, points["active_power"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)
            meter = await self._read_i32(self.huawei_client, points["meter_active_power"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)
            pv_target = await self._read_u16(self.huawei_client, points["pv_target_value"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)

            self.state["huawei"]["device_status"] = status
            self.state["huawei"]["device_status_text"] = f"{self._decode_huawei_status(status)} ({status})"
            self.state["huawei"]["active_power_kw"] = pwr / 1000.0
            self.state["huawei"]["meter_active_power_w"] = meter
            self.state["huawei"]["pv_target_value"] = pv_target
            self.state["status"]["huawei_connected"] = True
            self.state["status"]["huawei_last_error"] = None
        except Exception as exc:  # noqa: BLE001
            self.state["status"]["huawei_connected"] = False
            err_short = str(exc).split("\n")[0][:80]
            self.state["status"]["huawei_last_error"] = err_short
            self.state["status"]["huawei_last_error_time"] = _now_iso()
            _LOG.warning("Huawei not connected: %s", err_short)

    async def _poll_pcs(self) -> None:
        try:
            points = self.map["pcs"]["status"]
            alarm = await self._read_u16(self.pcs_client, points["alarm_word_1"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            status = await self._read_u16(self.pcs_client, points["pcs_status_word"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            monitor = await self._read_u16(self.pcs_client, points["monitor_alarm_word"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            total_power = await self._read_i16(self.pcs_client, points["total_power_meter"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            load_p = await self._read_i16(self.pcs_client, points["load_active_power"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            load_q = await self._read_i16(self.pcs_client, points["load_reactive_power"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            load_s = await self._read_i16(self.pcs_client, points["load_apparent_power"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            soc = await self._read_u16(self.pcs_client, points["soc"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)

            grid_v_ab = await self._read_i16(self.pcs_client, points["grid_voltage_ab"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            grid_v_bc = await self._read_i16(self.pcs_client, points["grid_voltage_bc"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)
            grid_v_ca = await self._read_i16(self.pcs_client, points["grid_voltage_ca"]["address"], self.opts.pcs_unit_id, self.opts.pcs_address_offset, input_reg=True)

            bms_charge_voltage_limit = await self._read_u16(
                self.pcs_client,
                self.map["pcs"]["limits"]["bms_charge_voltage_limit"]["address"],
                self.opts.pcs_unit_id,
                self.opts.pcs_address_offset,
            )
            bms_charge_current_limit = await self._read_u16(
                self.pcs_client,
                self.map["pcs"]["limits"]["bms_charge_current_limit"]["address"],
                self.opts.pcs_unit_id,
                self.opts.pcs_address_offset,
            )

            self.state["pcs"]["alarm_word_1"] = alarm
            self.state["pcs"]["pcs_status_word"] = status
            self.state["pcs"]["monitor_alarm_word"] = monitor
            self.state["pcs"]["total_power_meter_kw"] = total_power / 10.0
            self.state["pcs"]["load_active_power_kw"] = load_p / 10.0
            self.state["pcs"]["load_reactive_power_kvar"] = load_q / 10.0
            self.state["pcs"]["load_apparent_power_kva"] = load_s / 10.0
            self.state["pcs"]["soc"] = soc
            self.state["pcs"]["grid_voltage_ab_v"] = grid_v_ab / 10.0
            self.state["pcs"]["grid_voltage_bc_v"] = grid_v_bc / 10.0
            self.state["pcs"]["grid_voltage_ca_v"] = grid_v_ca / 10.0
            self.state["pcs"]["bms_charge_voltage_limit_v"] = bms_charge_voltage_limit
            self.state["pcs"]["bms_charge_current_limit_a"] = bms_charge_current_limit

            # Bit 2 in monitor alarm word is EMS communication failure in protocol docs.
            self.state["pcs"]["ems_comm_failure"] = bool(monitor & (1 << 2))

            avg_grid_v = (
                abs(self.state["pcs"]["grid_voltage_ab_v"])
                + abs(self.state["pcs"]["grid_voltage_bc_v"])
                + abs(self.state["pcs"]["grid_voltage_ca_v"])
            ) / 3.0
            grid_available = avg_grid_v >= self.opts.grid_voltage_present_threshold_v
            self.state["grid"]["is_available"] = grid_available
            self.state["grid"]["avg_voltage_v"] = avg_grid_v

            if self._last_grid_available is None:
                self._last_grid_available = grid_available
            elif self._last_grid_available != grid_available:
                self.state["grid"]["last_transition"] = "return" if grid_available else "fail"
                self._last_grid_available = grid_available

            self.state["status"]["pcs_connected"] = True
            self.state["status"]["pcs_last_error"] = None
        except Exception as exc:  # noqa: BLE001
            self.state["status"]["pcs_connected"] = False
            err_short = str(exc).split("\n")[0][:80]
            self.state["status"]["pcs_last_error"] = err_short
            self.state["status"]["pcs_last_error_time"] = _now_iso()
            _LOG.warning("PCS poll error [host=%s port=%s unit=%s]: %s",
                         self.opts.pcs_host, self.opts.pcs_port, self.opts.pcs_unit_id, err_short)

    async def scan_registers(
        self,
        device: str,
        start_addr: int,
        end_addr: int,
        reg_type: str = "input",
    ) -> dict[str, Any]:
        """Scan a range of Modbus registers and return success/failure status."""
        if device == "pcs":
            client = self.pcs_client
            unit_id = self.opts.pcs_unit_id
            offset = self.opts.pcs_address_offset
        elif device == "huawei":
            client = self.huawei_client
            unit_id = self.opts.huawei_unit_id
            offset = self.opts.huawei_address_offset
        else:
            return {"error": f"Unknown device: {device}"}

        results = {}
        input_reg = reg_type.lower() == "input"

        for addr in range(start_addr, end_addr + 1):
            try:
                await self._ensure_connected(client)
                wire_address = addr + offset
                
                if input_reg:
                    rr = await client.read_input_registers(address=wire_address, count=1, slave=unit_id)
                else:
                    rr = await client.read_holding_registers(address=wire_address, count=1, slave=unit_id)
                
                if rr.isError():
                    results[str(addr)] = {"success": False, "error": str(rr)}
                else:
                    value = int(rr.registers[0])
                    results[str(addr)] = {"success": True, "value": value}
            except Exception as exc:  # noqa: BLE001
                results[str(addr)] = {"success": False, "error": str(exc).split("\n")[0][:100]}
        
        return {
            "device": device,
            "reg_type": reg_type,
            "start_addr": start_addr,
            "end_addr": end_addr,
            "address_offset": offset,
            "results": results,
        }

    async def _probe_pcs_heartbeat(self) -> None:
        try:
            heartbeat_addr = int(self.map["pcs"]["status"]["heartbeat"]["address"])
            heartbeat = await self._read_u16(
                self.pcs_client,
                heartbeat_addr,
                self.opts.pcs_unit_id,
                self.opts.pcs_address_offset,
                input_reg=True,
            )
            self.state["pcs_direct_probe"]["last_ok"] = _now_iso()
            self.state["pcs_direct_probe"]["last_error"] = None
            self.state["pcs_direct_probe"]["last_heartbeat"] = heartbeat
            _LOG.info(
                "PCS direct heartbeat probe ok [host=%s port=%s unit=%s addr=%s value=%s]",
                self.opts.pcs_host,
                self.opts.pcs_port,
                self.opts.pcs_unit_id,
                heartbeat_addr,
                heartbeat,
            )
        except Exception as exc:  # noqa: BLE001
            err_short = str(exc).split("\n")[0][:80]
            self.state["pcs_direct_probe"]["last_error"] = err_short
            _LOG.warning(
                "PCS direct heartbeat probe failed [host=%s port=%s unit=%s]: %s",
                self.opts.pcs_host,
                self.opts.pcs_port,
                self.opts.pcs_unit_id,
                err_short,
            )

    async def _control_huawei(self) -> None:
        if self._is_read_only_mode():
            self.state["control"]["policy_reason"] = "read_only_mode"
            return
        point = self.map["huawei"]["control"]["active_power_percentage_derating"]
        derate = float(self.state["control"]["huawei_derate_percent"])
        if not self.state["grid"].get("is_available", True):
            soc = int(self.state["pcs"].get("soc", 50))
            if soc >= self.opts.outage_max_soc_from_huawei:
                derate = 0.0
        raw = int(max(0.0, min(100.0, derate)) * 10)
        await self._write_u16(
            self.huawei_client,
            point["address"],
            raw,
            self.opts.huawei_unit_id,
            self.opts.huawei_address_offset,
        )

    async def _control_pcs(self) -> None:
        if self._is_read_only_mode():
            self.state["control"]["policy_reason"] = "read_only_mode"
            return
        control = self.map["pcs"]["control"]
        target = self._resolve_target_power_kw()
        soc = int(self.state["pcs"].get("soc", 50))

        grid_available = bool(self.state["grid"].get("is_available", True))
        huawei_kw = float(self.state["huawei"].get("active_power_kw", 0.0))
        no_sun = huawei_kw < self.opts.solar_present_threshold_kw
        remote_cmd = self.opts.pcs_start_command
        if not grid_available and soc <= self.opts.outage_shutdown_soc_no_sun and no_sun:
            target = 0.0
            remote_cmd = self.opts.pcs_stop_command
            self.state["control"]["policy_reason"] = "outage_shutdown_low_soc_no_sun"

        raw_cmd = self._encode_i16(int(round(target * 10)))

        if self.server_ctx is not None:
            # EMS-server mode: the PCS reads its control commands from our holding registers.
            # The PCS disables its own TCP server once it establishes an EMS connection.
            ctx = self.server_ctx[self.opts.ems_unit_id]
            ctx.setValues(3, control["grid_connected_mode"]["address"],   [3],               _internal=True)
            ctx.setValues(3, control["power_control_type"]["address"],    [2],               _internal=True)
            ctx.setValues(3, control["remote_on_off"]["address"],         [remote_cmd],      _internal=True)
            ctx.setValues(3, control["control_mode"]["address"],          [1],               _internal=True)
            ctx.setValues(3, control["discharge_stop_soc"]["address"],    [self.opts.outage_reserve_soc],        _internal=True)
            ctx.setValues(3, control["charge_stop_soc"]["address"],       [self.opts.outage_max_soc_from_huawei], _internal=True)
            ctx.setValues(3, control["constant_power_command"]["address"],[raw_cmd],         _internal=True)
            self._refresh_server_address_view()
        else:
            # Direct TCP mode: connect to PCS Modbus TCP server.
            await self._write_u16(self.pcs_client, control["grid_connected_mode"]["address"], 3, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            await self._write_u16(self.pcs_client, control["power_control_type"]["address"], 2, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            await self._write_u16(self.pcs_client, control["remote_on_off"]["address"], self.opts.pcs_start_command, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            await self._write_u16(self.pcs_client, control["control_mode"]["address"], 1, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            await self._write_u16(self.pcs_client, control["discharge_stop_soc"]["address"], self.opts.outage_reserve_soc, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            await self._write_u16(self.pcs_client, control["charge_stop_soc"]["address"], self.opts.outage_max_soc_from_huawei, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            if not grid_available and soc <= self.opts.outage_shutdown_soc_no_sun and no_sun:
                await self._write_u16(self.pcs_client, control["remote_on_off"]["address"], self.opts.pcs_stop_command, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            elif grid_available and self.opts.auto_start_on_grid_return:
                await self._write_u16(self.pcs_client, control["remote_on_off"]["address"], self.opts.pcs_start_command, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
            await self._write_u16(self.pcs_client, control["constant_power_command"]["address"], raw_cmd, self.opts.pcs_unit_id, self.opts.pcs_address_offset)

        self.state["control"]["last_written_pcs_power_kw"] = target

    def _resolve_target_power_kw(self) -> float:
        manual_target = float(self.state["control"].get("target_power_kw", 0.0))
        auto_mode_enabled = bool(self.state["control"].get("auto_mode_enabled", True))
        soc = int(self.state["pcs"].get("soc", 50))
        grid_available = bool(self.state["grid"].get("is_available", True))

        target = manual_target
        reason = "manual"

        if auto_mode_enabled:
            if not grid_available:
                if soc <= self.opts.outage_reserve_soc:
                    target = 0.0
                    reason = "outage_hold_reserve_soc"
                elif soc < self.opts.outage_max_soc_from_huawei:
                    huawei_kw = float(self.state["huawei"].get("active_power_kw", 0.0))
                    if huawei_kw >= self.opts.solar_present_threshold_kw:
                        target = -min(self.opts.max_charge_kw, huawei_kw)
                        reason = "outage_solar_charge_to_max_soc"
                    else:
                        target = 0.0
                        reason = "outage_no_solar_hold"
            else:
                if self.opts.charge_from_grid_on_return and soc < self.opts.outage_reserve_soc:
                    target = -self.opts.max_charge_kw
                    reason = "grid_return_recover_reserve_soc"

        if target < 0:
            dynamic_limit = abs(self._dynamic_charge_limit_kw())
            target = max(target, -dynamic_limit)
        target = self._apply_common_limits(target, soc)

        self.state["control"]["effective_target_power_kw"] = target
        self.state["control"]["policy_reason"] = reason
        return target

    def _is_read_only_mode(self) -> bool:
        mode = str(self.state["control"].get("operation_mode", "control")).strip().lower()
        return mode != "control"

    def _dynamic_charge_limit_kw(self) -> float:
        if not self.opts.dynamic_charge_limit_enable:
            self.state["control"]["dynamic_charge_limit_kw"] = float(self.opts.max_charge_kw)
            return float(self.opts.max_charge_kw)

        voltage_v = float(self.state["pcs"].get("bms_charge_voltage_limit_v", 0.0))
        current_a = float(self.state["pcs"].get("bms_charge_current_limit_a", 0.0))
        if voltage_v <= 0 or current_a <= 0:
            limit = float(self.opts.max_charge_kw)
        else:
            limit = (voltage_v * current_a) / 1000.0
            limit *= max(0.1, min(1.0, self.opts.dynamic_charge_limit_margin))
            limit = min(limit, float(self.opts.max_charge_kw))
        self.state["control"]["dynamic_charge_limit_kw"] = limit
        return max(0.1, limit)

    def _apply_common_limits(self, target: float, soc: int) -> float:
        if target < 0 and soc <= self.opts.min_soc:
            return 0.0
        if target > 0 and soc >= self.opts.max_soc:
            return 0.0
        if target < 0:
            return max(target, -abs(self.opts.max_charge_kw))
        if target > 0:
            return min(target, abs(self.opts.max_discharge_kw))
        return 0.0

    async def _ensure_connected(self, client: AsyncModbusTcpClient) -> None:
        if getattr(client, "connected", False):
            return
        connected = await client.connect()
        if not connected:
            raise RuntimeError("Modbus client connect failed")

    async def _read_u16(
        self,
        client: AsyncModbusTcpClient,
        address: int,
        unit_id: int,
        offset: int,
        input_reg: bool = False,
    ) -> int:
        await self._ensure_connected(client)
        wire_address = address + offset
        if input_reg:
            rr = await client.read_input_registers(address=wire_address, count=1, slave=unit_id)
        else:
            rr = await client.read_holding_registers(address=wire_address, count=1, slave=unit_id)
        if rr.isError():
            raise RuntimeError(f"Modbus read error at {wire_address}: {rr}")
        return int(rr.registers[0])

    async def _read_i16(
        self,
        client: AsyncModbusTcpClient,
        address: int,
        unit_id: int,
        offset: int,
        input_reg: bool = False,
    ) -> int:
        raw = await self._read_u16(client, address, unit_id, offset, input_reg=input_reg)
        return self._decode_i16(raw)

    async def _read_i32(
        self,
        client: AsyncModbusTcpClient,
        address: int,
        unit_id: int,
        offset: int,
    ) -> int:
        await self._ensure_connected(client)
        wire_address = address + offset
        rr = await client.read_holding_registers(address=wire_address, count=2, slave=unit_id)
        if rr.isError():
            raise RuntimeError(f"Modbus read error at {wire_address}: {rr}")
        hi, lo = rr.registers[0], rr.registers[1]
        raw = (hi << 16) | lo
        if raw & 0x80000000:
            raw -= 0x100000000
        return raw

    async def _write_u16(
        self,
        client: AsyncModbusTcpClient,
        address: int,
        value: int,
        unit_id: int,
        offset: int,
    ) -> None:
        await self._ensure_connected(client)
        wire_address = address + offset
        wr = await client.write_register(address=wire_address, value=int(value) & 0xFFFF, slave=unit_id)
        if wr.isError():
            raise RuntimeError(f"Modbus write error at {wire_address}: {wr}")

    @staticmethod
    def _decode_i16(value: int) -> int:
        return value - 0x10000 if value & 0x8000 else value

    @staticmethod
    def _encode_i16(value: float | int) -> int:
        i = int(value)
        if i < 0:
            i += 0x10000
        return i & 0xFFFF


def load_options(path: Path) -> Options:
    opts = Options()
    if not path.exists():
        _LOG.warning("options file %s not found, using defaults", path)
        return opts

    data = json.loads(path.read_text(encoding="utf-8"))
    for key, value in data.items():
        if hasattr(opts, key):
            setattr(opts, key, value)
    return opts


def load_register_map(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"register map file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


async def _main_async() -> None:
    opts = load_options(Path("/data/options.json"))
    reg_map = load_register_map(Path(opts.register_map_file))

    svc = EMSService(opts, reg_map)
    loop = asyncio.get_running_loop()

    def _shutdown() -> None:
        _LOG.info("Shutdown requested")
        svc.stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass

    await svc.run()


def main() -> None:
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
