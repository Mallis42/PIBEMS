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
    enable_pcs_control: bool = True
    enable_ems_server: bool = True

    poll_interval_sec: float = 1.0
    control_interval_sec: float = 1.0
    heartbeat_interval_sec: float = 1.0

    target_power_kw: float = 0.0
    huawei_derate_percent: float = 100.0
    huawei_control_mode: str = "derate_percent"
    huawei_target_kw: float = 0.0
    huawei_follow_pcs_load: bool = False
    pv_charge_enable: bool = False
    pv_charge_extra_kw: float = 0.0
    outage_prevent_overcharge_enable: bool = False
    outage_prevent_draw_kw: float = 2.0
    outage_prevent_soc_low: int = 95
    outage_prevent_soc_high: int = 98
    huawei_target_ramp_enable: bool = True
    huawei_target_ramp_kw_per_cycle: float = 0.5
    huawei_target_ramp_up_kw_per_cycle: float = 0.3
    huawei_target_ramp_down_kw_per_cycle: float = 1.0

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
                "huawei_control_mode": str(opts.huawei_control_mode),
                "huawei_target_kw": float(opts.huawei_target_kw),
                "huawei_follow_pcs_load": bool(opts.huawei_follow_pcs_load),
                "pv_charge_enable": bool(opts.pv_charge_enable),
                "pv_charge_extra_kw": float(opts.pv_charge_extra_kw),
                "outage_prevent_overcharge_enable": bool(opts.outage_prevent_overcharge_enable),
                "outage_prevent_draw_kw": float(opts.outage_prevent_draw_kw),
                "outage_prevent_soc_low": int(opts.outage_prevent_soc_low),
                "outage_prevent_soc_high": int(opts.outage_prevent_soc_high),
                "outage_prevent_draw_active": False,
                "huawei_target_ramp_enable": bool(opts.huawei_target_ramp_enable),
                "huawei_target_ramp_kw_per_cycle": float(opts.huawei_target_ramp_kw_per_cycle),
                "huawei_target_ramp_up_kw_per_cycle": float(opts.huawei_target_ramp_up_kw_per_cycle),
                "huawei_target_ramp_down_kw_per_cycle": float(opts.huawei_target_ramp_down_kw_per_cycle),
                "huawei_target_last_kw_written": None,
                "huawei_trace": {},
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
            "pcs_control_enabled": self.opts.enable_pcs_control,
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
        <a href="./" class="back-link">← Back to Dashboard</a>
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
            
            // Use relative path that works with both local and HA Cloud ingress
            const scanPath = './scan';
            
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
            .then(res => {
                if (!res.ok) {
                    return res.text().then(text => {
                        throw new Error('HTTP ' + res.status + ': ' + text);
                    });
                }
                return res.json();
            })
            .then(data => {
                if (data.error) {
                    showError(data.error);
                } else {
                    displayResults(data);
                }
            })
            .catch(err => {
                console.error('Scan error:', err);
                showError('Scan failed: ' + err.toString());
            })
            .finally(() => {
                scanInProgress = false;
            });
        }

        function displayResults(data) {
            const results = data.results || {};
            const addresses = Object.keys(results).map(Number).sort((a, b) => a - b);
            const successfulAddresses = addresses.filter(addr => results[addr] && results[addr].success);
            
            const successful = successfulAddresses.length;
            const failed = addresses.length - successful;
            
            let html = '';
            html += '<div class="stats">';
            html += '<div class="stat-box"><div class="stat-label">Total</div><div class="stat-value">' + addresses.length + '</div></div>';
            html += '<div class="stat-box"><div class="stat-label">✓ Success</div><div class="stat-value" style="color: #28a745;">' + successful + '</div></div>';
            html += '<div class="stat-box"><div class="stat-label">✗ Failed</div><div class="stat-value" style="color: #dc3545;">' + failed + '</div></div>';
            html += '</div>';
            
            html += '<div class="success-msg">Scan completed for ' + data.device.toUpperCase() + ' (' + data.reg_type + ' registers) | Offset: ' + data.address_offset + '</div>';

            if (successful > 0) {
                html += '<table class="results-table">';
                html += '<thead><tr><th>Address</th><th>U16</th><th>S16</th></tr></thead>';
                html += '<tbody>';

                successfulAddresses.forEach(addr => {
                    const result = results[addr];
                    html += '<tr>';
                    html += '<td><strong>' + addr + '</strong></td>';
                    html += '<td>' + result.u16 + '</td>';
                    html += '<td>' + result.s16 + '</td>';
                    html += '</tr>';
                });

                html += '</tbody></table>';
            } else {
                html += '<div class="error">No readable registers found in this range.</div>';
            }
            
            if (successful > 0) {
                html += '<div style="margin-top: 15px; padding: 10px; background: #f0f0f0; border-radius: 4px;">';
                html += '<p><strong>Working addresses (copy to register_map.yaml):</strong></p>';
                html += '<code style="display: block; padding: 10px; background: white; border-radius: 4px; overflow-x: auto;">';
                const workingAddrs = successfulAddresses;
                html += workingAddrs.join(', ');
                html += '</code></div>';

                html += buildCandidateMapHtml(data, addresses, results);
            }
            
            document.getElementById('results').innerHTML = html;
        }

        function buildCandidateMapHtml(data, addresses, results) {
            const keyPoints = [
                {name: 'alarm_word_1', doc: 2100},
                {name: 'pcs_status_word', doc: 2105},
                {name: 'monitor_alarm_word', doc: 2107},
                {name: 'total_power_meter', doc: 2218},
                {name: 'igbt_temperature_c', doc: 2270},
                {name: 'load_active_power', doc: 2326},
                {name: 'load_reactive_power', doc: 2327},
                {name: 'load_apparent_power', doc: 2328},
            ];

            const successAddrs = addresses.filter(addr => results[addr] && results[addr].success);
            if (successAddrs.length === 0) {
                return '';
            }
            const successSet = new Set(successAddrs);

            const candidateScores = {};
            successAddrs.forEach(addr => {
                const off = addr - 2270; // use the 2270 point as a reference anchor
                let score = 0;
                keyPoints.forEach(p => {
                    if (successSet.has(p.doc + off)) {
                        score += 1;
                    }
                });
                if (score >= 3) {
                    candidateScores[off] = Math.max(candidateScores[off] || 0, score);
                }
            });

            if (!(data.address_offset in candidateScores)) {
                let currentScore = 0;
                keyPoints.forEach(p => {
                    if (successSet.has(p.doc + data.address_offset)) {
                        currentScore += 1;
                    }
                });
                candidateScores[data.address_offset] = currentScore;
            }

            const topOffsets = Object.keys(candidateScores)
                .map(Number)
                .sort((a, b) => (candidateScores[b] - candidateScores[a]) || (Math.abs(a - data.address_offset) - Math.abs(b - data.address_offset)))
                .slice(0, 3);

            let html = '';
            html += '<div style="margin-top: 15px; padding: 10px; background: #eef5ff; border-radius: 4px;">';
            html += '<p><strong>Candidate map hints (auto-detected):</strong></p>';
            html += '<p style="font-size: 12px; color: #555; margin-bottom: 8px;">Shows likely offsets based on key PCS points and decoded U16/S16 values.</p>';

            topOffsets.forEach(off => {
                html += '<div style="margin-top: 10px; padding: 8px; background: white; border: 1px solid #d9e7ff; border-radius: 4px;">';
                html += '<div style="font-weight: 600; margin-bottom: 6px;">Offset ' + off + ' (score ' + candidateScores[off] + '/8)' + (off === data.address_offset ? ' • current' : '') + '</div>';
                html += '<table class="results-table">';
                html += '<thead><tr><th>Point</th><th>Doc Addr</th><th>Wire Addr</th><th>U16</th><th>S16</th></tr></thead><tbody>';

                keyPoints.forEach(p => {
                    const wire = p.doc + off;
                    const r = results[wire];
                    const ok = !!(r && r.success);
                    if (!ok) {
                        return;
                    }
                    html += '<tr>';
                    html += '<td>' + p.name + '</td>';
                    html += '<td>' + p.doc + '</td>';
                    html += '<td>' + wire + '</td>';
                    html += '<td>' + r.u16 + '</td>';
                    html += '<td>' + r.s16 + '</td>';
                    html += '</tr>';
                });

                html += '</tbody></table>';
                html += '</div>';
            });

            html += '</div>';
            return html;
        }

        // Known PCS register map: doc_address -> {name, signed, scale, unit}
        const KNOWN_REGISTERS = {
            2100: {name:'alarm_word_1',           signed:false, scale:1,    unit:''},
            2105: {name:'pcs_status_word',         signed:false, scale:1,    unit:''},
            2107: {name:'monitor_alarm_word',      signed:false, scale:1,    unit:''},
            2207: {name:'grid_voltage_ab',         signed:false, scale:0.1,  unit:'V'},
            2208: {name:'grid_voltage_bc',         signed:false, scale:0.1,  unit:'V'},
            2209: {name:'grid_voltage_ca',         signed:false, scale:0.1,  unit:'V'},
            2210: {name:'inverter_current_a',      signed:true,  scale:0.1,  unit:'A'},
            2211: {name:'inverter_current_b',      signed:true,  scale:0.1,  unit:'A'},
            2212: {name:'inverter_current_c',      signed:true,  scale:0.1,  unit:'A'},
            2213: {name:'inverter_frequency',      signed:false, scale:0.01, unit:'Hz'},
            2214: {name:'pcs_power_factor',        signed:true,  scale:0.01, unit:''},
            2215: {name:'load_active_power',       signed:true,  scale:0.1,  unit:'kW'},
            2216: {name:'load_reactive_power',     signed:true,  scale:0.1,  unit:'kvar'},
            2217: {name:'load_apparent_power',     signed:true,  scale:0.1,  unit:'kVA'},
            2218: {name:'total_power_meter',       signed:true,  scale:0.1,  unit:'kW'},
            2267: {name:'dc_side_voltage',         signed:true,  scale:0.1,  unit:'V'},
            2268: {name:'dc_side_current',         signed:true,  scale:0.1,  unit:'A'},
            2269: {name:'dc_side_power',           signed:true,  scale:0.1,  unit:'kW'},
            2270: {name:'igbt_temperature',        signed:true,  scale:0.1,  unit:'°C'},
            2271: {name:'ambient_temperature',     signed:true,  scale:0.1,  unit:'°C'},
            2237: {name:'grid_side_voltage_ab',    signed:true,  scale:0.1,  unit:'V'},
            2238: {name:'grid_side_voltage_bc',    signed:true,  scale:0.1,  unit:'V'},
            2239: {name:'grid_side_voltage_ca',    signed:true,  scale:0.1,  unit:'V'},
            2240: {name:'grid_side_current_a',     signed:true,  scale:0.1,  unit:'A'},
            2241: {name:'grid_side_current_b',     signed:true,  scale:0.1,  unit:'A'},
            2242: {name:'grid_side_current_c',     signed:true,  scale:0.1,  unit:'A'},
            2243: {name:'grid_side_frequency',     signed:false, scale:0.01, unit:'Hz'},
            2244: {name:'grid_side_power_factor',  signed:true,  scale:0.01, unit:''},
            2245: {name:'grid_side_active_power',  signed:true,  scale:0.1,  unit:'kW'},
            2246: {name:'grid_side_reactive_power',signed:true,  scale:0.1,  unit:'kvar'},
            2247: {name:'grid_side_apparent_power',signed:true,  scale:0.1,  unit:'kVA'},
            2337: {name:'dod_on_grid',             signed:false, scale:1,    unit:'%'},
            2338: {name:'dod_off_grid',            signed:false, scale:1,    unit:'%'},
            2339: {name:'charge_voltage_upper_limit', signed:false, scale:1, unit:'V'},
            2340: {name:'discharge_voltage_lower_limit', signed:false, scale:1, unit:'V'},
            2341: {name:'charge_current_limit_setting', signed:false, scale:1, unit:'A'},
            2344: {name:'force_charge_start_cell_voltage', signed:false, scale:1, unit:'mV'},
            2345: {name:'force_charge_stop_cell_voltage', signed:false, scale:1, unit:'mV'},
            2347: {name:'battery_voltage',         signed:false, scale:0.1,  unit:'V'},
            2348: {name:'battery_current',         signed:true,  scale:0.1,  unit:'A'},
            2349: {name:'soc',                     signed:false, scale:1,    unit:'%'},
            2350: {name:'soh',                     signed:false, scale:1,    unit:'%'},
            2351: {name:'max_cell_voltage',        signed:false, scale:1,    unit:'mV'},
            2352: {name:'min_cell_voltage',        signed:false, scale:1,    unit:'mV'},
            2353: {name:'max_cell_temp',           signed:false, scale:0.1,  unit:'°C'},
            2354: {name:'min_cell_temp',           signed:false, scale:0.1,  unit:'°C'},
            2355: {name:'charge_current_limit',    signed:false, scale:0.1,  unit:'A'},
            2356: {name:'discharge_current_limit', signed:false, scale:0.1,  unit:'A'},
            2357: {name:'allow_charge_power',      signed:false, scale:1,    unit:'kW'},
            2358: {name:'allow_discharge_power',   signed:false, scale:1,    unit:'kW'},
            2359: {name:'battery_status',          signed:false, scale:1,    unit:'enum'},
            2627: {name:'rtc_hour',                signed:false, scale:1,    unit:'h'},
            2628: {name:'rtc_minute',              signed:false, scale:1,    unit:'min'},
            2629: {name:'rtc_second',              signed:false, scale:1,    unit:'s'},
            2630: {name:'rtc_year',                signed:false, scale:1,    unit:''},
            2631: {name:'rtc_month',               signed:false, scale:1,    unit:''},
            2632: {name:'rtc_day',                 signed:false, scale:1,    unit:''},
        };

        function resolveKnown(requestedAddr, wireAddr, offset) {
            const candidates = [
                requestedAddr,
                wireAddr,
                requestedAddr - offset,
                wireAddr - offset,
            ].filter(v => Number.isFinite(v));

            const seen = new Set();
            for (const docAddr of candidates) {
                const normalized = Number(docAddr);
                if (seen.has(normalized)) {
                    continue;
                }
                seen.add(normalized);
                const reg = KNOWN_REGISTERS[normalized];
                if (reg) {
                    return Object.assign({docAddr: normalized}, reg);
                }
            }
            return null;
        }

        function displayResults(data) {
            const results = data.results || {};
            const offset = data.address_offset || 0;
            const addresses = Object.keys(results).map(Number).sort((a, b) => a - b);
            const successfulAddresses = addresses.filter(addr => results[addr] && results[addr].success);

            const successful = successfulAddresses.length;
            const failed = addresses.length - successful;

            let html = '';
            html += '<div class="stats">';
            html += '<div class="stat-box"><div class="stat-label">Total</div><div class="stat-value">' + addresses.length + '</div></div>';
            html += '<div class="stat-box"><div class="stat-label">✓ Success</div><div class="stat-value" style="color: #28a745;">' + successful + '</div></div>';
            html += '<div class="stat-box"><div class="stat-label">✗ Failed</div><div class="stat-value" style="color: #dc3545;">' + failed + '</div></div>';
            html += '</div>';

            html += '<div class="success-msg">Scan completed for ' + data.device.toUpperCase() + ' (' + data.reg_type + ' registers) | Offset: ' + offset + '</div>';

            if (successful > 0) {
                html += '<table class="results-table">';
                html += '<thead><tr><th>Requested Addr</th><th>Wire Addr</th><th>Doc Addr</th><th>U16</th><th>S16</th><th>Known Register</th><th>Scaled Value</th></tr></thead>';
                html += '<tbody>';

                successfulAddresses.forEach(addr => {
                    const result = results[addr];
                    const requestedAddr = Number(result.requested_address ?? addr);
                    const wireAddr = Number(result.wire_address ?? requestedAddr);
                    const u16 = result.u16;
                    const s16 = result.s16;
                    const known = resolveKnown(requestedAddr, wireAddr, offset);
                    const docAddr = known ? known.docAddr : requestedAddr;

                    let knownCell = '';
                    let scaledCell = '';
                    if (known) {
                        knownCell = '<span style="background:#d4edda;color:#155724;padding:2px 8px;border-radius:10px;font-size:0.85em;font-weight:600;">' + known.name + '</span>';
                        const rawVal = known.signed ? s16 : u16;
                        const scaled = (known.scale === 1) ? rawVal : Math.round(rawVal * known.scale * 1000) / 1000;
                        const unitStr = known.unit ? ' ' + known.unit : '';
                        scaledCell = '<strong style="color:#0056b3;">' + scaled + unitStr + '</strong>';
                    }

                    const rowBg = known ? ' style="background:#f0faf2;"' : '';
                    html += '<tr' + rowBg + '>';
                    html += '<td><strong>' + requestedAddr + '</strong></td>';
                    html += '<td style="color:#666;">' + wireAddr + '</td>';
                    html += '<td style="color:#666;">' + docAddr + '</td>';
                    html += '<td>' + u16 + '</td>';
                    html += '<td>' + s16 + '</td>';
                    html += '<td>' + knownCell + '</td>';
                    html += '<td>' + scaledCell + '</td>';
                    html += '</tr>';
                });

                html += '</tbody></table>';
            } else {
                html += '<div class="error">No readable registers found in this range.</div>';
            }

            if (successful > 0) {
                html += '<div style="margin-top:15px;padding:10px;background:#f0f0f0;border-radius:4px;">';
                html += '<p><strong>Working wire addresses:</strong></p>';
                html += '<code style="display:block;padding:10px;background:white;border-radius:4px;overflow-x:auto;">';
                html += successfulAddresses.join(', ');
                html += '</code></div>';

                html += buildCandidateMapHtml(data, addresses, results);
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

        def indicator(ok: bool, text: str) -> str:
            color = "#28a745" if ok else "#dc3545"
            return (
                '<span style="display:inline-flex;align-items:center;gap:8px;">'
                f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{color};"></span>'
                f'{esc(text)}'
                '</span>'
            )

        def stat_row(label: str, value: str) -> str:
            return (
                '<div class="stat-row">'
                f'<span class="stat-label">{esc(label)}:</span>'
                f'<span class="stat-value">{value}</span>'
                '</div>'
            )

        def card(title: str, rows: list[str], error: Any = None, full: bool = False) -> str:
            classes = 'card full-width' if full else 'card'
            error_html = f'<div class="error-msg">{esc(error)}</div>' if error else ''
            return (
                f'<div class="{classes}">'
                f'<h2>{esc(title)}</h2>'
                + ''.join(rows)
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
        huawei_trace = control.get("huawei_trace", {}) if isinstance(control, dict) else {}
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
                    stat_row("Control Mode", esc(fmt(control.get("huawei_control_mode")))),
                    stat_row("Target kW", esc(fmt(control.get("huawei_target_kw"), " kW"))),
                    stat_row("Follow PCS Load", esc("Yes" if control.get("huawei_follow_pcs_load") else "No")),
                    stat_row("PV Charge Enable", esc("Yes" if control.get("pv_charge_enable") else "No")),
                    stat_row("PV Charge Extra", esc(fmt(control.get("pv_charge_extra_kw"), " kW"))),
                    stat_row("Outage Prevent", esc("Yes" if control.get("outage_prevent_overcharge_enable") else "No")),
                    stat_row("Prevent Draw", esc(fmt(control.get("outage_prevent_draw_kw"), " kW"))),
                    stat_row("SOC Window", esc(f"{fmt(control.get('outage_prevent_soc_low'))}% - {fmt(control.get('outage_prevent_soc_high'))}%")),
                    stat_row("Prevent Active", esc("Yes" if control.get("outage_prevent_draw_active") else "No")),
                    stat_row("Target Ramp", esc("Yes" if control.get("huawei_target_ramp_enable") else "No")),
                    stat_row("Ramp Up Step", esc(fmt(control.get("huawei_target_ramp_up_kw_per_cycle"), " kW/cycle"))),
                    stat_row("Ramp Down Step", esc(fmt(control.get("huawei_target_ramp_down_kw_per_cycle"), " kW/cycle"))),
                    stat_row("Last Target Written", esc(fmt(control.get("huawei_target_last_kw_written"), " kW"))),
                    stat_row("Derate %", esc(fmt(control.get("huawei_derate_percent"), "%"))),
                    stat_row("PV1", esc(f"{fmt(huawei.get('pv1_voltage_v'), ' V')} / {fmt(huawei.get('pv1_current_a'), ' A')}")),
                    stat_row("PV2", esc(f"{fmt(huawei.get('pv2_voltage_v'), ' V')} / {fmt(huawei.get('pv2_current_a'), ' A')}")),
                    stat_row("PV3", esc(f"{fmt(huawei.get('pv3_voltage_v'), ' V')} / {fmt(huawei.get('pv3_current_a'), ' A')}")),
                    stat_row("PV4", esc(f"{fmt(huawei.get('pv4_voltage_v'), ' V')} / {fmt(huawei.get('pv4_current_a'), ' A')}")),
                ],
                status.get("huawei_last_error"),
            ),
            card(
                "Huawei Control Trace",
                [
                    stat_row("Trace Time", esc(fmt(huawei_trace.get("time")))),
                    stat_row("Mode", esc(fmt(huawei_trace.get("mode")))),
                    stat_row("Grid Available", esc(fmt(huawei_trace.get("grid_available")))),
                    stat_row("Grid Active Raw", esc(fmt(huawei_trace.get("grid_side_active_power_kw_raw"), " kW"))),
                    stat_row("Grid Import Calc", esc(fmt(huawei_trace.get("grid_import_kw"), " kW"))),
                    stat_row("PCS Load Raw", esc(fmt(huawei_trace.get("pcs_load_kw_raw"), " kW"))),
                    stat_row("Source Selected", esc(fmt(huawei_trace.get("source_name")))),
                    stat_row("Base Load", esc(fmt(huawei_trace.get("base_load_kw"), " kW"))),
                    stat_row("Charge Extra Req", esc(fmt(huawei_trace.get("pv_charge_extra_requested_kw"), " kW"))),
                    stat_row("Allow Charge", esc(fmt(huawei_trace.get("allow_charge_kw"), " kW"))),
                    stat_row("Charge Extra Applied", esc(fmt(huawei_trace.get("pv_charge_extra_applied_kw"), " kW"))),
                    stat_row("Target Pre-Outage", esc(fmt(huawei_trace.get("target_pre_outage_kw"), " kW"))),
                    stat_row("Outage Prevent Active", esc(fmt(huawei_trace.get("outage_prevent_draw_active")))),
                    stat_row("Outage Draw Applied", esc(fmt(huawei_trace.get("outage_draw_applied_kw"), " kW"))),
                    stat_row("Target Pre-Ramp", esc(fmt(huawei_trace.get("target_pre_ramp_kw"), " kW"))),
                    stat_row("Prev Target", esc(fmt(huawei_trace.get("prev_target_kw"), " kW"))),
                    stat_row("Delta", esc(fmt(huawei_trace.get("delta_kw"), " kW"))),
                    stat_row("Ramp Up Step", esc(fmt(huawei_trace.get("ramp_up_step_kw"), " kW/cycle"))),
                    stat_row("Ramp Down Step", esc(fmt(huawei_trace.get("ramp_down_step_kw"), " kW/cycle"))),
                    stat_row("Ramp Action", esc(fmt(huawei_trace.get("ramp_action")))),
                    stat_row("Target Final", esc(fmt(huawei_trace.get("target_final_kw"), " kW"))),
                    stat_row("Write Raw", esc(fmt(huawei_trace.get("write_raw")))),
                    stat_row("Write Scale", esc(fmt(huawei_trace.get("write_scale")))),
                    stat_row("Policy Reason", esc(fmt(huawei_trace.get("policy_reason")))),
                ],
            ),
            card(
                "PCS Battery",
                [
                    stat_row("Connection", indicator(bool(status.get("pcs_connected")), "Connected" if status.get("pcs_connected") else "Disconnected")),
                    stat_row("SOC", esc(fmt(pcs.get("soc"), " %"))),
                    stat_row("SOH", esc(fmt(pcs.get("soh"), " %"))),
                    stat_row("Active Power", esc(fmt(pcs.get("load_active_power_kw"), " kW"))),
                    stat_row("Grid Power", esc(fmt(pcs.get("total_power_meter_kw"), " kW"))),
                ],
                status.get("pcs_last_error"),
            ),
            card(
                "PCS Battery Block 3097-3109",
                [
                    stat_row("Battery Voltage", esc(fmt(pcs.get("battery_voltage_v"), " V"))),
                    stat_row("Battery Current", esc(fmt(pcs.get("battery_current_a"), " A"))),
                    stat_row("SOC", esc(fmt(pcs.get("soc"), " %"))),
                    stat_row("SOH", esc(fmt(pcs.get("soh"), " %"))),
                    stat_row("Maximum Cell Voltage", esc(fmt(pcs.get("max_cell_voltage_mv"), " mV"))),
                    stat_row("Minimum Cell Voltage", esc(fmt(pcs.get("min_cell_voltage_mv"), " mV"))),
                    stat_row("Maximum Cell Temperature", esc(fmt(pcs.get("max_cell_temp_c"), " °C"))),
                    stat_row("Minimum Cell Temperature", esc(fmt(pcs.get("min_cell_temp_c"), " °C"))),
                    stat_row("Charge Current Limit", esc(fmt(pcs.get("charge_current_limit_a"), " A"))),
                    stat_row("Discharge Current Limit", esc(fmt(pcs.get("discharge_current_limit_a"), " A"))),
                    stat_row("Allow Charge Power", esc(fmt(pcs.get("allow_charge_power_kw"), " kW"))),
                    stat_row("Allow Discharge Power", esc(fmt(pcs.get("allow_discharge_power_kw"), " kW"))),
                    stat_row("Battery Status", (lambda v: (
                        '<span style="display:inline-block;padding:2px 10px;border-radius:12px;font-weight:600;font-size:0.92em;'
                        + {0:'background:#d4edda;color:#155724', 1:'background:#fff3cd;color:#856404', 2:'background:#fff3cd;color:#856404', 3:'background:#f8d7da;color:#721c24', 4:'background:#f5c6cb;color:#491217'}.get(int(v), 'background:#e2e3e5;color:#383d41')
                        + '">'
                        + {0:'✅ Normal', 1:'⚠️ Charge Disable', 2:'⚠️ Discharge Disable', 3:'🔴 Alarm', 4:'❌ Failure'}.get(int(v), f'Unknown ({v})')
                        + '</span>'
                    ) if v is not None else '—')(pcs.get("battery_status"))),
                ],
            ),
            card(
                "PCS Inverter Telemetry",
                [
                    stat_row("Voltage AB", esc(fmt(pcs.get("grid_voltage_ab_v"), " V"))),
                    stat_row("Voltage BC", esc(fmt(pcs.get("grid_voltage_bc_v"), " V"))),
                    stat_row("Voltage CA", esc(fmt(pcs.get("grid_voltage_ca_v"), " V"))),
                    stat_row("Current A", esc(fmt(pcs.get("inverter_current_a_a"), " A"))),
                    stat_row("Current B", esc(fmt(pcs.get("inverter_current_b_a"), " A"))),
                    stat_row("Current C", esc(fmt(pcs.get("inverter_current_c_a"), " A"))),
                    stat_row("Frequency", esc(fmt(pcs.get("inverter_frequency_hz"), " Hz"))),
                    stat_row("Power Factor", esc(fmt(pcs.get("pcs_power_factor")))),
                    stat_row("DC Side Voltage", esc(fmt(pcs.get("dc_side_voltage_v"), " V"))),
                    stat_row("DC Side Current", esc(fmt(pcs.get("dc_side_current_a"), " A"))),
                    stat_row("DC Side Power", esc(fmt(pcs.get("dc_side_power_kw"), " kW"))),
                    stat_row("IGBT Temp", esc(fmt(pcs.get("igbt_temperature_c"), " °C"))),
                    stat_row("Ambient Temp", esc(fmt(pcs.get("ambient_temperature_c"), " °C"))),
                ],
            ),
            card(
                "PCS Grid-Side Telemetry",
                [
                    '<div style="font-size:0.75em;font-weight:700;color:#6c757d;text-transform:uppercase;letter-spacing:0.05em;padding:4px 0 2px">Voltage</div>',
                    stat_row("V AB", esc(fmt(pcs.get("grid_side_voltage_ab_v"), " V"))),
                    stat_row("V BC", esc(fmt(pcs.get("grid_side_voltage_bc_v"), " V"))),
                    stat_row("V CA", esc(fmt(pcs.get("grid_side_voltage_ca_v"), " V"))),
                    '<div style="font-size:0.75em;font-weight:700;color:#6c757d;text-transform:uppercase;letter-spacing:0.05em;padding:6px 0 2px">Current</div>',
                    stat_row("I A", esc(fmt(pcs.get("grid_side_current_a_a"), " A"))),
                    stat_row("I B", esc(fmt(pcs.get("grid_side_current_b_a"), " A"))),
                    stat_row("I C", esc(fmt(pcs.get("grid_side_current_c_a"), " A"))),
                    '<div style="font-size:0.75em;font-weight:700;color:#6c757d;text-transform:uppercase;letter-spacing:0.05em;padding:6px 0 2px">Power</div>',
                    stat_row("Frequency", esc(fmt(pcs.get("grid_side_frequency_hz"), " Hz"))),
                    stat_row("Power Factor", esc(fmt(pcs.get("grid_side_power_factor")))),
                    stat_row("Active Power", esc(fmt(pcs.get("grid_side_active_power_kw"), " kW"))),
                    stat_row("Reactive Power", esc(fmt(pcs.get("grid_side_reactive_power_kvar"), " kvar"))),
                    stat_row("Apparent Power", esc(fmt(pcs.get("grid_side_apparent_power_kva"), " kVA"))),
                ],
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
        if "huawei_target_kw" in payload and payload["huawei_target_kw"] is not None:
            value = max(0.0, min(float(self.opts.huawei_max_power_kw), float(payload["huawei_target_kw"])))
            self.state["control"]["huawei_target_kw"] = value
        if "huawei_control_mode" in payload and payload["huawei_control_mode"] is not None:
            mode = str(payload["huawei_control_mode"]).strip().lower()
            if mode not in ("derate_percent", "target_kw"):
                return False, "huawei_control_mode must be 'derate_percent' or 'target_kw'"
            self.state["control"]["huawei_control_mode"] = mode
        if "huawei_follow_pcs_load" in payload and payload["huawei_follow_pcs_load"] is not None:
            self.state["control"]["huawei_follow_pcs_load"] = bool(payload["huawei_follow_pcs_load"])
        if "pv_charge_enable" in payload and payload["pv_charge_enable"] is not None:
            self.state["control"]["pv_charge_enable"] = bool(payload["pv_charge_enable"])
        if "pv_charge_extra_kw" in payload and payload["pv_charge_extra_kw"] is not None:
            value = max(0.0, float(payload["pv_charge_extra_kw"]))
            self.state["control"]["pv_charge_extra_kw"] = value
        if "outage_prevent_overcharge_enable" in payload and payload["outage_prevent_overcharge_enable"] is not None:
            self.state["control"]["outage_prevent_overcharge_enable"] = bool(payload["outage_prevent_overcharge_enable"])
        if "outage_prevent_draw_kw" in payload and payload["outage_prevent_draw_kw"] is not None:
            value = max(0.0, float(payload["outage_prevent_draw_kw"]))
            self.state["control"]["outage_prevent_draw_kw"] = value
        if "outage_prevent_soc_low" in payload and payload["outage_prevent_soc_low"] is not None:
            value = int(max(0, min(100, int(payload["outage_prevent_soc_low"]))))
            self.state["control"]["outage_prevent_soc_low"] = value
        if "outage_prevent_soc_high" in payload and payload["outage_prevent_soc_high"] is not None:
            value = int(max(0, min(100, int(payload["outage_prevent_soc_high"]))))
            self.state["control"]["outage_prevent_soc_high"] = value
        if "huawei_target_ramp_enable" in payload and payload["huawei_target_ramp_enable"] is not None:
            self.state["control"]["huawei_target_ramp_enable"] = bool(payload["huawei_target_ramp_enable"])
        if "huawei_target_ramp_kw_per_cycle" in payload and payload["huawei_target_ramp_kw_per_cycle"] is not None:
            value = max(0.0, float(payload["huawei_target_ramp_kw_per_cycle"]))
            self.state["control"]["huawei_target_ramp_kw_per_cycle"] = value
            # Backward compatibility: if old single-step is provided, apply it to both.
            self.state["control"]["huawei_target_ramp_up_kw_per_cycle"] = value
            self.state["control"]["huawei_target_ramp_down_kw_per_cycle"] = value
        if "huawei_target_ramp_up_kw_per_cycle" in payload and payload["huawei_target_ramp_up_kw_per_cycle"] is not None:
            value = max(0.0, float(payload["huawei_target_ramp_up_kw_per_cycle"]))
            self.state["control"]["huawei_target_ramp_up_kw_per_cycle"] = value
        if "huawei_target_ramp_down_kw_per_cycle" in payload and payload["huawei_target_ramp_down_kw_per_cycle"] is not None:
            value = max(0.0, float(payload["huawei_target_ramp_down_kw_per_cycle"]))
            self.state["control"]["huawei_target_ramp_down_kw_per_cycle"] = value
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
                        try:
                            result = asyncio.run_coroutine_threadsafe(
                                service.scan_registers(device, int(start_addr), int(end_addr), reg_type),
                                service.loop
                            ).result(timeout=60)
                            self._send_json(200, result)
                        except TimeoutError:
                            self._send_json(504, {"ok": False, "error": "Scan timeout (exceeded 60 seconds)"})
                        except Exception as scan_exc:  # noqa: BLE001
                            error_msg = str(scan_exc).split("\n")[0][:200]
                            _LOG.exception("Scan error")
                            self._send_json(500, {"ok": False, "error": error_msg})
                    except Exception as outer_exc:  # noqa: BLE001
                        error_msg = str(outer_exc).split("\n")[0][:200]
                        _LOG.exception("Outer scan error")
                        self._send_json(500, {"ok": False, "error": error_msg})
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
                if self.opts.enable_pcs_direct:
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
                if self.opts.enable_pcs_control and (self.opts.enable_pcs_direct or self.server_ctx is not None):
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

            async def read_optional_scaled_u16(name: str) -> float | None:
                point = points.get(name)
                if not isinstance(point, dict) or "address" not in point:
                    return None
                try:
                    raw = await self._read_u16(
                        self.huawei_client,
                        int(point["address"]),
                        self.opts.huawei_unit_id,
                        self.opts.huawei_address_offset,
                    )
                except Exception:
                    return None
                scale = float(point.get("scale", 1.0))
                return raw * scale

            pv1_v = await read_optional_scaled_u16("pv1_voltage_v")
            pv1_a = await read_optional_scaled_u16("pv1_current_a")
            pv2_v = await read_optional_scaled_u16("pv2_voltage_v")
            pv2_a = await read_optional_scaled_u16("pv2_current_a")
            pv3_v = await read_optional_scaled_u16("pv3_voltage_v")
            pv3_a = await read_optional_scaled_u16("pv3_current_a")
            pv4_v = await read_optional_scaled_u16("pv4_voltage_v")
            pv4_a = await read_optional_scaled_u16("pv4_current_a")
            pv_target_scale = float(points["pv_target_value"].get("scale", 1.0))

            self.state["huawei"]["device_status"] = status
            self.state["huawei"]["device_status_text"] = f"{self._decode_huawei_status(status)} ({status})"
            self.state["huawei"]["active_power_kw"] = pwr / 1000.0
            self.state["huawei"]["meter_active_power_w"] = meter
            self.state["huawei"]["pv_target_value"] = pv_target * pv_target_scale
            self.state["huawei"]["pv1_voltage_v"] = pv1_v
            self.state["huawei"]["pv1_current_a"] = pv1_a
            self.state["huawei"]["pv2_voltage_v"] = pv2_v
            self.state["huawei"]["pv2_current_a"] = pv2_a
            self.state["huawei"]["pv3_voltage_v"] = pv3_v
            self.state["huawei"]["pv3_current_a"] = pv3_a
            self.state["huawei"]["pv4_voltage_v"] = pv4_v
            self.state["huawei"]["pv4_current_a"] = pv4_a
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
            points = self.map.get("pcs", {}).get("status", {})
            if not isinstance(points, dict):
                raise RuntimeError("pcs.status map is invalid")

            had_success = False

            async def read_point(name: str, signed: bool = False) -> int | None:
                nonlocal had_success
                point = points.get(name)
                if not isinstance(point, dict) or "address" not in point:
                    return None
                try:
                    address = int(point["address"])
                    if signed:
                        value = await self._read_i16(
                            self.pcs_client,
                            address,
                            self.opts.pcs_unit_id,
                            self.opts.pcs_address_offset,
                            input_reg=True,
                        )
                    else:
                        value = await self._read_u16(
                            self.pcs_client,
                            address,
                            self.opts.pcs_unit_id,
                            self.opts.pcs_address_offset,
                            input_reg=True,
                        )
                    had_success = True
                    return value
                except Exception:
                    return None

            alarm = await read_point("alarm_word_1")
            status = await read_point("pcs_status_word")
            monitor = await read_point("monitor_alarm_word")
            total_power = await read_point("total_power_meter", signed=True)
            load_p = await read_point("load_active_power", signed=True)
            load_q = await read_point("load_reactive_power", signed=True)
            load_s = await read_point("load_apparent_power", signed=True)
            dc_v = await read_point("dc_side_voltage_v", signed=True)
            dc_i = await read_point("dc_side_current_a", signed=True)
            dc_kw = await read_point("dc_side_power_kw", signed=True)
            igbt_t = await read_point("igbt_temperature_c", signed=True)
            amb_t = await read_point("ambient_temperature_c", signed=True)
            soc = await read_point("soc")
            soh = await read_point("soh")

            battery_voltage = await read_point("battery_voltage")
            battery_current = await read_point("battery_current", signed=True)
            max_cell_voltage_mv = await read_point("max_cell_voltage_mv")
            min_cell_voltage_mv = await read_point("min_cell_voltage_mv")
            max_cell_temp_c = await read_point("max_cell_temp_c")
            min_cell_temp_c = await read_point("min_cell_temp_c")
            charge_current_limit_a = await read_point("charge_current_limit_a")
            discharge_current_limit_a = await read_point("discharge_current_limit_a")
            allow_charge_power_kw = await read_point("allow_charge_power_kw")
            allow_discharge_power_kw = await read_point("allow_discharge_power_kw")
            battery_status = await read_point("battery_status")

            grid_v_ab = await read_point("grid_voltage_ab", signed=True)
            grid_v_bc = await read_point("grid_voltage_bc", signed=True)
            grid_v_ca = await read_point("grid_voltage_ca", signed=True)

            inv_i_a = await read_point("inverter_current_a", signed=True)
            inv_i_b = await read_point("inverter_current_b", signed=True)
            inv_i_c = await read_point("inverter_current_c", signed=True)
            inv_freq = await read_point("inverter_frequency")
            inv_pf = await read_point("pcs_power_factor", signed=True)

            gsv_ab = await read_point("grid_side_voltage_ab", signed=True)
            gsv_bc = await read_point("grid_side_voltage_bc", signed=True)
            gsv_ca = await read_point("grid_side_voltage_ca", signed=True)
            gsi_a = await read_point("grid_side_current_a", signed=True)
            gsi_b = await read_point("grid_side_current_b", signed=True)
            gsi_c = await read_point("grid_side_current_c", signed=True)
            gsf = await read_point("grid_side_frequency")
            gspf = await read_point("grid_side_power_factor", signed=True)
            gsp = await read_point("grid_side_active_power", signed=True)
            gsq = await read_point("grid_side_reactive_power", signed=True)
            gss = await read_point("grid_side_apparent_power", signed=True)

            if alarm is not None:
                self.state["pcs"]["alarm_word_1"] = alarm
            if status is not None:
                self.state["pcs"]["pcs_status_word"] = status
            if monitor is not None:
                self.state["pcs"]["monitor_alarm_word"] = monitor
                # Bit 2 in monitor alarm word is EMS communication failure in protocol docs.
                self.state["pcs"]["ems_comm_failure"] = bool(monitor & (1 << 2))
            if total_power is not None:
                self.state["pcs"]["total_power_meter_kw"] = total_power / 10.0
            if load_p is not None:
                self.state["pcs"]["load_active_power_kw"] = load_p / 10.0
            if load_q is not None:
                self.state["pcs"]["load_reactive_power_kvar"] = load_q / 10.0
            if load_s is not None:
                self.state["pcs"]["load_apparent_power_kva"] = load_s / 10.0
            if dc_v is not None:
                self.state["pcs"]["dc_side_voltage_v"] = dc_v / 10.0
            if dc_i is not None:
                self.state["pcs"]["dc_side_current_a"] = dc_i / 10.0
            if dc_kw is not None:
                self.state["pcs"]["dc_side_power_kw"] = dc_kw / 10.0
            if igbt_t is not None:
                self.state["pcs"]["igbt_temperature_c"] = igbt_t / 10.0
            if amb_t is not None:
                self.state["pcs"]["ambient_temperature_c"] = amb_t / 10.0
            if soc is not None:
                self.state["pcs"]["soc"] = soc
            if soh is not None:
                self.state["pcs"]["soh"] = soh

            if battery_voltage is not None:
                self.state["pcs"]["battery_voltage_v"] = battery_voltage / 10.0
            if battery_current is not None:
                self.state["pcs"]["battery_current_a"] = battery_current / 10.0
            if max_cell_voltage_mv is not None:
                self.state["pcs"]["max_cell_voltage_mv"] = max_cell_voltage_mv
            if min_cell_voltage_mv is not None:
                self.state["pcs"]["min_cell_voltage_mv"] = min_cell_voltage_mv
            if max_cell_temp_c is not None:
                self.state["pcs"]["max_cell_temp_c"] = max_cell_temp_c / 10.0
            if min_cell_temp_c is not None:
                self.state["pcs"]["min_cell_temp_c"] = min_cell_temp_c / 10.0
            if charge_current_limit_a is not None:
                self.state["pcs"]["charge_current_limit_a"] = charge_current_limit_a / 10.0
            if discharge_current_limit_a is not None:
                self.state["pcs"]["discharge_current_limit_a"] = discharge_current_limit_a / 10.0
            if allow_charge_power_kw is not None:
                self.state["pcs"]["allow_charge_power_kw"] = allow_charge_power_kw
            if allow_discharge_power_kw is not None:
                self.state["pcs"]["allow_discharge_power_kw"] = allow_discharge_power_kw
            if battery_status is not None:
                self.state["pcs"]["battery_status"] = battery_status

            if grid_v_ab is not None and grid_v_bc is not None and grid_v_ca is not None:
                self.state["pcs"]["grid_voltage_ab_v"] = grid_v_ab / 10.0
                self.state["pcs"]["grid_voltage_bc_v"] = grid_v_bc / 10.0
                self.state["pcs"]["grid_voltage_ca_v"] = grid_v_ca / 10.0

                avg_grid_v = (
                    abs(self.state["pcs"]["grid_voltage_ab_v"])
                    + abs(self.state["pcs"]["grid_voltage_bc_v"])
                    + abs(self.state["pcs"]["grid_voltage_ca_v"])
                ) / 3.0
                grid_available = avg_grid_v >= self.opts.grid_voltage_present_threshold_v
                self.state["grid"]["is_available"] = grid_available
                self.state["grid"]["avg_voltage_v"] = avg_grid_v
            else:
                grid_available = bool(self.state["grid"].get("is_available", True))

            if inv_i_a is not None and inv_i_b is not None and inv_i_c is not None:
                self.state["pcs"]["inverter_current_a_a"] = inv_i_a / 10.0
                self.state["pcs"]["inverter_current_b_a"] = inv_i_b / 10.0
                self.state["pcs"]["inverter_current_c_a"] = inv_i_c / 10.0
            if inv_freq is not None:
                self.state["pcs"]["inverter_frequency_hz"] = inv_freq / 100.0
            if inv_pf is not None:
                self.state["pcs"]["pcs_power_factor"] = inv_pf

            if gsv_ab is not None:
                self.state["pcs"]["grid_side_voltage_ab_v"] = gsv_ab / 10.0
            if gsv_bc is not None:
                self.state["pcs"]["grid_side_voltage_bc_v"] = gsv_bc / 10.0
            if gsv_ca is not None:
                self.state["pcs"]["grid_side_voltage_ca_v"] = gsv_ca / 10.0
            if gsi_a is not None:
                self.state["pcs"]["grid_side_current_a_a"] = gsi_a / 10.0
            if gsi_b is not None:
                self.state["pcs"]["grid_side_current_b_a"] = gsi_b / 10.0
            if gsi_c is not None:
                self.state["pcs"]["grid_side_current_c_a"] = gsi_c / 10.0
            if gsf is not None:
                self.state["pcs"]["grid_side_frequency_hz"] = gsf / 100.0
            if gspf is not None:
                self.state["pcs"]["grid_side_power_factor"] = round(gspf / 100.0, 2)
            if gsp is not None:
                self.state["pcs"]["grid_side_active_power_kw"] = round(gsp / 10.0, 1)
            if gsq is not None:
                self.state["pcs"]["grid_side_reactive_power_kvar"] = round(gsq / 10.0, 1)
            if gss is not None:
                self.state["pcs"]["grid_side_apparent_power_kva"] = round(gss / 10.0, 1)

            if self._last_grid_available is None:
                self._last_grid_available = grid_available
            elif self._last_grid_available != grid_available:
                self.state["grid"]["last_transition"] = "return" if grid_available else "fail"
                self._last_grid_available = grid_available

            self.state["status"]["pcs_connected"] = had_success
            self.state["status"]["pcs_last_error"] = None if had_success else "No configured PCS points readable"
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
                    results[str(addr)] = {
                        "success": False,
                        "error": str(rr),
                        "requested_address": addr,
                        "wire_address": wire_address,
                    }
                else:
                    u16 = int(rr.registers[0]) & 0xFFFF
                    s16 = self._decode_i16(u16)
                    results[str(addr)] = {
                        "success": True,
                        "requested_address": addr,
                        "wire_address": wire_address,
                        "u16": u16,
                        "s16": s16,
                    }
            except Exception as exc:  # noqa: BLE001
                results[str(addr)] = {
                    "success": False,
                    "error": str(exc).split("\n")[0][:100],
                    "requested_address": addr,
                    "wire_address": wire_address,
                }
        
        return {
            "device": device,
            "reg_type": reg_type,
            "start_addr": start_addr,
            "end_addr": end_addr,
            "address_offset": offset,
            "results": results,
        }

    async def _probe_pcs_heartbeat(self) -> None:
        status_points = self.map.get("pcs", {}).get("status", {})
        heartbeat_point = status_points.get("heartbeat") if isinstance(status_points, dict) else None
        if not isinstance(heartbeat_point, dict) or "address" not in heartbeat_point:
            self.state["pcs_direct_probe"]["last_error"] = "heartbeat probe disabled (not configured)"
            self.state["pcs_direct_probe"]["last_heartbeat"] = None
            return
        try:
            heartbeat_addr = int(heartbeat_point["address"])
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
        control_points = self.map["huawei"]["control"]
        mode = str(self.state["control"].get("huawei_control_mode", "derate_percent")).strip().lower()
        if mode == "target_kw":
            point = control_points.get("active_power_kw_target")
            if not isinstance(point, dict):
                point = control_points.get("pv_target_value")
            if not isinstance(point, dict):
                point = {"address": 40120, "scale": 1}

            follow_load = bool(self.state["control"].get("huawei_follow_pcs_load", False))
            target_kw = float(self.state["control"].get("huawei_target_kw", 0.0))
            reason = "huawei_target_kw"
            trace: dict[str, Any] = {
                "time": _now_iso(),
                "mode": "target_kw",
                "grid_available": bool(self.state["grid"].get("is_available", True)),
                "grid_side_active_power_kw_raw": None,
                "grid_import_kw": None,
                "pcs_load_kw_raw": None,
                "source_name": "manual",
                "base_load_kw": None,
                "pv_charge_extra_requested_kw": 0.0,
                "allow_charge_kw": None,
                "pv_charge_extra_applied_kw": 0.0,
                "target_pre_outage_kw": None,
                "outage_prevent_draw_active": bool(self.state["control"].get("outage_prevent_draw_active", False)),
                "outage_draw_applied_kw": 0.0,
                "target_pre_ramp_kw": None,
                "prev_target_kw": self.state["control"].get("huawei_target_last_kw_written"),
                "delta_kw": None,
                "ramp_up_step_kw": None,
                "ramp_down_step_kw": None,
                "ramp_action": "none",
                "target_final_kw": None,
                "write_raw": None,
                "write_scale": None,
                "policy_reason": None,
            }
            if follow_load:
                grid_available = bool(self.state["grid"].get("is_available", True))
                grid_incoming_kw = self.state["pcs"].get("grid_side_active_power_kw")
                pcs_load_kw = self.state["pcs"].get("load_active_power_kw")
                trace["grid_available"] = grid_available
                trace["grid_side_active_power_kw_raw"] = grid_incoming_kw
                trace["pcs_load_kw_raw"] = pcs_load_kw

                # Requested behaviour:
                # - Grid ON: follow incoming grid kW.
                # - Grid OFF: follow PCS load kW.
                # Sign convention note: grid_side_active_power_kw < 0 means importing from grid.
                source_name = ""
                source_kw: float | None = None
                if grid_available and grid_incoming_kw is not None:
                    source_name = "grid_incoming"
                    source_kw = max(0.0, -float(grid_incoming_kw))
                    trace["grid_import_kw"] = source_kw
                elif (not grid_available) and pcs_load_kw is not None:
                    source_name = "pcs_load"
                    source_kw = abs(float(pcs_load_kw))
                elif pcs_load_kw is not None:
                    source_name = "pcs_load_fallback"
                    source_kw = abs(float(pcs_load_kw))
                elif grid_incoming_kw is not None:
                    source_name = "grid_incoming_fallback"
                    source_kw = max(0.0, -float(grid_incoming_kw))
                    trace["grid_import_kw"] = source_kw

                if source_kw is not None:
                    base_load_kw = source_kw
                    trace["source_name"] = source_name
                    trace["base_load_kw"] = base_load_kw
                    if bool(self.state["control"].get("pv_charge_enable", False)):
                        extra_kw = max(0.0, float(self.state["control"].get("pv_charge_extra_kw", 0.0)))
                        trace["pv_charge_extra_requested_kw"] = extra_kw
                        allow_charge = float(self.state["pcs"].get("allow_charge_power_kw", 0.0) or 0.0)
                        trace["allow_charge_kw"] = allow_charge
                        if allow_charge > 0.0:
                            extra_kw = min(extra_kw, allow_charge)
                        else:
                            extra_kw = 0.0
                        trace["pv_charge_extra_applied_kw"] = extra_kw
                        target_kw = base_load_kw + extra_kw
                        reason = f"huawei_target_kw_follow_{source_name}_with_charge"
                    else:
                        target_kw = base_load_kw
                        reason = f"huawei_target_kw_follow_{source_name}_no_charge"
                else:
                    trace["source_name"] = "no_source"
                    reason = "huawei_target_kw_follow_no_source_value"

            trace["target_pre_outage_kw"] = target_kw

            # Grid-fail anti-overcharge hysteresis:
            # when active, we intentionally keep a small PCS draw by reducing Huawei target.
            if bool(self.state["control"].get("outage_prevent_overcharge_enable", False)):
                grid_available = bool(self.state["grid"].get("is_available", True))
                soc_raw = self.state["pcs"].get("soc")
                if not grid_available and soc_raw is not None:
                    soc = int(soc_raw)
                    low = int(self.state["control"].get("outage_prevent_soc_low", 95))
                    high = int(self.state["control"].get("outage_prevent_soc_high", 98))
                    if low > high:
                        low, high = high, low
                    active = bool(self.state["control"].get("outage_prevent_draw_active", False))
                    if active and soc <= low:
                        active = False
                    elif (not active) and soc >= high:
                        active = True
                    self.state["control"]["outage_prevent_draw_active"] = active
                    if active:
                        draw_kw = max(0.0, float(self.state["control"].get("outage_prevent_draw_kw", 2.0)))
                        target_kw = max(0.0, target_kw - draw_kw)
                        trace["outage_draw_applied_kw"] = draw_kw
                        reason = "outage_prevent_overcharge_draw"
                elif grid_available:
                    self.state["control"]["outage_prevent_draw_active"] = False
            trace["outage_prevent_draw_active"] = bool(self.state["control"].get("outage_prevent_draw_active", False))
            trace["target_pre_ramp_kw"] = target_kw

            # Smooth target changes to avoid oscillation and abrupt inverter commands.
            if bool(self.state["control"].get("huawei_target_ramp_enable", True)):
                fallback_step = max(0.0, float(self.state["control"].get("huawei_target_ramp_kw_per_cycle", 0.5)))
                ramp_up_step = max(0.0, float(self.state["control"].get("huawei_target_ramp_up_kw_per_cycle", fallback_step)))
                ramp_down_step = max(0.0, float(self.state["control"].get("huawei_target_ramp_down_kw_per_cycle", fallback_step)))
                trace["ramp_up_step_kw"] = ramp_up_step
                trace["ramp_down_step_kw"] = ramp_down_step
                prev_target_raw = self.state["control"].get("huawei_target_last_kw_written")
                if prev_target_raw is not None:
                    prev_target = float(prev_target_raw)
                    delta = target_kw - prev_target
                    trace["prev_target_kw"] = prev_target
                    trace["delta_kw"] = delta
                    if delta > ramp_up_step and ramp_up_step > 0.0:
                        target_kw = prev_target + ramp_up_step
                        trace["ramp_action"] = "up_limited"
                        reason = f"{reason}_ramp_up_limited"
                    elif delta < -ramp_down_step and ramp_down_step > 0.0:
                        target_kw = prev_target - ramp_down_step
                        trace["ramp_action"] = "down_limited"
                        reason = f"{reason}_ramp_down_limited"

            target_kw = max(0.0, min(float(self.opts.huawei_max_power_kw), target_kw))
            scale = float(point.get("scale", 1.0))
            if scale <= 0:
                scale = 1.0
            raw = int(round(target_kw / scale))
            await self._write_u16(
                self.huawei_client,
                int(point["address"]),
                raw,
                self.opts.huawei_unit_id,
                self.opts.huawei_address_offset,
            )
            trace["target_final_kw"] = target_kw
            trace["write_raw"] = raw
            trace["write_scale"] = scale
            trace["policy_reason"] = reason
            self.state["control"]["huawei_target_last_kw_written"] = target_kw
            self.state["control"]["huawei_trace"] = trace
            self.state["control"]["policy_reason"] = reason
            return

        point = control_points["active_power_percentage_derating"]
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
        self.state["control"]["huawei_trace"] = {
            "time": _now_iso(),
            "mode": "derate_percent",
            "target_final_kw": None,
            "write_raw": raw,
            "write_scale": 0.1,
            "policy_reason": self.state["control"].get("policy_reason"),
        }

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
