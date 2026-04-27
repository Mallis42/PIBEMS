import asyncio
import json
import logging
import signal
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext
from pymodbus.server import StartAsyncTcpServer

try:
    from .constants import HUAWEI_STATUS_MAP as _HUAWEI_STATUS_MAP
    from .modbus_logging import LoggingSlaveContext as _LoggingSlaveContext
    from .options import Options, load_options, load_register_map
    from .ui import render_dashboard_html, render_scanner_html
except ImportError:
    from constants import HUAWEI_STATUS_MAP as _HUAWEI_STATUS_MAP
    from modbus_logging import LoggingSlaveContext as _LoggingSlaveContext
    from options import Options, load_options, load_register_map
    from ui import render_dashboard_html, render_scanner_html

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
        return render_scanner_html()

    def _get_ui_html(self) -> str:
        return render_dashboard_html(self.state)

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
                point = {"address": 40120, "scale": 0.1}

            follow_load = bool(self.state["control"].get("huawei_follow_pcs_load", False))
            target_kw = float(self.state["control"].get("huawei_target_kw", 0.0))
            reason = "huawei_target_kw"
            trace: dict[str, Any] = {
                "time": _now_iso(),
                "mode": "target_kw",
                "grid_available": bool(self.state["grid"].get("is_available", True)),
                "pv_active_power_kw_raw": None,
                "grid_side_active_power_kw_raw": None,
                "grid_import_kw": None,
                "grid_net_kw": None,
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
                pv_active_kw = float(self.state["huawei"].get("active_power_kw", 0.0) or 0.0)
                grid_incoming_kw = self.state["pcs"].get("grid_side_active_power_kw")
                pcs_load_kw = self.state["pcs"].get("load_active_power_kw")
                trace["grid_available"] = grid_available
                trace["pv_active_power_kw_raw"] = pv_active_kw
                trace["grid_side_active_power_kw_raw"] = grid_incoming_kw
                trace["pcs_load_kw_raw"] = pcs_load_kw

                # Requested behaviour:
                # - Grid ON: follow total site load (PV active + grid net).
                # - Grid OFF: follow PCS load kW only.
                # Sign convention note: grid_side_active_power_kw < 0 means importing from grid.
                source_name = ""
                source_kw: float | None = None
                if grid_available:
                    if grid_incoming_kw is not None:
                        source_name = "site_load_from_pv_grid_net"
                        grid_net_kw = -float(grid_incoming_kw)  # import=positive, export=negative
                        source_kw = max(0.0, pv_active_kw + grid_net_kw)
                        trace["grid_net_kw"] = grid_net_kw
                        trace["grid_import_kw"] = max(0.0, grid_net_kw)
                    elif pcs_load_kw is not None:
                        source_name = "pcs_load_fallback_grid_on"
                        source_kw = abs(float(pcs_load_kw))
                else:
                    if pcs_load_kw is not None:
                        source_name = "pcs_load_grid_off"
                        source_kw = abs(float(pcs_load_kw))

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
