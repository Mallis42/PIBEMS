import asyncio
import json
import logging
import signal
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import yaml
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext, ModbusSlaveContext
from pymodbus.server import StartAsyncTcpServer

_LOG = logging.getLogger("pibems")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)


@dataclass
class Options:
    huawei_host: str = "192.168.1.20"
    huawei_port: int = 502
    huawei_unit_id: int = 1
    huawei_address_offset: int = 0
    huawei_max_power_kw: float = 50.0

    pcs_host: str = "192.168.1.30"
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

        self.state: dict[str, Any] = {
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
        self.httpd: ThreadingHTTPServer | None = None
        self.http_thread: threading.Thread | None = None

    def _health_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "huawei_enabled": self.opts.enable_huawei,
            "pcs_enabled": self.opts.enable_pcs_direct,
            "ems_server_enabled": self.opts.enable_ems_server,
            "operation_mode": self.state["control"].get("operation_mode", "control"),
        }

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
                if self.path == "/health":
                    self._send_json(200, service._health_payload())
                    return
                if self.path == "/api/diagnostics":
                    self._send_json(200, service.state)
                    return
                self._send_json(404, {"ok": False, "error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/api/control":
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
        store = ModbusSlaveContext(ir=ir_block, hr=hr_block)
        self.server_ctx = ModbusServerContext(slaves={self.opts.ems_unit_id: store}, single=False)

        for addr, value in input_defaults.items():
            store.setValues(4, addr, [value])
        for addr, value in holding_defaults.items():
            store.setValues(3, addr, [value])

        _LOG.info(
            "Starting EMS Modbus server on %s:%s unit=%s",
            self.opts.ems_bind_host,
            self.opts.ems_bind_port,
            self.opts.ems_unit_id,
        )
        await StartAsyncTcpServer(
            context=self.server_ctx,
            address=(self.opts.ems_bind_host, self.opts.ems_bind_port),
        )

    async def _poll_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if self.opts.enable_huawei:
                    await self._poll_huawei()
                if self.opts.enable_pcs_direct:
                    await self._poll_pcs()
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
                if self.opts.enable_pcs_direct:
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
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2219, [self.heartbeat_counter])

                # Mirror latest telemetry into server-side registers the PCS may poll.
                meter_kw = float(self.state["pcs"].get("total_power_meter_kw", 0.0))
                load_kw = float(self.state["pcs"].get("load_active_power_kw", 0.0))
                load_kvar = float(self.state["pcs"].get("load_reactive_power_kvar", 0.0))
                load_kva = float(self.state["pcs"].get("load_apparent_power_kva", 0.0))

                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2218, [self._encode_i16(meter_kw * 10)])
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2326, [self._encode_i16(load_kw * 10)])
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2327, [self._encode_i16(load_kvar * 10)])
                self.server_ctx[self.opts.ems_unit_id].setValues(4, 2328, [self._encode_i16(load_kva * 10)])

            await asyncio.sleep(self.opts.heartbeat_interval_sec)

    async def _poll_huawei(self) -> None:
        points = self.map["huawei"]["status"]
        status = await self._read_u16(self.huawei_client, points["device_status"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)
        pwr = await self._read_i32(self.huawei_client, points["active_power"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)
        meter = await self._read_i32(self.huawei_client, points["meter_active_power"]["address"], self.opts.huawei_unit_id, self.opts.huawei_address_offset)

        self.state["huawei"]["device_status"] = status
        self.state["huawei"]["active_power_kw"] = pwr / 1000.0
        self.state["huawei"]["meter_active_power_w"] = meter

    async def _poll_pcs(self) -> None:
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

        # Ensure control mode bits are primed before writing power commands.
        await self._write_u16(self.pcs_client, control["grid_connected_mode"]["address"], 3, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
        await self._write_u16(self.pcs_client, control["power_control_type"]["address"], 2, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
        await self._write_u16(self.pcs_client, control["remote_on_off"]["address"], self.opts.pcs_start_command, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
        await self._write_u16(self.pcs_client, control["control_mode"]["address"], 1, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
        await self._write_u16(self.pcs_client, control["discharge_stop_soc"]["address"], self.opts.outage_reserve_soc, self.opts.pcs_unit_id, self.opts.pcs_address_offset)
        await self._write_u16(self.pcs_client, control["charge_stop_soc"]["address"], self.opts.outage_max_soc_from_huawei, self.opts.pcs_unit_id, self.opts.pcs_address_offset)

        grid_available = bool(self.state["grid"].get("is_available", True))
        huawei_kw = float(self.state["huawei"].get("active_power_kw", 0.0))
        no_sun = huawei_kw < self.opts.solar_present_threshold_kw
        if not grid_available and soc <= self.opts.outage_shutdown_soc_no_sun and no_sun:
            target = 0.0
            await self._write_u16(
                self.pcs_client,
                control["remote_on_off"]["address"],
                self.opts.pcs_stop_command,
                self.opts.pcs_unit_id,
                self.opts.pcs_address_offset,
            )
            self.state["control"]["policy_reason"] = "outage_shutdown_low_soc_no_sun"
        elif grid_available and self.opts.auto_start_on_grid_return:
            await self._write_u16(
                self.pcs_client,
                control["remote_on_off"]["address"],
                self.opts.pcs_start_command,
                self.opts.pcs_unit_id,
                self.opts.pcs_address_offset,
            )

        raw_cmd = self._encode_i16(int(round(target * 10)))
        await self._write_u16(
            self.pcs_client,
            control["constant_power_command"]["address"],
            raw_cmd,
            self.opts.pcs_unit_id,
            self.opts.pcs_address_offset,
        )
        self.state["control"]["last_written_pcs_power_kw"] = target

        if self.server_ctx is not None:
            self.server_ctx[self.opts.ems_unit_id].setValues(3, 2761, [raw_cmd])
            self.server_ctx[self.opts.ems_unit_id].setValues(3, 2765, [3])
            self.server_ctx[self.opts.ems_unit_id].setValues(3, 2768, [2])
            self.server_ctx[self.opts.ems_unit_id].setValues(3, 2769, [0x5555])
            self.server_ctx[self.opts.ems_unit_id].setValues(3, 2770, [1])

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

    async def _read_u16(
        self,
        client: AsyncModbusTcpClient,
        address: int,
        unit_id: int,
        offset: int,
        input_reg: bool = False,
    ) -> int:
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
