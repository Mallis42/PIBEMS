"""Server-rendered UI helpers.

The project deliberately uses plain HTML templates plus Python string assembly.
That keeps the add-on lightweight, removes any dependency on a separate web
framework, and still lets us keep bulky markup out of the runtime service file.
"""

from datetime import datetime
from functools import lru_cache
import html
import json
from pathlib import Path
from typing import Any


_TEMPLATES_DIR = Path(__file__).with_name("templates")


@lru_cache(maxsize=None)
def _load_template(name: str) -> str:
    """Cache template files because they are static for the lifetime of the process."""
    return (_TEMPLATES_DIR / name).read_text(encoding="utf-8")


def _render_template(name: str, replacements: dict[str, str]) -> str:
    """Render a static HTML file using simple token replacement.

    A full template engine would be overkill here. The pages are small, static,
    and rendered entirely on the server, so predictable token replacement keeps
    the stack simple and easy to debug.
    """
    html_text = _load_template(name)
    for token, value in replacements.items():
        html_text = html_text.replace(token, value)
    return html_text


def render_scanner_html() -> str:
    """Return the static Modbus scanner page."""
    return _load_template("scanner.html")


def render_dashboard_html(state: dict[str, Any]) -> str:
    """Render the diagnostics dashboard from the current runtime state."""

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
            f"{esc(text)}"
            "</span>"
        )

    def stat_row(label: str, value: str) -> str:
        return (
            '<div class="stat-row">'
            f'<span class="stat-label">{esc(label)}:</span>'
            f'<span class="stat-value">{value}</span>'
            "</div>"
        )

    def card(title: str, rows: list[str], error: Any = None, full: bool = False) -> str:
        classes = "card full-width" if full else "card"
        error_html = f'<div class="error-msg">{esc(error)}</div>' if error else ""
        return (
            f'<div class="{classes}">'
            f"<h2>{esc(title)}</h2>"
            + "".join(rows)
            + error_html
            + "</div>"
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

    def format_battery_status(value: Any) -> str:
        if value is None:
            return "—"
        status_value = int(value)
        pill_styles = {
            0: "background:#d4edda;color:#155724",
            1: "background:#fff3cd;color:#856404",
            2: "background:#fff3cd;color:#856404",
            3: "background:#f8d7da;color:#721c24",
            4: "background:#f5c6cb;color:#491217",
        }
        pill_text = {
            0: "✅ Normal",
            1: "⚠️ Charge Disable",
            2: "⚠️ Discharge Disable",
            3: "🔴 Alarm",
            4: "❌ Failure",
        }
        style = pill_styles.get(status_value, "background:#e2e3e5;color:#383d41")
        text = pill_text.get(status_value, f"Unknown ({status_value})")
        return (
            '<span style="display:inline-block;padding:2px 10px;border-radius:12px;font-weight:600;font-size:0.92em;'
            + style
            + '">'
            + text
            + "</span>"
        )

    status = state.get("status", {})
    huawei = state.get("huawei", {})
    pcs = state.get("pcs", {})
    control = state.get("control", {})
    server = state.get("server", {})
    grid = state.get("grid", {})
    probe = state.get("pcs_direct_probe", {})
    huawei_trace = control.get("huawei_trace", {}) if isinstance(control, dict) else {}
    now = datetime.now().strftime("%H:%M:%S")

    debug_json = esc(json.dumps(state, indent=2, default=str))
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

    # Each card stays close to the data it displays so it is easy to audit when a
    # new telemetry field or control flag is added.
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
                stat_row("PV Active Raw", esc(fmt(huawei_trace.get("pv_active_power_kw_raw"), " kW"))),
                stat_row("Grid Active Raw", esc(fmt(huawei_trace.get("grid_side_active_power_kw_raw"), " kW"))),
                stat_row("Grid Import Calc", esc(fmt(huawei_trace.get("grid_import_kw"), " kW"))),
                stat_row("Grid Net Calc", esc(fmt(huawei_trace.get("grid_net_kw"), " kW"))),
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
                stat_row("Battery Status", format_battery_status(pcs.get("battery_status"))),
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

    return _render_template(
        "dashboard.html",
        {
            "__LAST_UPDATE__": esc(now),
            "__CARDS__": "".join(cards),
        },
    )