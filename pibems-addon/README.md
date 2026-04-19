# PIBEMS Home Assistant Add-on (Open Source Scaffold)

This scaffold provides a starting point for an open-source EMS add-on that can:

- control Huawei inverter active power derating
- control Megarevo PCS battery charge/discharge power
- emulate an EMS Modbus TCP server with heartbeat and key telemetry registers
- expose diagnostics and runtime control endpoints over HTTP
- enforce outage-aware SOC reserve policies and auto start/stop behavior
- apply dynamic charging limits using PCS-exposed battery charge voltage/current limits
- toggle between control mode and read-only mode for commissioning safety

## Current capabilities

- Async Modbus TCP clients for Huawei and Megarevo PCS
- Async Modbus TCP server for EMS emulation mode
- Heartbeat register update at 2219 (incrementing counter)
- Telemetry mirror into server input registers 2218, 2326, 2327, 2328
- Basic safety limits for charge/discharge and SOC windows
- Outage policy controls:
  - reserve SOC while grid is down
  - optional charge from grid on return to reserve SOC
  - optional PCS auto start on grid return
  - optional PCS shutdown at low SOC when no solar is present
- Dynamic charge protection:
  - reads PCS-side charge voltage/current limit registers
  - computes dynamic max charge power as V * A with safety margin
- API:
  - `GET /health`
  - `GET /api/diagnostics`
  - `POST /api/control`

## Control API payload

`POST /api/control`

```json
{
  "target_power_kw": -20,
  "huawei_derate_percent": 80,
  "auto_mode_enabled": true,
  "operation_mode": "control"
}
```

- `target_power_kw`: positive = discharge, negative = charge
- `auto_mode_enabled=true`: outage policy and auto-recovery logic can override manual target
- `auto_mode_enabled=false`: manual target applies, still bounded by safety limits
- `operation_mode="control"`: enables Modbus write commands
- `operation_mode="read_only"`: blocks all Modbus writes and keeps telemetry/diagnostics only

## Folder structure

- `config.yaml`: Home Assistant add-on metadata and options schema
- `Dockerfile`: add-on build
- `requirements.txt`: Python dependencies
- `config/register_map.yaml`: register definitions for Huawei, PCS, and EMS server defaults
- `app/main.py`: EMS runtime service
- `examples/homeassistant/rest.yaml`: starter Home Assistant entities

## Notes on register addressing

Modbus address conventions vary by vendor and library.

- This project exposes `huawei_address_offset` and `pcs_address_offset` options.
- If reads/writes are off by one register, set offset to `-1`.
- If your device expects absolute documented addresses, keep offset at `0`.

## Running in Home Assistant

1. Copy this folder into your local add-ons directory.
2. Build/install the add-on from Home Assistant.
3. Configure options in add-on UI.
4. Start add-on.
5. Verify:
   - `GET http://<ha-host>:8080/health`
   - `GET http://<ha-host>:8080/api/diagnostics`

## Home Assistant UI card package

Example UI files are provided in:

- `examples/homeassistant/rest.yaml`
- `examples/homeassistant/package_pibems_ui.yaml`
- `examples/homeassistant/dashboard_pibems.yaml`

Recommended setup:

1. Merge `rest.yaml` into your Home Assistant configuration.
2. Merge `package_pibems_ui.yaml` into your Home Assistant packages.
3. Create a new dashboard and paste `dashboard_pibems.yaml` as the raw YAML.

This gives you:

- a power-flow view card
- live telemetry stats
- control widgets for mode, target power, and Huawei derating
- one-click push of values to the PIBEMS API

## Safe commissioning order

1. Huawei read-only telemetry first.
2. Huawei derating command with conservative values.
3. PCS read-only telemetry.
4. PCS direct command at low power and with SOC limits.
5. EMS server emulation mode last.

## Disclaimer

This is a commissioning scaffold, not production-certified EMS software.
Validate all control writes with your exact PCS/inverter firmware and site protection requirements.
