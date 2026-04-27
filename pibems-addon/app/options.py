"""Runtime option and register-map loading.

Home Assistant injects add-on options via a JSON file. Keeping the parsing logic
here makes the startup path clearer and avoids mixing configuration concerns into
the EMS runtime implementation.
"""

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any
import logging

import yaml


_LOG = logging.getLogger("pibems")


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


def load_options(path: Path) -> Options:
    """Load Home Assistant add-on options and overlay them onto defaults."""
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
    """Load the YAML register map used for Huawei, PCS, and EMS server points."""
    if not path.exists():
        raise FileNotFoundError(f"register map file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))