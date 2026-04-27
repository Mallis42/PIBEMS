"""Modbus server helpers.

The runtime uses a custom slave context so every PCS read/write can be traced in
one place. Keeping this class separate makes the main service file much easier to
scan while preserving the existing logging behaviour.
"""

from typing import Any
import logging

from pymodbus.datastore import ModbusSlaveContext

try:
    from .constants import ANSI_PURPLE, ANSI_RESET
except ImportError:
    from constants import ANSI_PURPLE, ANSI_RESET


class LoggingSlaveContext(ModbusSlaveContext):
    """Wrap ModbusSlaveContext and log every PCS-facing register access.

    External reads/writes are important commissioning signals, so they are logged
    loudly. Internal writes from our own control loops are still recorded, but only
    at DEBUG level to avoid burying the interesting traffic.
    """

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
            ANSI_PURPLE, action, reg, address, count, reg_labels, values, ANSI_RESET,
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
                ANSI_PURPLE, reg, address, reg_labels, values, ANSI_RESET,
            )
        else:
            logging.getLogger("pibems").info(
                "%sEMS-SERVER  PCS-WRITE  %s  addr=%s labels=[%s] values=%s%s",
                ANSI_PURPLE, reg, address, reg_labels, values, ANSI_RESET,
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