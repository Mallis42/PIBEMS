# PIBEMS Home Assistant Add-on Repository

This repository contains a custom Home Assistant add-on for PIBEMS.

## Add-on

- `pibems-addon`: Open-source EMS for Huawei + Megarevo PCS.

## Add this repository to Home Assistant

1. In Home Assistant, go to **Settings -> Add-ons -> Add-on Store**.
2. Open the top-right menu and select **Repositories**.
3. Add this GitHub URL:
   - `https://github.com/Mallis42/PIBEMS`
4. Install the **PIBEMS** add-on from the store.

## First startup safety

Start with `operation_mode: read_only` and verify telemetry before enabling control writes.
