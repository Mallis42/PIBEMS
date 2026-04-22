# PIBEMS Modbus Register Scanner - Implementation Summary

## Overview
A new **Modbus Register Scanner** feature has been integrated into the PIBEMS addon to help diagnose and identify valid register addresses on your Modbus devices (PCS and Huawei inverter).

## Features Implemented

### 1. **Register Scanner Function** (`scan_registers()`)
- **Location**: [app/main.py](pibems-addon/app/main.py) - `EMSService` class
- **Functionality**: 
  - Scans a configurable range of Modbus addresses
  - Tests each address individually
  - Captures success/failure status and values
  - Supports both input and holding registers
  - Supports both PCS and Huawei devices
  - Respects configured address offsets

### 2. **HTTP API Endpoint** (`/api/scan`)
- **Method**: POST
- **Request format**:
  ```json
  {
    "device": "pcs" | "huawei",
    "start_addr": 2100,
    "end_addr": 2500,
    "reg_type": "input" | "holding"
  }
  ```
- **Response format**:
  ```json
  {
    "device": "pcs",
    "reg_type": "input",
    "start_addr": 2100,
    "end_addr": 2500,
    "address_offset": 0,
    "results": {
      "2100": {"success": true, "value": 123},
      "2219": {"success": false, "error": "IllegalAddress"},
      ...
    }
  }
  ```

### 3. **Web-based Scanner UI** (`/api/scanner`)
- **Access**: Navigate to `http://<ha-host>:8080/api/scanner`
- **Features**:
  - Device selection (PCS or Huawei)
  - Register type selection (Input or Holding)
  - Configurable address range
  - Real-time scan progress indicator
  - Results table with:
    - Address column
    - Status column (✓ Success / ✗ Failed) with color coding
    - Value/Error column
  - Summary statistics (Total, Successful, Failed count)
  - Copy-to-clipboard helper for successful addresses
  - Responsive design with dark theme

### 4. **Dashboard Integration**
- Added link to "Modbus Scanner" in the main dashboard navigation
- Accessible from the refresh notes section on the main PIBEMS Dashboard

## How to Use

### Step 1: Open the Scanner
Navigate to your PIBEMS addon in Home Assistant:
```
http://<ha-host>:8080/api/scanner
```

Or click the "Modbus Scanner" link on the main dashboard.

### Step 2: Configure Scan Parameters
1. **Device**: Select `PCS` or `Huawei`
2. **Register Type**: Select `Input` or `Holding`
3. **Start Address**: Enter the starting register address
4. **End Address**: Enter the ending register address
5. Click **Scan**

### Step 3: Analyze Results
The results will show:
- **Green (Success)**: Register is valid and readable. Value is displayed.
- **Red (Failed)**: Register is invalid/inaccessible. Error message is shown.
- **Summary**: Count of total, successful, and failed addresses

### Step 4: Update Configuration
Once you've identified working registers:
1. Copy the successful addresses from the "Working addresses" section
2. Update [pibems-addon/config/register_map.yaml](pibems-addon/config/register_map.yaml) with correct addresses
3. Restart the addon

## Technical Details

### Implementation Changes

#### 1. EMSService Class Updates
- Added `loop` attribute to store the running event loop (set in `run()`)
- Added `scan_registers()` async method

#### 2. API Handler Updates
- Added `/api/scanner` GET endpoint → returns scanner HTML UI
- Added `/api/scan` POST endpoint → executes scan and returns JSON results
- Uses `asyncio.run_coroutine_threadsafe()` to call async scanner from HTTP handler thread

#### 3. Event Loop Management
- Stores reference to running event loop in `EMSService.run()`
- Allows HTTP handler (running in separate thread) to safely call async functions

## Constraints & Safety

- **Max scan range**: 1000 registers at a time (prevents excessive load)
- **Timeout**: 60 seconds per scan request
- **Thread-safe**: Uses `asyncio.run_coroutine_threadsafe()` for thread-to-async communication
- **Non-blocking**: Scans are performed asynchronously without blocking other operations
- **Respects address offsets**: Takes configured `pcs_address_offset` and `huawei_address_offset` into account

## Files Modified

- [pibems-addon/app/main.py](pibems-addon/app/main.py)
  - Added `scan_registers()` method
  - Added `_get_scanner_html()` method  
  - Updated `__init__()` to initialize loop attribute
  - Updated `run()` to capture event loop
  - Updated API handler to add `/api/scanner` and `/api/scan` endpoints
  - Updated main dashboard to include scanner link

## Example Workflow for PCS50 Issue Resolution

1. Open scanner at `http://<ha-host>:8080/api/scanner`
2. Configure:
   - Device: `PCS`
   - Register Type: `Input`
   - Start: `2000`
   - End: `2400`
3. Click Scan
4. Identify working addresses (e.g., 2100, 2326, 2327, 2328)
5. Update `register_map.yaml` with actual valid addresses
6. Test with heartbeat register discovery in same range
7. Re-enable `enable_pcs_direct: true` in config

## Future Enhancements

Possible improvements:
- Persistent scan history/logging
- Batch register definitions from successful scans
- Auto-update register_map.yaml from scan results
- Performance testing (timing data for each read)
- Scan scheduling (recurring background scans)
