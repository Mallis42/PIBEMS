# PIBEMS Modbus Scanner - Quick Start Guide

## What is it?
A web-based tool to scan your PCS50 or Huawei inverter's Modbus registers and identify which addresses are valid. This helps diagnose address mismatch issues like the "IllegalAddress" errors you were seeing.

## Quick Access
**URL**: `http://<your-home-assistant>:8080/api/scanner`

Or click "Modbus Scanner" link on the main PIBEMS Dashboard.

## For Your PCS50 Issue

### The Problem
Your PCS50 doesn't respond to register address 2219, causing:
```
PCS direct heartbeat probe failed: Modbus read error at 2219: Exception Response(132, 4, IllegalAddress)
```

### The Solution
Use the scanner to find valid input registers on your PCS50:

1. **Open Scanner**
   - Go to `http://<ha-host>:8080/api/scanner`

2. **Scan for Input Registers**
   - Device: **PCS**
   - Register Type: **Input**
   - Start Address: **2000**
   - End Address: **2500**
   - Click **Scan**

3. **Wait for Results**
   - Green addresses = Valid (readable)
   - Red addresses = Invalid (device doesn't support them)

4. **Update Configuration**
   - Note down the working addresses
   - Edit `pibems-addon/config/register_map.yaml`
   - Replace invalid addresses with working ones
   - Restart the addon

### Example Result
If scan shows addresses 2100, 2105, 2107 are valid, update register_map.yaml:
```yaml
pcs:
  status:
    alarm_word_1:
      address: 2100      # ✓ Valid
    pcs_status_word:
      address: 2105      # ✓ Valid
    monitor_alarm_word:
      address: 2107      # ✓ Valid
    heartbeat:
      address: 2999      # ✗ Not valid - find/skip this one
```

## Tips

### Scanning Ranges
- **PCS Input Registers**: Try 2000-2500
- **Huawei Input Registers**: Try 32000-32500
- **Holding Registers**: Try appropriate ranges for your device

### Understanding Results
| Color | Status | Meaning |
|-------|--------|---------|
| 🟢 Green | Success | Address is valid, value shown |
| 🔴 Red | Failed | Address doesn't exist or isn't readable |

### If Most Addresses Fail
- Check device is powered on and connected
- Verify IP address and port (usually 192.168.x.x:502)
- Try different address ranges
- Check if you need a different address offset

## API for Advanced Users

### Manual Scan (cURL)
```bash
curl -X POST http://localhost:8080/api/scan \
  -H "Content-Type: application/json" \
  -d '{
    "device": "pcs",
    "start_addr": 2100,
    "end_addr": 2200,
    "reg_type": "input"
  }'
```

### Response Example
```json
{
  "device": "pcs",
  "reg_type": "input",
  "start_addr": 2100,
  "end_addr": 2105,
  "address_offset": 0,
  "results": {
    "2100": {"success": true, "value": 123},
    "2101": {"success": false, "error": "IllegalAddress"},
    "2102": {"success": true, "value": 456}
  }
}
```

## Common Issues

**Q: Scan takes a long time**
- Normal for large ranges (each address tested individually)
- Max range is 1000 addresses
- Try smaller ranges (50-100 at a time)

**Q: All addresses fail**
- Check device connectivity (can you ping the IP?)
- Verify port (default Modbus TCP is 502)
- Try different address range (device may have non-contiguous registers)

**Q: "Event loop not initialized" error**
- Addon may not be fully started
- Wait a moment and try again

## Once You Find Valid Addresses

1. Update `register_map.yaml` with discovered addresses
2. Set `enable_pcs_direct: true` in `config.yaml`
3. Restart addon
4. Check logs - heartbeat probe should succeed

---

**Need help?** Check the full documentation at [MODBUS_SCANNER_IMPLEMENTATION.md](../MODBUS_SCANNER_IMPLEMENTATION.md)
