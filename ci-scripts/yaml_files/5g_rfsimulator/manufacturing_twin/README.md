# Manufacturing Twin

Telemetry-only Ender-3 machine twin for the OAI 5G N6 enforcement framework.
This package reads OctoPrint state and writes append-only machine-twin outputs
for later consumption by the Digital Twin and policy manager.

## Safety

This integration only performs read-only OctoPrint REST calls:

- `GET /api/version`
- `GET /api/printer`
- `GET /api/job`

It does not start, pause, stop, cancel, jog, heat, upload, or otherwise control
the printer.

## Configuration

Set OctoPrint access through environment variables. Do not hardcode API keys.
When the OAI laptop is on the same Wi-Fi/network as the Raspberry Pi, use the
Pi's OctoPrint client URL directly.

```bash
export OCTOPRINT_URL="http://10.88.188.190:5000"
export OCTOPRINT_API_KEY="your-octoprint-api-key"
```

The default machine metadata and output paths are in `config.yaml`.

Outputs are written under:

- `logs/manufacturing_twin/machine_twin_state.jsonl`
- `logs/manufacturing_twin/latest_machine_twin_state.json`
- `logs/manufacturing_twin/machine_twin_metrics.csv`

## Run

From `ci-scripts/yaml_files/5g_rfsimulator`:

```bash
manufacturing_twin/run_manufacturing_twin.sh
```

This is real mode. It reads OctoPrint with `GET /api/version`,
`GET /api/printer`, and `GET /api/job` and writes the machine twin state.
The wrapper rejects placeholder URL/API-key values and never prints the API key.

Read one sample, write outputs, and print JSON:

```bash
manufacturing_twin/run_manufacturing_twin.sh --once
```

Override the polling interval:

```bash
manufacturing_twin/run_manufacturing_twin.sh --interval 5
```

Write to a different output directory:

```bash
manufacturing_twin/run_manufacturing_twin.sh --once --output-dir /tmp/manufacturing_twin
```

## Mock Mode

Use mock mode to test backend behavior without contacting OctoPrint:

```bash
python3 manufacturing_twin/manufacturing_twin_sync.py --once --mock-file /path/to/mock_octoprint.json --output-dir /tmp/manufacturing_twin
```

The wrapper also supports mock mode:

```bash
MOCK_MACHINE_TWIN_FILE=/path/to/mock_octoprint.json manufacturing_twin/run_manufacturing_twin.sh --once
```

Example offline-printer mock:

```json
{
  "octoprint_reachable": true,
  "printer_operational": false,
  "availability": "printer_offline",
  "printer_state_text": "Offline",
  "job_state": "Offline",
  "api_error": "Printer is not operational",
  "version": {"server": "1.x"},
  "printer": {"error": "Printer is not operational"},
  "job": {"state": "Offline"}
}
```

This should produce `manufacturing_phase: printer_offline`.

## Offline Printer Handling

OctoPrint can be reachable while the printer is disconnected or offline. The
twin records these as separate fields:

- `octoprint_reachable`
- `printer_operational`
- `printer_state_text`
- `job_state`
- `api_error`

If `/api/version` works but `/api/printer` returns `403` or
`{"error": "Printer is not operational"}`, the backend does not crash. It emits
a valid twin state with `octoprint_reachable: true`,
`printer_operational: false`, and `manufacturing_phase: printer_offline`.

## Health Check

Check that the latest machine twin state exists, has a timestamp, confirms
OctoPrint reachability, and has a manufacturing phase:

```bash
python3 manufacturing_twin/check_manufacturing_twin.py
```

Offline printers are allowed by default so backend tests can pass without an
operational printer. Require an operational printer explicitly:

```bash
python3 manufacturing_twin/check_manufacturing_twin.py --require-printer-operational
```

Override the latest-state path or add an optional maximum age:

```bash
python3 manufacturing_twin/check_manufacturing_twin.py --latest-path /tmp/manufacturing_twin/latest_machine_twin_state.json --max-age-seconds 30
```

## Test

Syntax check:

```bash
python3 -m py_compile manufacturing_twin/*.py
```

Unavailable-state smoke test without contacting a printer:

```bash
unset OCTOPRINT_URL OCTOPRINT_API_KEY
python3 manufacturing_twin/manufacturing_twin_sync.py --once --output-dir /tmp/manufacturing_twin
```

## Requirements

- `requests`
- `pyyaml` if using full YAML config parsing. The default config also works
  with the built-in minimal parser.
