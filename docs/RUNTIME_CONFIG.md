# Runtime Configuration

Black Swan's coordinator has a local-safe default configuration and supports environment-variable overrides.

## Defaults

| Setting | Default |
| --- | --- |
| Node ID | `node_a` |
| Listen address | `127.0.0.1:9199` |
| Max concurrent frames | `512` |
| Clock skew tolerance | `30` seconds |
| Current term | `1` |

## Environment Variables

| Variable | Meaning |
| --- | --- |
| `BLACK_SWAN_NODE_ID` | Node identifier used for storage namespacing |
| `BLACK_SWAN_LISTEN_ADDR` | TCP listen address, for example `127.0.0.1:9199` |
| `BLACK_SWAN_MAX_CONCURRENT_FRAMES` | Maximum concurrent packet-processing tasks |
| `BLACK_SWAN_CLOCK_SKEW_SECS` | Allowed timestamp drift in seconds |
| `BLACK_SWAN_CURRENT_TERM` | Runtime term used for WAL entries until consensus owns term changes |

## PowerShell Example

```powershell
$env:BLACK_SWAN_NODE_ID = "node_dev_01"
$env:BLACK_SWAN_LISTEN_ADDR = "127.0.0.1:9199"
$env:BLACK_SWAN_MAX_CONCURRENT_FRAMES = "512"
$env:BLACK_SWAN_CLOCK_SKEW_SECS = "30"
$env:BLACK_SWAN_CURRENT_TERM = "1"

cargo run -p black_swan_coordinator
```

## Current Limitation

`BLACK_SWAN_CURRENT_TERM` is still a runtime setting.

Future consensus work should make the consensus controller the source of truth for term changes.