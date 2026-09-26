#!/usr/bin/env bash
# Long-running manual soak. Prints progress lines every 10s so RSS drift is visible.
# Example: DURATION=3600 CONNECTIONS=100 PAYLOAD=512 bash tests/interop/soak.sh
set -u
server_bin=${SERVER_BIN:-./zig-out/bin/raknet-interop}
client_bin=${CLIENT_BIN:-./zig-out/bin/raknet-interop}
duration=${DURATION:-600}
connections=${CONNECTIONS:-50}
payload=${PAYLOAD:-512}
port=${PORT:-19700}

"$server_bin" server "127.0.0.1:$port" $((duration + 15)) &
sleep 1
"$client_bin" client "127.0.0.1:$port" "$connections" "$payload" "$duration" 1000
wait
