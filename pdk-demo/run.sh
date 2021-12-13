#!/usr/bin/env bash
# This script sets up the demo for tremor with PDK configured.

export TREMOR_PLUGIN_PATH="etc/tremor/plugins"
export TREMOR_PATH="etc/tremor/config:$TREMOR_PATH"
export RUST_LOG=debug

if [ $# -ne 1 ]; then
    echo "Usage: $0 CONFIG_FILE"
    exit 1
fi

../target/debug/tremor server run -f "$1"
