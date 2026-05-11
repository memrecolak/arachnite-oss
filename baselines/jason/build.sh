#!/usr/bin/env bash
# Compile the Jason baseline into baselines/jason/build/
# Requires:
#   - JDK 11+ (javac on PATH)
#   - Jason 3.x installed; either JASON_HOME set OR jason.jar already on
#     CLASSPATH. JASON_HOME is the directory containing libs/jason.jar
#     (the layout produced by the official jason-bin-3.x.zip release).
set -euo pipefail

cd "$(dirname "$0")"

if [[ -z "${JASON_HOME:-}" ]]; then
    echo "JASON_HOME is not set." >&2
    echo "Download Jason 3.x from https://github.com/jason-lang/jason/releases" >&2
    echo "Unzip and export JASON_HOME=/path/to/jason-3.x" >&2
    exit 1
fi

JASON_JAR="${JASON_HOME}/libs/jason.jar"
if [[ ! -f "${JASON_JAR}" ]]; then
    echo "Cannot find ${JASON_JAR}" >&2
    echo "JASON_HOME must point to the unpacked jason-bin distribution." >&2
    exit 1
fi

mkdir -p build
CP="${JASON_HOME}/libs/*"

javac -cp "${CP}" -d build ArmState.java RobotArmEnv.java JasonBench.java

echo "Built. Run via:"
echo "  java -cp \"build:${CP}\" JasonBench --out latencies.jsonl --cycles 10000 --warmup 1000"
