#!/usr/bin/env bash
# Offline render of generated kind artifacts (Kafka topics from AsyncAPI).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd ruby
ruby "${SCRIPT_DIR}/render-kafka-topics.rb"
