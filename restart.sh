#!/bin/bash
set -euo pipefail

bash ./stop.sh
sleep 1
bash ./run.sh