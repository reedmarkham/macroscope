#!/usr/bin/env bash
set -euo pipefail

exec micromamba run -n dvid-cropper "$@"
