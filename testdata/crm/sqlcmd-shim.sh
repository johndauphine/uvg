#!/usr/bin/env bash
# Shim that proxies `sqlcmd` invocations into the mssql-test container
# where the real sqlcmd binary lives. Lets `run_matrix.sh` (which needs
# host-side `sqlcmd` for MSSQL target operations) work without
# installing Microsoft's go-sqlcmd separately — useful for both CI
# runners and dev machines where adding a system-wide tool is friction.
#
# Install:
#   cp testdata/crm/sqlcmd-shim.sh ~/.local/bin/sqlcmd
#   chmod +x ~/.local/bin/sqlcmd
# Or in CI:
#   sudo install -m 0755 testdata/crm/sqlcmd-shim.sh /usr/local/bin/sqlcmd
#
# Translates `-i <hostpath>` into stdin (since the host path isn't
# visible inside the container); every other flag passes through.
set -euo pipefail

CONTAINER="${MSSQL_CONTAINER:-mssql-test}"

# The sqlcmd binary moved between MSSQL image revisions:
#   /opt/mssql-tools18/bin/sqlcmd   — newer 2022 cumulative updates
#   /opt/mssql-tools/bin/sqlcmd     — older 2022 tags + 2019
# Probe both so the shim survives an image-tag bump in either direction.
SQLCMD_IN_CONTAINER=""
for p in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
  if docker exec "$CONTAINER" test -x "$p" 2>/dev/null; then
    SQLCMD_IN_CONTAINER="$p"
    break
  fi
done
if [[ -z "$SQLCMD_IN_CONTAINER" ]]; then
  echo "error: sqlcmd not found inside container '$CONTAINER' at either" \
    "/opt/mssql-tools18/bin/sqlcmd or /opt/mssql-tools/bin/sqlcmd" >&2
  exit 1
fi

args=()
input_file=""
i=0
argv=("$@")
while [[ $i -lt ${#argv[@]} ]]; do
  a="${argv[$i]}"
  if [[ "$a" == "-i" && $((i + 1)) -lt ${#argv[@]} ]]; then
    input_file="${argv[$((i + 1))]}"
    i=$((i + 2))
    continue
  fi
  args+=("$a")
  i=$((i + 1))
done

if [[ -n "$input_file" ]]; then
  exec docker exec -i "$CONTAINER" "$SQLCMD_IN_CONTAINER" "${args[@]}" < "$input_file"
else
  exec docker exec -i "$CONTAINER" "$SQLCMD_IN_CONTAINER" "${args[@]}"
fi
