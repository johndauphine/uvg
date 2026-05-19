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
SQLCMD_IN_CONTAINER="/opt/mssql-tools18/bin/sqlcmd"

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
