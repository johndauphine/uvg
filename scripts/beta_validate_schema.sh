#!/usr/bin/env bash
# Run a repeatable beta/RC validation pass against an authorized real schema.
#
# The script writes generated artifacts and per-step logs to an output bundle.
# It does not echo connection URLs, but artifacts can still contain sensitive
# schema metadata such as comments, identifiers, defaults, and constraints.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/beta_validate_schema.sh --label LABEL --source URL [options]

Required:
  --label LABEL              Short bundle name, e.g. customer-a-postgres
  --source URL               Source database URL or sqlite:/// path

Options:
  --out DIR                  Output root (default: /tmp/uvg-beta-validation)
  --target URL               Disposable target URL for diff generation
  --apply                    Apply the live diff to --target, then re-diff
  --migration-target URL     Disposable target URL for versioned migration flow
  --target-dialect DIALECT   postgres, mysql, sqlite, or mssql
  --schemas LIST             Passed through to uvg --schemas
  --tables LIST              Passed through to uvg --tables
  --exclude-tables LIST      Passed through to uvg --exclude-tables
  --options LIST             Passed through to uvg --options
  --trust-cert               Passed through to uvg for MSSQL validation
  --snapshot                 Also write a source schema snapshot YAML
  -h, --help                 Show this help

Environment:
  UVG                        uvg binary path. Defaults to target/release/uvg,
                             then uvg on PATH.
  PYTHON                     Python interpreter used to validate generated
                             code. Defaults to python3, then python on PATH.

Examples:
  cargo build --release
  scripts/beta_validate_schema.sh \
    --label app-pg \
    --source "$SOURCE_URL" \
    --schemas public \
    --target-dialect postgres \
    --snapshot

  scripts/beta_validate_schema.sh \
    --label app-pg-to-empty-pg \
    --source "$SOURCE_URL" \
    --target "$DISPOSABLE_TARGET_URL" \
    --apply \
    --migration-target "$SECOND_DISPOSABLE_TARGET_URL"
USAGE
}

slugify() {
  printf '%s' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9._-]+/-/g; s/^-+//; s/-+$//'
}

timestamp_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

first_nonempty_line() {
  awk '
    NF {
      gsub(/\r/, "")
      sub(/^[[:space:]]+/, "")
      sub(/[[:space:]]+$/, "")
      print
      exit
    }
  '
}

database_engine() {
  case "$1" in
    postgres://*|postgresql://*|postgresql+*://*) printf 'postgresql' ;;
    mysql://*|mysql+*://*|mariadb://*|mariadb+*://*) printf 'mysql' ;;
    sqlite:*|sqlite://*) printf 'sqlite' ;;
    mssql://*|mssql+*://*|sqlserver://*) printf 'mssql' ;;
    *) printf 'unknown' ;;
  esac
}

database_engine_name() {
  case "$1" in
    postgresql) printf 'PostgreSQL' ;;
    mysql) printf 'MySQL' ;;
    sqlite) printf 'SQLite' ;;
    mssql) printf 'Microsoft SQL Server' ;;
    *) printf 'Unknown' ;;
  esac
}

query_version_with_sqlalchemy() {
  local url=$1
  local engine=$2
  local python_code

  python_code=$(cat <<'PY'
import os

from sqlalchemy import create_engine, text

queries = {
    "postgresql": "SHOW server_version",
    "mysql": "SELECT VERSION()",
    "sqlite": "SELECT sqlite_version()",
    "mssql": "SELECT CONVERT(varchar(128), SERVERPROPERTY('ProductVersion'))",
}

engine_name = os.environ["UVG_METADATA_ENGINE"]
db = create_engine(os.environ["UVG_METADATA_DATABASE_URL"])
try:
    with db.connect() as connection:
        value = connection.execute(text(queries[engine_name])).scalar()
        if value is not None:
            print(value)
finally:
    db.dispose()
PY
)

  if command -v timeout >/dev/null 2>&1; then
    UVG_METADATA_DATABASE_URL="$url" UVG_METADATA_ENGINE="$engine" \
      timeout 10s "$PYTHON_BIN" -c "$python_code"
  else
    UVG_METADATA_DATABASE_URL="$url" UVG_METADATA_ENGINE="$engine" \
      "$PYTHON_BIN" -c "$python_code"
  fi
}

database_version() {
  local url=$1
  local engine=$2
  local version=
  local postgres_url

  # SQLite stores no engine version in the database file. Read a local
  # library version without opening (and accidentally creating) the path.
  if [[ "$engine" == "sqlite" ]]; then
    if command -v sqlite3 >/dev/null 2>&1; then
      version=$(sqlite3 --version 2>/dev/null | awk '{print $1}') || version=
    else
      version=$("$PYTHON_BIN" -c 'import sqlite3; print(sqlite3.sqlite_version)' 2>/dev/null) || version=
    fi
  elif [[ $SQLALCHEMY_AVAILABLE -eq 1 && "$engine" != "unknown" ]]; then
    version=$(query_version_with_sqlalchemy "$url" "$engine" 2>/dev/null) || version=
  fi

  if [[ -z "$version" ]]; then
    case "$engine" in
      postgresql)
        if command -v psql >/dev/null 2>&1; then
          postgres_url=$url
          case "$postgres_url" in
            postgresql+*://*) postgres_url="postgresql://${postgres_url#*://}" ;;
          esac
          version=$(PGCONNECT_TIMEOUT=5 psql "$postgres_url" -X -w -A -t -q \
            -c 'SHOW server_version' 2>/dev/null) || version=
        fi
        ;;
    esac
  fi

  version=$(printf '%s\n' "$version" | first_nonempty_line)
  printf '%s' "${version:-unavailable}"
}

database_description() {
  local url=$1
  local engine version

  if [[ -z "$url" ]]; then
    printf 'not provided'
    return
  fi

  engine=$(database_engine "$url")
  version=$(database_version "$url" "$engine")
  printf '%s (version %s)' "$(database_engine_name "$engine")" "$version"
}

LABEL=
SOURCE_URL=
TARGET_URL=
MIGRATION_TARGET_URL=
OUT_ROOT=${UVG_VALIDATION_OUT:-/tmp/uvg-beta-validation}
TARGET_DIALECT=
SCHEMAS=
TABLES=
EXCLUDE_TABLES=
OPTIONS=
TRUST_CERT=0
APPLY=0
SNAPSHOT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label)
      LABEL=${2:-}
      shift 2
      ;;
    --source)
      SOURCE_URL=${2:-}
      shift 2
      ;;
    --out)
      OUT_ROOT=${2:-}
      shift 2
      ;;
    --target)
      TARGET_URL=${2:-}
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --migration-target)
      MIGRATION_TARGET_URL=${2:-}
      shift 2
      ;;
    --target-dialect)
      TARGET_DIALECT=${2:-}
      shift 2
      ;;
    --schemas)
      SCHEMAS=${2:-}
      shift 2
      ;;
    --tables)
      TABLES=${2:-}
      shift 2
      ;;
    --exclude-tables)
      EXCLUDE_TABLES=${2:-}
      shift 2
      ;;
    --options)
      OPTIONS=${2:-}
      shift 2
      ;;
    --trust-cert)
      TRUST_CERT=1
      shift
      ;;
    --snapshot)
      SNAPSHOT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1' (try --help)" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$LABEL" || -z "$SOURCE_URL" ]]; then
  usage >&2
  exit 2
fi
if [[ $APPLY -eq 1 && -z "$TARGET_URL" ]]; then
  echo "error: --apply requires --target" >&2
  exit 2
fi
if [[ $APPLY -eq 1 && -n "$MIGRATION_TARGET_URL" && "$TARGET_URL" == "$MIGRATION_TARGET_URL" ]]; then
  echo "error: --apply and --migration-target must use separate disposable targets" >&2
  exit 2
fi

UVG=${UVG:-}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [[ -z "$UVG" ]]; then
  if [[ -x "$REPO_ROOT/target/release/uvg" ]]; then
    UVG="$REPO_ROOT/target/release/uvg"
  elif command -v uvg >/dev/null 2>&1; then
    UVG="$(command -v uvg)"
  else
    echo "error: uvg binary not found. Build with 'cargo build --release' or set UVG." >&2
    exit 1
  fi
fi

PYTHON_BIN=${PYTHON:-}
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=$(command -v python3)
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN=$(command -v python)
  else
    echo "error: Python not found; it is required to syntax-check generated models" >&2
    exit 1
  fi
elif ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "error: PYTHON does not name an executable: $PYTHON_BIN" >&2
  exit 1
fi

SQLALCHEMY_AVAILABLE=0
if "$PYTHON_BIN" -c 'import sqlalchemy' >/dev/null 2>&1; then
  SQLALCHEMY_AVAILABLE=1
fi

UVG_VERSION_OUTPUT=unavailable
if version_output=$("$UVG" --version 2>/dev/null); then
  UVG_VERSION_OUTPUT=$(printf '%s\n' "$version_output" | first_nonempty_line)
  UVG_VERSION_OUTPUT=${UVG_VERSION_OUTPUT:-unavailable}
fi

GIT_SHA=unavailable
GIT_WORKTREE=unavailable
if git_sha=$(git -C "$REPO_ROOT" rev-parse --verify HEAD 2>/dev/null); then
  GIT_SHA=$git_sha
  if [[ -z "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null)" ]]; then
    GIT_WORKTREE=clean
  else
    GIT_WORKTREE=dirty
  fi
fi

SLUG=$(slugify "$LABEL")
if [[ -z "$SLUG" ]]; then
  echo "error: --label must contain at least one alphanumeric, dot, underscore, or dash" >&2
  exit 2
fi

SOURCE_DATABASE=$(database_description "$SOURCE_URL")
TARGET_DATABASE=$(database_description "$TARGET_URL")
MIGRATION_TARGET_DATABASE=$(database_description "$MIGRATION_TARGET_URL")

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
OUT_DIR="$OUT_ROOT/${RUN_ID}_${SLUG}"
mkdir -p "$OUT_DIR/logs"

MANIFEST="$OUT_DIR/manifest.tsv"
SUMMARY="$OUT_DIR/summary.md"
printf 'step\tstatus\tseconds\tartifact\tlog\n' > "$MANIFEST"

cat > "$SUMMARY" <<EOF
# UVG Beta Validation: $LABEL

- Started: $(timestamp_utc)
- Output bundle: $OUT_DIR
- Git commit: $GIT_SHA
- Git worktree: $GIT_WORKTREE
- UVg version: $UVG_VERSION_OUTPUT
- Source URL: not recorded by this script
- Source database: $SOURCE_DATABASE
- Target URL: $([[ -n "$TARGET_URL" ]] && printf 'provided' || printf 'not provided')
- Target database: $TARGET_DATABASE
- Migration target URL: $([[ -n "$MIGRATION_TARGET_URL" ]] && printf 'provided' || printf 'not provided')
- Migration target database: $MIGRATION_TARGET_DATABASE
- Target dialect: ${TARGET_DIALECT:-inferred}
- Schemas: ${SCHEMAS:-default}
- Tables: ${TABLES:-all}
- Excluded tables: ${EXCLUDE_TABLES:-none}
- Generator options: ${OPTIONS:-default}

Generated artifacts may contain sensitive schema metadata. Keep this bundle
private unless the source schema has been cleared for disclosure.

## Steps

EOF

COMMON_ARGS=()
[[ $TRUST_CERT -eq 1 ]] && COMMON_ARGS+=(--trust-cert)
[[ -n "$SCHEMAS" ]] && COMMON_ARGS+=(--schemas "$SCHEMAS")
[[ -n "$TABLES" ]] && COMMON_ARGS+=(--tables "$TABLES")
[[ -n "$EXCLUDE_TABLES" ]] && COMMON_ARGS+=(--exclude-tables "$EXCLUDE_TABLES")
[[ -n "$OPTIONS" ]] && COMMON_ARGS+=(--options "$OPTIONS")

TARGET_DIALECT_ARGS=()
[[ -n "$TARGET_DIALECT" ]] && TARGET_DIALECT_ARGS+=(--target-dialect "$TARGET_DIALECT")

POST_MIGRATION_ARGS=()
[[ $TRUST_CERT -eq 1 ]] && POST_MIGRATION_ARGS+=(--trust-cert)
[[ -n "$SCHEMAS" ]] && POST_MIGRATION_ARGS+=(--schemas "$SCHEMAS")
[[ -n "$TABLES" ]] && POST_MIGRATION_ARGS+=(--tables "$TABLES")
if [[ -n "$EXCLUDE_TABLES" ]]; then
  POST_MIGRATION_ARGS+=(--exclude-tables "${EXCLUDE_TABLES},uvg_version")
else
  POST_MIGRATION_ARGS+=(--exclude-tables "uvg_version")
fi
[[ -n "$OPTIONS" ]] && POST_MIGRATION_ARGS+=(--options "$OPTIONS")

fail_count=0
skip_count=0

run_step() {
  local step=$1
  local artifact=$2
  shift 2
  local log="$OUT_DIR/logs/${step}.log"
  local start end seconds

  start=$(date +%s)
  {
    echo "step=$step"
    echo "started_at=$(timestamp_utc)"
    echo "artifact=$artifact"
  } > "$log"

  if "$@" >> "$log" 2>&1; then
    end=$(date +%s)
    seconds=$((end - start))
    printf '%s\tOK\t%s\t%s\t%s\n' "$step" "$seconds" "$artifact" "$log" >> "$MANIFEST"
    printf -- '- `%s`: OK in %ss\n' "$step" "$seconds" >> "$SUMMARY"
  else
    end=$(date +%s)
    seconds=$((end - start))
    printf '%s\tFAIL\t%s\t%s\t%s\n' "$step" "$seconds" "$artifact" "$log" >> "$MANIFEST"
    printf -- '- `%s`: FAIL in %ss, see `%s`\n' "$step" "$seconds" "$log" >> "$SUMMARY"
    fail_count=$((fail_count + 1))
  fi
}

skip_step() {
  local step=$1
  local artifact=$2
  local reason=$3
  local log="$OUT_DIR/logs/${step}.log"

  {
    echo "step=$step"
    echo "started_at=$(timestamp_utc)"
    echo "artifact=$artifact"
    echo "skipped=$reason"
  } > "$log"
  printf '%s\tSKIP\t0\t%s\t%s\n' "$step" "$artifact" "$log" >> "$MANIFEST"
  printf -- '- `%s`: SKIP (%s)\n' "$step" "$reason" >> "$SUMMARY"
  skip_count=$((skip_count + 1))
}

check_no_schema_changes() {
  local step=$1
  local artifact=$2
  local log="$OUT_DIR/logs/${step}.log"
  local start end seconds content

  start=$(date +%s)
  {
    echo "step=$step"
    echo "started_at=$(timestamp_utc)"
    echo "artifact=$artifact"
  } > "$log"

  content=
  [[ -f "$artifact" ]] && content=$(cat "$artifact")
  end=$(date +%s)
  seconds=$((end - start))

  if [[ "$content" == "-- No schema changes detected." ]]; then
    printf '%s\tOK\t%s\t%s\t%s\n' "$step" "$seconds" "$artifact" "$log" >> "$MANIFEST"
    printf -- '- `%s`: OK in %ss\n' "$step" "$seconds" >> "$SUMMARY"
  else
    {
      echo "expected: -- No schema changes detected."
      echo "actual:"
      if [[ -f "$artifact" ]]; then
        sed -n '1,80p' "$artifact"
      else
        echo "missing artifact"
      fi
    } >> "$log"
    printf '%s\tFAIL\t%s\t%s\t%s\n' "$step" "$seconds" "$artifact" "$log" >> "$MANIFEST"
    printf -- '- `%s`: FAIL in %ss, see `%s`\n' "$step" "$seconds" "$log" >> "$SUMMARY"
    fail_count=$((fail_count + 1))
  fi
}

run_step declarative "$OUT_DIR/declarative.py" \
  "$UVG" "${COMMON_ARGS[@]}" "$SOURCE_URL" --generator declarative --outfile "$OUT_DIR/declarative.py"

run_step tables "$OUT_DIR/tables.py" \
  "$UVG" "${COMMON_ARGS[@]}" "$SOURCE_URL" --generator tables --outfile "$OUT_DIR/tables.py"

run_step declarative-syntax "$OUT_DIR/declarative.py" \
  "$PYTHON_BIN" -c \
  'from pathlib import Path; import sys; path = Path(sys.argv[1]); compile(path.read_bytes(), str(path), "exec")' \
  "$OUT_DIR/declarative.py"

run_step tables-syntax "$OUT_DIR/tables.py" \
  "$PYTHON_BIN" -c \
  'from pathlib import Path; import sys; path = Path(sys.argv[1]); compile(path.read_bytes(), str(path), "exec")' \
  "$OUT_DIR/tables.py"

if [[ $SQLALCHEMY_AVAILABLE -eq 1 ]]; then
  run_step declarative-import "$OUT_DIR/declarative.py" \
    "$PYTHON_BIN" -c \
    'import runpy, sys; runpy.run_path(sys.argv[1], run_name="__uvg_validation__"); from sqlalchemy.orm import configure_mappers; configure_mappers()' \
    "$OUT_DIR/declarative.py"

  run_step tables-import "$OUT_DIR/tables.py" \
    "$PYTHON_BIN" -c \
    'import runpy, sys; runpy.run_path(sys.argv[1], run_name="__uvg_validation__")' \
    "$OUT_DIR/tables.py"
else
  skip_step declarative-import "$OUT_DIR/declarative.py" \
    "SQLAlchemy is not installed for $PYTHON_BIN"
  skip_step tables-import "$OUT_DIR/tables.py" \
    "SQLAlchemy is not installed for $PYTHON_BIN"
fi

run_step source-ddl "$OUT_DIR/source.sql" \
  "$UVG" "${COMMON_ARGS[@]}" "$SOURCE_URL" --generator ddl "${TARGET_DIALECT_ARGS[@]}" --outfile "$OUT_DIR/source.sql"

if [[ $SNAPSHOT -eq 1 ]]; then
  run_step snapshot "$OUT_DIR/source.snapshot.yaml" \
    "$UVG" "${COMMON_ARGS[@]}" snapshot "$SOURCE_URL" --output "$OUT_DIR/source.snapshot.yaml"
fi

if [[ -n "$TARGET_URL" ]]; then
  run_step diff "$OUT_DIR/diff.sql" \
    "$UVG" "${COMMON_ARGS[@]}" "$SOURCE_URL" "$TARGET_URL" --generator ddl "${TARGET_DIALECT_ARGS[@]}" --outfile "$OUT_DIR/diff.sql"

  if [[ $APPLY -eq 1 ]]; then
    run_step direct-apply "$OUT_DIR/direct_apply.sql" \
      "$UVG" "${COMMON_ARGS[@]}" "$SOURCE_URL" "$TARGET_URL" --generator ddl "${TARGET_DIALECT_ARGS[@]}" --outfile "$OUT_DIR/direct_apply.sql" --apply --progress off

    run_step post-apply-diff "$OUT_DIR/post_apply_diff.sql" \
      "$UVG" "${COMMON_ARGS[@]}" "$SOURCE_URL" "$TARGET_URL" --generator ddl "${TARGET_DIALECT_ARGS[@]}" --outfile "$OUT_DIR/post_apply_diff.sql"

    check_no_schema_changes post-apply-convergence "$OUT_DIR/post_apply_diff.sql"
  fi
fi

if [[ -n "$MIGRATION_TARGET_URL" ]]; then
  MIGRATIONS_DIR="$OUT_DIR/migrations"
  run_step migration-init "$OUT_DIR/uvg.toml" \
    "$UVG" init --migrations-dir "$MIGRATIONS_DIR" --config "$OUT_DIR/uvg.toml"

  run_step migration-revision "$MIGRATIONS_DIR" \
    "$UVG" "${COMMON_ARGS[@]}" revision "$SOURCE_URL" "$MIGRATION_TARGET_URL" --message "beta validation $LABEL" --migrations-dir "$MIGRATIONS_DIR"

  run_step migration-upgrade "$MIGRATIONS_DIR" \
    "$UVG" "${COMMON_ARGS[@]}" upgrade "$MIGRATION_TARGET_URL" --migrations-dir "$MIGRATIONS_DIR"

  run_step migration-current "$OUT_DIR/migration_current.txt" \
    "$UVG" "${COMMON_ARGS[@]}" current "$MIGRATION_TARGET_URL"

  run_step post-migration-diff "$OUT_DIR/post_migration_diff.sql" \
    "$UVG" "${POST_MIGRATION_ARGS[@]}" "$SOURCE_URL" "$MIGRATION_TARGET_URL" --generator ddl "${TARGET_DIALECT_ARGS[@]}" --outfile "$OUT_DIR/post_migration_diff.sql"

  check_no_schema_changes post-migration-convergence "$OUT_DIR/post_migration_diff.sql"
fi

cat >> "$SUMMARY" <<EOF

## Result

- Finished: $(timestamp_utc)
- Failed steps: $fail_count
- Skipped steps: $skip_count
- Manifest: $MANIFEST
EOF

echo "Validation bundle: $OUT_DIR"
echo "Manifest: $MANIFEST"
echo "Summary: $SUMMARY"

if [[ $fail_count -ne 0 ]]; then
  exit 1
fi
