#!/usr/bin/env bash
# StackOverflow2010 schema-drift matrix harness.
#
# The restored StackOverflow2010 SQL Server database is the pristine seed. For
# each target dialect, this runner recreates a disposable SQL Server source
# clone, bootstraps a disposable target, applies staged SQL Server drift packs to
# the source clone, and requires the target to converge after each supported
# pack. Direct catalog checks make sure the expected drift actually landed.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  testdata/stackoverflow-drift/run_drift.sh [options]

Options:
  --targets LIST          Comma-separated targets: mssql,postgres,mysql,sqlite
                          (default: mssql,postgres,mysql,sqlite)
  --source-db NAME        Existing pristine SQL Server database (default: StackOverflow2010)
  --work-source-db NAME   Disposable SQL Server source clone (default: UVG_SO2010_Drift_Source)
  --mssql-target-db NAME  Disposable SQL Server target (default: UVG_SO2010_Drift_Target)
  --pg-target-db NAME     Disposable PostgreSQL target (default: uvg_so2010_drift_pg)
  --mysql-target-db NAME  Disposable MySQL target (default: uvg_so2010_drift_mysql)
  --sqlite-target PATH    Disposable SQLite target file (default: bundle-local file)
  --out DIR               Output root (default: /tmp/uvg-stackoverflow-drift)
  -h, --help              Show this help

Environment:
  UVG                     uvg binary path. Defaults to target/release/uvg, then PATH.
  MSSQL_HOST              SQL Server host (default: localhost)
  MSSQL_PORT              SQL Server port (default: 1433)
  MSSQL_USER              SQL Server user (default: sa)
  MSSQL_PASSWORD          SQL Server password (default: TestPass2024)
  MSSQL_CONTAINER         Container used by sqlcmd-shim.sh (default: mssql-test)
  PG_HOST                 PostgreSQL host for UVG URL (default: localhost)
  PG_PORT                 PostgreSQL port for UVG URL (default: 5432)
  PG_USER                 PostgreSQL user (default: postgres)
  PG_PASSWORD             PostgreSQL password (default: TestPass2024)
  PG_CONTAINER            PostgreSQL container for catalog checks (default: pg-test)
  MYSQL_HOST              MySQL host for UVG URL (default: localhost)
  MYSQL_PORT              MySQL port for UVG URL (default: 3306)
  MYSQL_USER              MySQL user (default: root)
  MYSQL_PASSWORD          MySQL password (default: TestPass2024)
  MYSQL_CONTAINER         MySQL container for catalog checks (default: mysql-test)

The script recreates the disposable source clone and every selected target. Do
not point any target option at a database or file that is not safe to drop.
USAGE
}

safe_db_name() {
  [[ "$1" =~ ^[A-Za-z0-9_]+$ ]]
}

timestamp_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

slug_step() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/-/g'
}

split_targets() {
  local raw=$1
  local item
  IFS=',' read -r -a TARGETS <<< "$raw"
  for item in "${TARGETS[@]}"; do
    case "$item" in
      mssql|postgres|mysql|sqlite) ;;
      *)
        echo "error: unsupported target '$item' in --targets" >&2
        exit 2
        ;;
    esac
  done
}

targets_include() {
  local needle=$1
  local item
  for item in "${TARGETS[@]}"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

SOURCE_DB=StackOverflow2010
WORK_SOURCE_DB=UVG_SO2010_Drift_Source
MSSQL_TARGET_DB=UVG_SO2010_Drift_Target
PG_TARGET_DB=uvg_so2010_drift_pg
MYSQL_TARGET_DB=uvg_so2010_drift_mysql
SQLITE_TARGET_PATH=
TARGETS_RAW=mssql,postgres,mysql,sqlite
OUT_ROOT=${UVG_STACKOVERFLOW_DRIFT_OUT:-/tmp/uvg-stackoverflow-drift}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets)
      TARGETS_RAW=${2:-}
      shift 2
      ;;
    --source-db)
      SOURCE_DB=${2:-}
      shift 2
      ;;
    --work-source-db)
      WORK_SOURCE_DB=${2:-}
      shift 2
      ;;
    --mssql-target-db)
      MSSQL_TARGET_DB=${2:-}
      shift 2
      ;;
    --pg-target-db)
      PG_TARGET_DB=${2:-}
      shift 2
      ;;
    --mysql-target-db)
      MYSQL_TARGET_DB=${2:-}
      shift 2
      ;;
    --sqlite-target)
      SQLITE_TARGET_PATH=${2:-}
      shift 2
      ;;
    --out)
      OUT_ROOT=${2:-}
      shift 2
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

split_targets "$TARGETS_RAW"
for db in "$SOURCE_DB" "$WORK_SOURCE_DB" "$MSSQL_TARGET_DB" "$PG_TARGET_DB" "$MYSQL_TARGET_DB"; do
  if ! safe_db_name "$db"; then
    echo "error: database name '$db' must contain only letters, numbers, and underscores" >&2
    exit 2
  fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PACK_DIR="$SCRIPT_DIR/mssql"

UVG=${UVG:-}
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

if ! command -v sqlcmd >/dev/null 2>&1; then
  echo "error: sqlcmd not found. Install it or use testdata/crm/sqlcmd-shim.sh." >&2
  exit 1
fi
if { targets_include postgres || targets_include mysql; } && ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required for PostgreSQL/MySQL catalog checks." >&2
  exit 1
fi
if targets_include sqlite && ! command -v sqlite3 >/dev/null 2>&1; then
  echo "error: sqlite3 is required for SQLite catalog checks." >&2
  exit 1
fi

MSSQL_HOST=${MSSQL_HOST:-localhost}
MSSQL_PORT=${MSSQL_PORT:-1433}
MSSQL_USER=${MSSQL_USER:-sa}
MSSQL_PASSWORD=${MSSQL_PASSWORD:-TestPass2024}
export MSSQL_CONTAINER=${MSSQL_CONTAINER:-mssql-test}

PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_USER=${PG_USER:-postgres}
PG_PASSWORD=${PG_PASSWORD:-TestPass2024}
PG_CONTAINER=${PG_CONTAINER:-pg-test}

MYSQL_HOST=${MYSQL_HOST:-localhost}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-TestPass2024}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-mysql-test}

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
OUT_DIR="$OUT_ROOT/$RUN_ID"
LOG_DIR="$OUT_DIR/logs"
mkdir -p "$LOG_DIR"
if [[ -z "$SQLITE_TARGET_PATH" ]]; then
  SQLITE_TARGET_PATH="$OUT_DIR/sqlite/uvg_so2010_drift.sqlite"
fi

RESULTS="$OUT_DIR/results.tsv"
SUMMARY="$OUT_DIR/summary.md"
printf 'target\tstep\tstatus\tseconds\tartifact\tlog\n' > "$RESULTS"

SOURCE_URL="mssql://${MSSQL_USER}:${MSSQL_PASSWORD}@${MSSQL_HOST}:${MSSQL_PORT}/${SOURCE_DB}"
WORK_SOURCE_URL="mssql://${MSSQL_USER}:${MSSQL_PASSWORD}@${MSSQL_HOST}:${MSSQL_PORT}/${WORK_SOURCE_DB}"

cat > "$SUMMARY" <<EOF
# StackOverflow2010 Drift Matrix

- Started: $(timestamp_utc)
- Output bundle: $OUT_DIR
- Targets: ${TARGETS[*]}
- Pristine source DB: $SOURCE_DB
- Disposable SQL Server source clone: $WORK_SOURCE_DB
- UVG: $UVG

Connection URLs are not recorded here. Artifacts contain schema metadata and
should be treated as validation output, not public release artifacts.

## Steps

EOF

run_step() {
  local target=$1
  local step=$2
  local artifact=$3
  shift 3
  local slug log start end seconds

  slug=$(slug_step "${target}-${step}")
  log="$LOG_DIR/${slug}.log"
  start=$(date +%s)
  {
    echo "target=$target"
    echo "step=$step"
    echo "started_at=$(timestamp_utc)"
    echo "artifact=$artifact"
  } > "$log"

  if "$@" >> "$log" 2>&1; then
    end=$(date +%s)
    seconds=$((end - start))
    printf '%s\t%s\tOK\t%s\t%s\t%s\n' "$target" "$step" "$seconds" "$artifact" "$log" >> "$RESULTS"
    printf -- '- `%s/%s`: OK in %ss\n' "$target" "$step" "$seconds" >> "$SUMMARY"
  else
    end=$(date +%s)
    seconds=$((end - start))
    printf '%s\t%s\tFAIL\t%s\t%s\t%s\n' "$target" "$step" "$seconds" "$artifact" "$log" >> "$RESULTS"
    printf -- '- `%s/%s`: FAIL in %ss, see `%s`\n' "$target" "$step" "$seconds" "$log" >> "$SUMMARY"
    echo "error: step '$target/$step' failed; see $log" >&2
    tail -100 "$log" >&2 || true
    exit 1
  fi
}

mssql_query() {
  local db=$1
  local query=$2
  sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "$MSSQL_USER" -P "$MSSQL_PASSWORD" -C -d "$db" -b -Q "$query"
}

mssql_file() {
  local db=$1
  local file=$2
  sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "$MSSQL_USER" -P "$MSSQL_PASSWORD" -C -d "$db" -b -i "$file"
}

mssql_scalar() {
  local db=$1
  local query=$2
  sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "$MSSQL_USER" -P "$MSSQL_PASSWORD" -C \
    -d "$db" -h -1 -W -s '|' -Q "SET NOCOUNT ON; $query" 2>/dev/null \
    | awk 'NF {print; exit}'
}

pg_scalar() {
  local db=$1
  local sql=$2
  docker exec -i -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d "$db" -v ON_ERROR_STOP=1 -At -F '|' -c "$sql"
}

mysql_scalar() {
  local db=$1
  local sql=$2
  docker exec "$MYSQL_CONTAINER" mysql -h 127.0.0.1 -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
    --batch --raw --skip-column-names "$db" -e "$sql" 2>/dev/null | tr '\t' '|'
}

sqlite_scalar() {
  local path=$1
  local sql=$2
  sqlite3 -noheader -separator '|' "$path" "$sql"
}

reset_mssql_db() {
  local db=$1
  mssql_query master "IF DB_ID(N'${db}') IS NOT NULL BEGIN ALTER DATABASE [${db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [${db}]; END; CREATE DATABASE [${db}];"
}

reset_postgres_db() {
  local db=$1
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS ${db}" \
    -c "CREATE DATABASE ${db}"
}

reset_mysql_db() {
  local db=$1
  docker exec "$MYSQL_CONTAINER" mysql -h 127.0.0.1 -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
    -e "DROP DATABASE IF EXISTS ${db}; CREATE DATABASE ${db};"
}

reset_sqlite_db() {
  local path=$1
  mkdir -p "$(dirname "$path")"
  rm -f "$path"
}

target_url() {
  case "$1" in
    mssql) echo "mssql://${MSSQL_USER}:${MSSQL_PASSWORD}@${MSSQL_HOST}:${MSSQL_PORT}/${MSSQL_TARGET_DB}" ;;
    postgres) echo "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_TARGET_DB}" ;;
    mysql) echo "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_TARGET_DB}" ;;
    sqlite) echo "sqlite:////${SQLITE_TARGET_PATH#/}?mode=rwc" ;;
  esac
}

target_label() {
  case "$1" in
    mssql) echo "$MSSQL_TARGET_DB" ;;
    postgres) echo "$PG_TARGET_DB" ;;
    mysql) echo "$MYSQL_TARGET_DB" ;;
    sqlite) echo "$SQLITE_TARGET_PATH" ;;
  esac
}

reset_target() {
  case "$1" in
    mssql) reset_mssql_db "$MSSQL_TARGET_DB" ;;
    postgres) reset_postgres_db "$PG_TARGET_DB" ;;
    mysql) reset_mysql_db "$MYSQL_TARGET_DB" ;;
    sqlite) reset_sqlite_db "$SQLITE_TARGET_PATH" ;;
  esac
}

target_scalar() {
  local target=$1
  local sql=$2
  case "$target" in
    mssql) mssql_scalar "$MSSQL_TARGET_DB" "$sql" ;;
    postgres) pg_scalar "$PG_TARGET_DB" "$sql" ;;
    mysql) mysql_scalar "$MYSQL_TARGET_DB" "$sql" ;;
    sqlite) sqlite_scalar "$SQLITE_TARGET_PATH" "$sql" ;;
  esac
}

target_sql() {
  local target=$1
  local key=$2
  case "$key" in
    table_exists)
      case "$target" in
        mssql) echo "SELECT CASE WHEN OBJECT_ID(N'dbo.PostModerationEvents', N'U') IS NOT NULL THEN 't' ELSE 'f' END;" ;;
        postgres) echo "SELECT to_regclass('\"PostModerationEvents\"') IS NOT NULL;" ;;
        mysql) echo "SELECT IF(EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'PostModerationEvents'), 't', 'f');" ;;
        sqlite) echo "SELECT CASE WHEN EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'PostModerationEvents') THEN 't' ELSE 'f' END;" ;;
      esac
      ;;
    user_added_columns)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'Users' AND column_name IN ('LastSeenAt', 'ProfileScore');" ;;
        postgres) echo "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'Users' AND column_name IN ('LastSeenAt', 'ProfileScore');" ;;
        mysql) echo "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Users' AND column_name IN ('LastSeenAt', 'ProfileScore');" ;;
        sqlite) echo "SELECT COUNT(*) FROM pragma_table_info('Users') WHERE name IN ('LastSeenAt', 'ProfileScore');" ;;
      esac
      ;;
    additive_constraints)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.objects WHERE name IN ('PK_PostModerationEvents_Id', 'FK_PostModerationEvents_Posts', 'FK_PostModerationEvents_Users', 'CK_PostModerationEvents_EventType');" ;;
        postgres) echo "SELECT COUNT(*) FROM pg_constraint WHERE conname IN ('PK_PostModerationEvents_Id', 'FK_PostModerationEvents_Posts', 'FK_PostModerationEvents_Users', 'CK_PostModerationEvents_EventType');" ;;
        mysql) echo "SELECT (SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = DATABASE() AND constraint_name IN ('PK_PostModerationEvents_Id', 'FK_PostModerationEvents_Posts', 'FK_PostModerationEvents_Users')) + (SELECT COUNT(*) FROM information_schema.check_constraints WHERE constraint_schema = DATABASE() AND constraint_name = 'CK_PostModerationEvents_EventType');" ;;
        sqlite) echo "SELECT CASE WHEN (SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'PostModerationEvents') LIKE '%CK_PostModerationEvents_EventType%' THEN 4 ELSE 0 END;" ;;
      esac
      ;;
    additive_indexes)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.indexes WHERE name IN ('IX_Posts_OwnerUserId', 'IX_PostModerationEvents_PostId');" ;;
        postgres) echo "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname IN ('IX_Posts_OwnerUserId', 'IX_PostModerationEvents_PostId');" ;;
        mysql) echo "SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema = DATABASE() AND index_name IN ('IX_Posts_OwnerUserId', 'IX_PostModerationEvents_PostId');" ;;
        sqlite) echo "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN ('IX_Posts_OwnerUserId', 'IX_PostModerationEvents_PostId');" ;;
      esac
      ;;
    location_shape)
      case "$target" in
        mssql) echo "SELECT CAST(character_maximum_length AS varchar(20)) + '|' + is_nullable FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'Users' AND column_name = 'Location';" ;;
        postgres) echo "SELECT character_maximum_length, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'Users' AND column_name = 'Location';" ;;
        mysql) echo "SELECT CONCAT(character_maximum_length, '|', is_nullable) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Users' AND column_name = 'Location';" ;;
        sqlite) echo "SELECT type || '|' || CASE WHEN \"notnull\" = 1 THEN 'NO' ELSE 'YES' END FROM pragma_table_info('Users') WHERE name = 'Location';" ;;
      esac
      ;;
    website_shape)
      case "$target" in
        mssql) echo "SELECT CAST(character_maximum_length AS varchar(20)) + '|' + is_nullable FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'Users' AND column_name = 'WebsiteUrl';" ;;
        postgres) echo "SELECT character_maximum_length, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'Users' AND column_name = 'WebsiteUrl';" ;;
        mysql) echo "SELECT CONCAT(character_maximum_length, '|', is_nullable) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Users' AND column_name = 'WebsiteUrl';" ;;
        sqlite) echo "SELECT type || '|' || CASE WHEN \"notnull\" = 1 THEN 'NO' ELSE 'YES' END FROM pragma_table_info('Users') WHERE name = 'WebsiteUrl';" ;;
      esac
      ;;
    views_default)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.default_constraints dc JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id JOIN sys.objects o ON o.object_id = c.object_id WHERE o.name = 'Users' AND c.name = 'Views';" ;;
        postgres) echo "SELECT COALESCE(column_default, '') FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'Users' AND column_name = 'Views';" ;;
        mysql) echo "SELECT COALESCE(column_default, '') FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Users' AND column_name = 'Views';" ;;
        sqlite) echo "SELECT COALESCE(dflt_value, '') FROM pragma_table_info('Users') WHERE name = 'Views';" ;;
      esac
      ;;
    reason_length)
      case "$target" in
        mssql) echo "SELECT CAST(character_maximum_length AS varchar(20)) FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'PostModerationEvents' AND column_name = 'Reason';" ;;
        postgres) echo "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'PostModerationEvents' AND column_name = 'Reason';" ;;
        mysql) echo "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'PostModerationEvents' AND column_name = 'Reason';" ;;
        sqlite) echo "SELECT type FROM pragma_table_info('PostModerationEvents') WHERE name = 'Reason';" ;;
      esac
      ;;
    pack03_constraints)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.objects WHERE name IN ('CK_Users_ProfileScore_NonNegative', 'UQ_PostTypes_Type', 'CK_Posts_PostTypeId_Known', 'UQ_PostModerationEvents_Post_Event_CreatedAt');" ;;
        postgres) echo "SELECT COUNT(*) FROM pg_constraint WHERE conname IN ('CK_Users_ProfileScore_NonNegative', 'UQ_PostTypes_Type', 'CK_Posts_PostTypeId_Known', 'UQ_PostModerationEvents_Post_Event_CreatedAt');" ;;
        mysql) echo "SELECT (SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = DATABASE() AND constraint_name IN ('UQ_PostTypes_Type', 'UQ_PostModerationEvents_Post_Event_CreatedAt')) + (SELECT COUNT(*) FROM information_schema.check_constraints WHERE constraint_schema = DATABASE() AND constraint_name IN ('CK_Users_ProfileScore_NonNegative', 'CK_Posts_PostTypeId_Known'));" ;;
        sqlite) echo "SELECT 0;" ;;
      esac
      ;;
    comments_index)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.indexes WHERE name = 'IX_Comments_UserId_CreationDate';" ;;
        postgres) echo "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'IX_Comments_UserId_CreationDate';" ;;
        mysql) echo "SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema = DATABASE() AND index_name = 'IX_Comments_UserId_CreationDate';" ;;
        sqlite) echo "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'IX_Comments_UserId_CreationDate';" ;;
      esac
      ;;
    table_dropped)
      case "$target" in
        mssql) echo "SELECT CASE WHEN OBJECT_ID(N'dbo.PostModerationEvents', N'U') IS NOT NULL THEN 't' ELSE 'f' END;" ;;
        postgres) echo "SELECT to_regclass('\"PostModerationEvents\"') IS NOT NULL;" ;;
        mysql) echo "SELECT IF(EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'PostModerationEvents'), 't', 'f');" ;;
        sqlite) echo "SELECT CASE WHEN EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'PostModerationEvents') THEN 't' ELSE 'f' END;" ;;
      esac
      ;;
    last_seen_column)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'Users' AND column_name = 'LastSeenAt';" ;;
        postgres) echo "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'Users' AND column_name = 'LastSeenAt';" ;;
        mysql) echo "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Users' AND column_name = 'LastSeenAt';" ;;
        sqlite) echo "SELECT COUNT(*) FROM pragma_table_info('Users') WHERE name = 'LastSeenAt';" ;;
      esac
      ;;
    posts_owner_index)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.indexes WHERE name = 'IX_Posts_OwnerUserId';" ;;
        postgres) echo "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'IX_Posts_OwnerUserId';" ;;
        mysql) echo "SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema = DATABASE() AND index_name = 'IX_Posts_OwnerUserId';" ;;
        sqlite) echo "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'IX_Posts_OwnerUserId';" ;;
      esac
      ;;
    profile_check)
      case "$target" in
        mssql) echo "SELECT COUNT(*) FROM sys.objects WHERE name = 'CK_Users_ProfileScore_NonNegative';" ;;
        postgres) echo "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'CK_Users_ProfileScore_NonNegative';" ;;
        mysql) echo "SELECT COUNT(*) FROM information_schema.check_constraints WHERE constraint_schema = DATABASE() AND constraint_name = 'CK_Users_ProfileScore_NonNegative';" ;;
        sqlite) echo "SELECT CASE WHEN (SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'Users') LIKE '%CK_Users_ProfileScore_NonNegative%' THEN 1 ELSE 0 END;" ;;
      esac
      ;;
  esac
}

check_no_schema_changes() {
  local target=$1
  local step=$2
  local artifact=$3
  local log="$LOG_DIR/$(slug_step "${target}-${step}").log"
  local content

  content=
  [[ -f "$artifact" ]] && content=$(tr -d '\r' < "$artifact")
  {
    echo "target=$target"
    echo "step=$step"
    echo "artifact=$artifact"
    echo "expected=-- No schema changes detected."
  } > "$log"
  if [[ "$content" == "-- No schema changes detected." ]]; then
    printf '%s\t%s\tOK\t0\t%s\t%s\n' "$target" "$step" "$artifact" "$log" >> "$RESULTS"
    printf -- '- `%s/%s`: OK\n' "$target" "$step" >> "$SUMMARY"
  else
    {
      echo "actual:"
      if [[ -f "$artifact" ]]; then
        sed -n '1,160p' "$artifact"
      else
        echo "missing artifact"
      fi
    } >> "$log"
    printf '%s\t%s\tFAIL\t0\t%s\t%s\n' "$target" "$step" "$artifact" "$log" >> "$RESULTS"
    printf -- '- `%s/%s`: FAIL, see `%s`\n' "$target" "$step" "$log" >> "$SUMMARY"
    echo "error: convergence check '$target/$step' failed; see $log" >&2
    tail -160 "$log" >&2 || true
    exit 1
  fi
}

expect_target_value() {
  local target=$1
  local label=$2
  local sql_key=$3
  local expected=$4
  local actual

  actual=$(target_scalar "$target" "$(target_sql "$target" "$sql_key")")
  if [[ "$actual" != "$expected" ]]; then
    echo "catalog check failed: $target/$label" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    return 1
  fi
}

expected_value() {
  local target=$1
  local key=$2
  case "$key" in
    location_shape)
      [[ "$target" == sqlite ]] && echo "VARCHAR(200)|NO" || echo "200|NO"
      ;;
    website_shape)
      [[ "$target" == sqlite ]] && echo "VARCHAR(512)|YES" || echo "512|YES"
      ;;
    views_default)
      case "$target" in
        mssql) echo "1" ;;
        postgres|mysql|sqlite) echo "0" ;;
      esac
      ;;
    reason_length)
      [[ "$target" == sqlite ]] && echo "VARCHAR(800)" || echo "800"
      ;;
  esac
}

catalog_checks() {
  local target=$1
  local label=$2
  case "$label" in
    01_additive)
      expect_target_value "$target" "$label table exists" table_exists "t"
      expect_target_value "$target" "$label user columns" user_added_columns "2"
      expect_target_value "$target" "$label constraints" additive_constraints "4"
      expect_target_value "$target" "$label indexes" additive_indexes "2"
      ;;
    02_column_evolution)
      expect_target_value "$target" "$label location" location_shape "$(expected_value "$target" location_shape)"
      expect_target_value "$target" "$label website" website_shape "$(expected_value "$target" website_shape)"
      expect_target_value "$target" "$label views default" views_default "$(expected_value "$target" views_default)"
      expect_target_value "$target" "$label reason" reason_length "$(expected_value "$target" reason_length)"
      ;;
    03_constraints_indexes)
      expect_target_value "$target" "$label constraints" pack03_constraints "4"
      expect_target_value "$target" "$label index" comments_index "1"
      ;;
    04_destructive)
      expect_target_value "$target" "$label table dropped" table_dropped "f"
      expect_target_value "$target" "$label column dropped" last_seen_column "0"
      expect_target_value "$target" "$label index dropped" posts_owner_index "0"
      ;;
    05_drop_constraint)
      expect_target_value "$target" "$label check dropped" profile_check "0"
      ;;
    *)
      echo "error: no catalog checks defined for pack '$label'" >&2
      return 1
      ;;
  esac
}

run_catalog_checks() {
  local target=$1
  local label=$2
  local log="$LOG_DIR/$(slug_step "${target}-${label}-catalog").log"
  local start end seconds

  start=$(date +%s)
  {
    echo "target=$target"
    echo "pack=$label"
    echo "started_at=$(timestamp_utc)"
  } > "$log"
  if catalog_checks "$target" "$label" >> "$log" 2>&1; then
    end=$(date +%s)
    seconds=$((end - start))
    printf '%s\t%s\tOK\t%s\t%s\t%s\n' "$target" "${label}-catalog-checks" "$seconds" "$(target_label "$target")" "$log" >> "$RESULTS"
    printf -- '- `%s/%s-catalog-checks`: OK in %ss\n' "$target" "$label" "$seconds" >> "$SUMMARY"
  else
    end=$(date +%s)
    seconds=$((end - start))
    printf '%s\t%s\tFAIL\t%s\t%s\t%s\n' "$target" "${label}-catalog-checks" "$seconds" "$(target_label "$target")" "$log" >> "$RESULTS"
    printf -- '- `%s/%s-catalog-checks`: FAIL in %ss, see `%s`\n' "$target" "$label" "$seconds" "$log" >> "$SUMMARY"
    echo "error: catalog checks for '$target/$label' failed; see $log" >&2
    tail -100 "$log" >&2 || true
    exit 1
  fi
}

pack_supported_for_target() {
  local target=$1
  local label=$2
  if [[ "$target" == sqlite ]]; then
    case "$label" in
      02_column_evolution|03_constraints_indexes|05_drop_constraint) return 1 ;;
    esac
  fi
  return 0
}

run_step all source-db-exists "$SOURCE_DB" \
  mssql_query master "IF DB_ID(N'${SOURCE_DB}') IS NULL THROW 50000, 'StackOverflow2010 source database is missing', 1;"

for target in "${TARGETS[@]}"; do
  target_dir="$OUT_DIR/$target"
  mkdir -p "$target_dir"
  target_url_value=$(target_url "$target")

  run_step "$target" reset-sqlserver-source-clone "$WORK_SOURCE_DB" reset_mssql_db "$WORK_SOURCE_DB"
  run_step "$target" reset-target "$(target_label "$target")" reset_target "$target"

  run_step "$target" bootstrap-sqlserver-source-clone "$target_dir/bootstrap_source_mssql.sql" \
    "$UVG" --trust-cert "$SOURCE_URL" "$WORK_SOURCE_URL" \
      --generator ddl --target-dialect mssql --outfile "$target_dir/bootstrap_source_mssql.sql" \
      --apply --progress off

  run_step "$target" bootstrap-target "$target_dir/bootstrap_${target}.sql" \
    "$UVG" --trust-cert "$WORK_SOURCE_URL" "$target_url_value" \
      --generator ddl --target-dialect "$target" --outfile "$target_dir/bootstrap_${target}.sql" \
      --apply --progress off

  run_step "$target" baseline-diff "$target_dir/baseline_diff.sql" \
    "$UVG" --trust-cert "$WORK_SOURCE_URL" "$target_url_value" \
      --generator ddl --target-dialect "$target" --outfile "$target_dir/baseline_diff.sql"
  check_no_schema_changes "$target" baseline-convergence "$target_dir/baseline_diff.sql"

  for pack in "$PACK_DIR"/*.sql; do
    label="$(basename "$pack" .sql)"
    if ! pack_supported_for_target "$target" "$label"; then
      printf '%s\t%s\tSKIP\t0\t%s\t%s\n' "$target" "$label" "$pack" "" >> "$RESULTS"
      printf -- '- `%s/%s`: SKIP (SQLite requires table rebuild for this drift class)\n' "$target" "$label" >> "$SUMMARY"
      continue
    fi

    apply_sql="$target_dir/${label}_apply.sql"
    post_diff="$target_dir/${label}_post_diff.sql"

    run_step "$target" "${label}-apply-source-pack" "$pack" mssql_file "$WORK_SOURCE_DB" "$pack"
    run_step "$target" "${label}-uvg-apply-target" "$apply_sql" \
      "$UVG" --trust-cert "$WORK_SOURCE_URL" "$target_url_value" \
        --generator ddl --target-dialect "$target" --outfile "$apply_sql" \
        --apply --progress off
    run_step "$target" "${label}-post-diff" "$post_diff" \
      "$UVG" --trust-cert "$WORK_SOURCE_URL" "$target_url_value" \
        --generator ddl --target-dialect "$target" --outfile "$post_diff"
    check_no_schema_changes "$target" "${label}-convergence" "$post_diff"
    run_catalog_checks "$target" "$label"
  done
done

cat >> "$SUMMARY" <<EOF

## Result

- Finished: $(timestamp_utc)
- Status: pass
- Results: $RESULTS
EOF

echo "Stack Overflow drift matrix bundle: $OUT_DIR"
echo "Results: $RESULTS"
echo "Summary: $SUMMARY"
