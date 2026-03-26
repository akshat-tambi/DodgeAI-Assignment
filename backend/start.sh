#!/usr/bin/env bash
set -euo pipefail

: "${MYSQL_DATABASE:=dodgeai}"
: "${MYSQL_USER:=dodgeai}"
: "${MYSQL_PASSWORD:=somepassword}"
: "${MYSQL_HOST:=127.0.0.1}"
: "${MYSQL_PORT:=3306}"
: "${MYSQL_DATA_DIR:=/var/data/mysql}"
: "${APP_HOST:=0.0.0.0}"
: "${APP_PORT:=${PORT:-8000}}"

if [[ "${MYSQL_HOST}" != "127.0.0.1" && "${MYSQL_HOST}" != "localhost" ]]; then
  echo "This container starts a local MariaDB. Set MYSQL_HOST to 127.0.0.1 or localhost."
  exit 1
fi

mkdir -p "${MYSQL_DATA_DIR}" /run/mysqld
chown -R mysql:mysql "${MYSQL_DATA_DIR}" /run/mysqld

if [[ ! -d "${MYSQL_DATA_DIR}/mysql" ]]; then
  mariadb-install-db --user=mysql --datadir="${MYSQL_DATA_DIR}" >/tmp/mariadb-install.log
fi

mariadbd \
  --user=mysql \
  --datadir="${MYSQL_DATA_DIR}" \
  --bind-address=127.0.0.1 \
  --port="${MYSQL_PORT}" \
  --socket=/run/mysqld/mysqld.sock \
  --pid-file=/run/mysqld/mysqld.pid \
  --skip-networking=0 \
  --log-error=/tmp/mariadb.err &
MYSQL_PID=$!

cleanup() {
  kill "${MYSQL_PID}" >/dev/null 2>&1 || true
  wait "${MYSQL_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

for i in {1..60}; do
  if mariadb-admin ping -h127.0.0.1 -P"${MYSQL_PORT}" --silent; then
    break
  fi
  sleep 1
  if [[ "${i}" -eq 60 ]]; then
    echo "MariaDB failed to start in time."
    exit 1
  fi
done

mariadb -h127.0.0.1 -P"${MYSQL_PORT}" -uroot <<SQL
CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'127.0.0.1' IDENTIFIED BY '${MYSQL_PASSWORD}';
CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'localhost' IDENTIFIED BY '${MYSQL_PASSWORD}';
GRANT ALL PRIVILEGES ON \`${MYSQL_DATABASE}\`.* TO '${MYSQL_USER}'@'127.0.0.1';
GRANT ALL PRIVILEGES ON \`${MYSQL_DATABASE}\`.* TO '${MYSQL_USER}'@'localhost';
FLUSH PRIVILEGES;
SQL

mkdir -p "${UPLOAD_DIR:-/var/data/uploads}"

echo "Starting API on ${APP_HOST}:${APP_PORT}"
exec uvicorn app.main:app --host "${APP_HOST}" --port "${APP_PORT}"
