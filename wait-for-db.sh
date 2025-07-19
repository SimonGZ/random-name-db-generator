#!/bin/sh
set -e

if [ -f /status/initialized ]; then
  echo "Initialization already completed, skipping."
  exit 0
fi

echo "Waiting for Postgres to be ready..."
until pg_isready -d "${DB_NAME}" -h "$DB_HOST" -U "${DB_USER}"; do
    sleep 2
done

echo "Postgres is ready. Running setup scripts..."

# Option 1: Auto-confirm interaction (recommended for Docker)
NONINTERACTIVE=1 ruby surnames-db-builder.rb
NONINTERACTIVE=1 ruby firstnames-db-builder.rb

touch /status/initialized
