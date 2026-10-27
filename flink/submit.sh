#!/usr/bin/env bash
set -euo pipefail

# A bounded Kafka scan finishes at the offsets present when the job starts.
# Batch execution finalizes the last window without artificial transactions.
mode="${1:-unbounded}"
case "$mode" in
  unbounded) runtime=streaming ;;
  latest-offset) runtime=batch ;;
  *) echo "Usage: submit.sh [unbounded|latest-offset]" >&2; exit 2 ;;
esac
# Batch execution sorts a finite snapshot and closes all windows, even for an
# unordered historical replay. Live streaming still uses the one-minute watermark.
sed -e "s/__BOUNDED_MODE__/$mode/g" -e "s/__RUNTIME_MODE__/$runtime/g" \
  /opt/flink/aml/account_features.sql > /tmp/account_features.sql
exec /opt/flink/bin/sql-client.sh -f /tmp/account_features.sql
