#!/bin/bash

set -e

SCHEMA_PATH="schema"
OUTPUT_PATH="data/output"

function import_table() {
	tablename="$1"

	echo "DROP TABLE IF EXISTS ${tablename};" | psql "$POSTGRESQL_URI"
	time rows pgimport \
		--schema="$SCHEMA_PATH/${tablename}.csv" \
		--input-encoding="utf-8" \
		--dialect="excel" \
		"$OUTPUT_PATH/${tablename}.csv.gz" \
		"$POSTGRESQL_URI" \
		"$tablename"
}

if [ -z "$POSTGRESQL_URI" ]; then
	echo "ERROR: you must set POSTGRESQL_URI environment variable."
	exit 1
fi

if [ ! -z "$1" ]; then
	import_table $1
else
	for table in pagamento pagamento_historico execucao_despesa orcamento_despesa; do
		import_table $table
	done
fi
