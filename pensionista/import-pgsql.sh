#!/bin/bash

if [ -z "$DATABASE_URL" ]; then
	echo "ERROR: must set $DATABASE_URL with postgres connection string"
	exit 1
fi

for table in cadastro observacao remuneracao; do
	rows pgimport \
		--dialect=excel \
		--input-encoding=utf-8 \
		--schema=schema/pensionista_${table}.csv \
		data/output/pensionista_${table}.csv.gz \
		$DATABASE_URL \
		pensionista_${table}
done
