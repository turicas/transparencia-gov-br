#!/bin/bash

set -e
OUTPUT_PATH=data/output
LOG_PATH=data/log

run_spider() {
	spider="$1"

	mkdir -p $LOG_PATH $OUTPUT_PATH
	log_filename="$LOG_PATH/${spider}.log"
	output_filename="$OUTPUT_PATH/${spider}.csv.gz"
	rm -rf $log_filename $output_filename
	echo "Running ${spider} - check $log_filename for logs and $output_filename for output"
	time scrapy crawl \
		--loglevel=INFO --logfile=$log_filename \
		$spider \
		-t "csv.gz" \
		-o $output_filename
}

if [ ! -z "$1" ]; then
	run_spider $1
else
	for spider in pagamento pagamento_historico execucao_despesa orcamento_despesa; do
		run_spider $spider
	done
fi
