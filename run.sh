#!/bin/bash

set -e
OUTPUT_PATH=data/output
LOG_PATH=data/log
LOG_LEVEL=INFO
if [ "$1" = "--use-mirror" ]; then
	OPTS="-a use_mirror=true"
	shift
else
	OPTS=""
fi

run_spider() {
	spider="$1"

	mkdir -p $LOG_PATH $OUTPUT_PATH
	log_filename="$LOG_PATH/${spider}.log"
	output_filename="$OUTPUT_PATH/${spider}.csv.gz"
	rm -rf $log_filename $output_filename
	echo "Running ${spider} - check $log_filename for logs and $output_filename for output"
	time scrapy crawl \
		--loglevel=$LOG_LEVEL \
		--logfile=$log_filename \
		$OPTS \
		$spider \
		-t "csv.gz" \
		-o $output_filename
}

if [ ! -z "$1" ]; then
	spiders="$@"
else
	spiders="$(python transparenciagovbr/utils/print_spider_names.py)"
fi
for spider in $spiders; do
	run_spider $spider
done
