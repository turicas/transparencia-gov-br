#!/bin/bash

rm -rf data/output
mkdir -p data/output data/log

for spider in pagamento pagamento-hist; do
	log_filename="data/log/${spider}.log"
	output_filename="data/output/${spider}.csv.gz"
	echo "Running ${spider} - check $log_filename for logs and $output_filename for output"
	time scrapy crawl \
		--loglevel=INFO --logfile=$log_filename \
		$spider \
		-t csv.gz \
		-o $output_filename
done
