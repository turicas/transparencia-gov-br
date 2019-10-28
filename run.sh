#!/bin/bash

rm -rf data/output
mkdir -p data/output data/log

#time scrapy crawl \
#	--loglevel=INFO --logfile=data/log/pagamento.log \
#	pagamento \
#	-t csv.gz -o data/output/pagamento.csv.gz

time scrapy crawl \
	--loglevel=INFO --logfile=data/log/pagamento2.log \
	pagamento \
	-t csv.gz -o data/output/pagamento2.csv.gz
