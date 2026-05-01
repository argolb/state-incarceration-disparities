# -*- coding: utf-8 -*-
#  :vim ft=make:
# Author: LB
# Maintainer(s): LB
#  import/Makefile
# -----------------------------------------------------------

HERE := $(shell git rev-parse --show-toplevel)

.PHONY: all clean


all: output/prisoners.parquet

clean:
	rm output/*

output/prisoners.parquet: \
		src/import.py \
		../_import_/p23stt03.csv \
		../_import_/p23stt05.csv
	-mkdir output/
	$(HERE)/.venv/bin/python $< \
		--input-counts=../_import_/p23stt03.csv \
		--input-rates=../_import_/p23stt05.csv \
		--output=$@

# done
