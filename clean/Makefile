# -*- coding: utf-8 -*-
#  :vim ft=make:
# Author: LB
# Maintainer(s): LB
#  clean/Makefile
# -----------------------------------------------------------

HERE := $(shell git rev-parse --show-toplevel)

.PHONY: all clean


all: output/clean.parquet

clean:
	rm output/*

output/clean.parquet: \
		clean.py \
		../import/output/prisoners.parquet
	-mkdir output/
	$(HERE)/.venv/bin/python $< \
		--input=../import/output/prisoners.parquet \
		--output=$@

# done
