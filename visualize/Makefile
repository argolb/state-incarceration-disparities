# -*- coding: utf-8 -*-
#  :vim ft=make:
# Author: LB
# Maintainer(s): LB
#  visualize/Makefile
# -----------------------------------------------------------

HERE := $(shell git rev-parse --show-toplevel)

.PHONY: all clean


all: output/visualize.parquet

clean:
	rm output/*

output/visualize.parquet: \
		visualize.py \
		../analyze/output/analyze.parquet
	-mkdir output/
	$(HERE)/.venv/bin/python $< \
		--input=../analyze/output/analyze.parquet \
		--output=$@

# done
