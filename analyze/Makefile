# -*- coding: utf-8 -*-
#  :vim ft=make:
# Author: LB
# Maintainer(s): LB
#  analyze/Makefile
# -----------------------------------------------------------

HERE := $(shell git rev-parse --show-toplevel)

.PHONY: all clean


all: output/analyze.parquet

clean:
	rm output/*

output/analyze.parquet: \
		src/analyze.py \
		../transform/output/transform.parquet
	-mkdir output/
	$(HERE)/.venv/bin/python $< \
		--input=../transform/output/transform.parquet \
		--output=$@

# done
