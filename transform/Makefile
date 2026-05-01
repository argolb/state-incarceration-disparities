# -*- coding: utf-8 -*-
#  :vim ft=make:
# Author: LB
# Maintainer(s): LB
#  transform/Makefile
# -----------------------------------------------------------

HERE := $(shell git rev-parse --show-toplevel)

.PHONY: all clean


all: output/transform.parquet

clean:
	rm output/*

output/transform.parquet: \
		src/transform.py \
		../clean/output/clean.parquet
	-mkdir output/
	$(HERE)/.venv/bin/python $< \
		--input=../clean/output/clean.parquet \
		--output=$@

# done
