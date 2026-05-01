# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     LB
# Maintainers: LB
# Copyright:   2024,  GPL v2 or later
# =========================================
# transform/transform.py

# ---- dependencies {{{
from pathlib import Path
from sys import stdout
import argparse
import logging
import pandas as pd
#}}}

# ---- support methods {{{
def initial_asserts(df):
	return 1


def get_args():
	parser = argparse.ArgumentParser()
	parser.add_argument("--input", default="../clean/output/clean.parquet")
	parser.add_argument("--output", default="output/transform.parquet")
	args = parser.parse_args()
	assert Path(args.input).exists()
	return args
#}}}

# main {{{
if __name__ == '__main__':
	# arg handling
	args = get_args()

	df = pd.read_parquet(args.input)

	df.to_parquet(args.output)
#}}}
# done.
