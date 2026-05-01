# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     LB
# Maintainers: LB
# Copyright:   2024,  GPL v2 or later
# =========================================
# import/import.py

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


def parse_counts(input_counts):
	df = pd.read_csv(input_counts, skiprows=10, encoding="latin-1")
	df = df.iloc[1:12].copy()
	df = df[[df.columns[0], df.columns[2], df.columns[4], df.columns[6], df.columns[8],
					 df.columns[10], df.columns[12], df.columns[14], df.columns[16], df.columns[18], df.columns[20]]]
	df.columns = ["year", "total", "federal", "state", "male", "female",
								"white", "black", "hispanic", "american_indian_alaska_native", "asian"]
	df = df[df["year"].astype(str).str.isdigit()].copy()
	for col in df.columns[1:]:
		df[col] = df[col].astype(str).str.replace(",", "").str.strip().astype(int)
	df["year"] = df["year"].astype(int)
	return df


def parse_rates(input_rates):
	df = pd.read_csv(input_rates, skiprows=11, encoding="latin-1")
	df = df.iloc[1:12].copy()
	df = df[[df.columns[0], df.columns[2], df.columns[4], df.columns[6],
					 df.columns[9], df.columns[11], df.columns[13], df.columns[15],
					 df.columns[17], df.columns[19], df.columns[21]]]
	df.columns = ["year", "total", "federal", "state", "male", "female",
								"white", "black", "hispanic", "american_indian_alaska_native", "asian"]
	df = df[df["year"].astype(str).str.isdigit()].copy()
	for col in df.columns[1:]:
		df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "").str.strip(), errors="coerce").fillna(0).astype(int)
	df["year"] = df["year"].astype(int)
	return df


def get_args():
	parser = argparse.ArgumentParser()
	parser.add_argument("--input-counts", default="../_import_/p23stt03.csv")
	parser.add_argument("--input-rates", default="../_import_/p23stt05.csv")
	parser.add_argument("--output", default="output/prisoners.parquet")
	args = parser.parse_args()
	assert Path(args.input_counts).exists()
	assert Path(args.input_rates).exists()
	return args
#}}}

# main {{{
if __name__ == '__main__':
	# arg handling
	args = get_args()

	logging.info("Parsing prisoner counts")
	counts = parse_counts(args.input_counts)

	logging.info("Parsing imprisonment rates")
	rates = parse_rates(args.input_rates)

	counts = counts.add_suffix("_count")
	counts = counts.rename(columns={"year_count": "year"})
	rates = rates.add_suffix("_rate")
	rates = rates.rename(columns={"year_rate": "year"})

	df = counts.merge(rates, on="year", how="outer")

	assert initial_asserts(df)

	df.to_parquet(args.output)
#}}}
# done.
