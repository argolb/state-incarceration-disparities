INPUTS = ../_import_/p23stt03.csv ../_import_/p23stt05.csv
OUTPUTS = prisoners_counts.parquet imprisonment_rates.parquet

.PHONY: all clean

all: $(OUTPUTS)

$(OUTPUTS): import.py $(INPUTS)
	../.venv/bin/python import.py

clean:
	rm -f $(OUTPUTS)