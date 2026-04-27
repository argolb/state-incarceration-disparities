import pandas as pd


def parse_counts():
    df = pd.read_csv("../_import_/p23stt03.csv", skiprows=10, encoding="latin-1")
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


def parse_rates():
    df = pd.read_csv("../_import_/p23stt05.csv", skiprows=11, encoding="latin-1")
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


def main():
    counts = parse_counts()
    rates = parse_rates()

    counts.to_parquet("prisoners_counts.parquet", index=False)
    rates.to_parquet("imprisonment_rates.parquet", index=False)


if __name__ == "__main__":
    main()