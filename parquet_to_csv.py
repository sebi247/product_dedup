#!/usr/bin/env python3
"""
parquet_to_csv.py — Convert a Parquet file to CSV.

Usage
-----
$ python parquet_to_csv.py deduplicated_products.parquet [-o output.csv]

If -o/--out is omitted the script writes a CSV next to the input file with
“.csv” substituted for the “.parquet” extension.

Requirements
------------
• pandas ≥ 1.5 (automatically pulls in pyarrow, the default engine)
  pip install pandas pyarrow
"""

import argparse
import pathlib
import sys
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a Parquet file to CSV.")
    parser.add_argument(
        "parquet_path", help="Path to the input .parquet file (e.g. deduplicated_products.parquet)"
    )
    parser.add_argument(
        "-o",
        "--out",
        "--output",
        dest="csv_path",
        help="Output .csv file path (default: replace .parquet with .csv)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=None,
        help="Write CSV in chunks of this many rows (reduces peak RAM).",
    )

    args = parser.parse_args()

    parquet_path = pathlib.Path(args.parquet_path)
    if not parquet_path.exists():
        sys.exit(f"Input file not found: {parquet_path}")

    # derive default CSV path if none supplied
    csv_path = pathlib.Path(args.csv_path) if args.csv_path else parquet_path.with_suffix(".csv")

    print(f"Loading {parquet_path} …")
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as exc:
        sys.exit(f"Unable to read parquet: {exc}")

    print(f"Writing {csv_path} ({len(df):,} rows) …")
    # If chunksize was specified, stream to CSV in pieces to lower memory use
    if args.chunksize:
        for i in range(0, len(df), args.chunksize):
            mode = "w" if i == 0 else "a"
            header = i == 0
            df.iloc[i : i + args.chunksize].to_csv(csv_path, index=False, mode=mode, header=header)
    else:
        df.to_csv(csv_path, index=False)

    print("Done.")


if __name__ == "__main__":
    main()
