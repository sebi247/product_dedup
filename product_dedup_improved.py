from __future__ import annotations

import json
import argparse
import hashlib
import os
from collections import defaultdict
from typing import Any, Dict, List

import pandas as pd
from joblib import Parallel, delayed
from rapidfuzz import fuzz
import numpy as np

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def clean_text(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower()
    return " ".join(
        text.translate(str.maketrans("", "", "\n\r\t"))
            .translate(str.maketrans("", "", "!\"#$%&'()*+,./:;<=>?@[\\]^`{|}~"))
            .split()
    )


def normalize_id(val: Any) -> Any:
    """Flatten numpy arrays or lists to single scalar or tuple for hashing."""
    # 1) Handle numpy arrays first
    if isinstance(val, np.ndarray):
        return val.item() if val.size == 1 else tuple(val.tolist())
    # 2) Handle lists next
    if isinstance(val, list):
        return tuple(val)
    # 3) Now handle None / NaN
    if val is None or pd.isna(val):
        return None
    # 4) Everything else stays
    return val

# ---------------------------------------------------------------------------
# Blocking key
# ---------------------------------------------------------------------------

def blocking_key(record: Dict[str, Any]) -> str:
    """Brand + first 10 chars of title → block similar items together"""
    brand = clean_text(record.get("brand", ""))
    title = clean_text(
        record.get("product_title", "") or record.get("product_name", "")
    )
    return f"{brand}|{title[:10]}"

# ---------------------------------------------------------------------------
# Merging logic
# ---------------------------------------------------------------------------

NAME_FIELDS = ("product_title", "product_name", "name", "title")


def are_similar(a: Dict[str, Any], b: Dict[str, Any], thresh: int = 90) -> bool:
    """Fuzzy-check two records for duplication."""
    id1, id2 = a.get("product_identifier"), b.get("product_identifier")
    if id1 and id2 and id1 == id2:
        return True
    for field in NAME_FIELDS:
        n1, n2 = a.get(field), b.get(field)
        if n1 and n2:
            if fuzz.token_set_ratio(clean_text(n1), clean_text(n2)) >= thresh:
                return True
            break
    return False


def merge_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge a list of duplicate product dicts into one enriched entry."""
    merged: Dict[str, Any] = {}
    list_fields = {
        k for rec in records for k, v in rec.items() if isinstance(v, (list, tuple, np.ndarray))
    }
    for rec in records:
        for k, v in rec.items():
            if v is None or (isinstance(v, (float, int)) and pd.isna(v)):
                continue
            if k in list_fields:
                merged.setdefault(k, [])
                items = (
                    v.tolist() if isinstance(v, np.ndarray)
                    else list(v) if isinstance(v, (list, tuple))
                    else [v]
                )
                merged[k].extend(items)
            else:
                if k not in merged:
                    merged[k] = v
                else:
                    old = merged[k]
                    if isinstance(v, str) and len(v) > len(str(old)):
                        merged[k] = v
                    elif isinstance(v, (int, float)) and v > (old or float('-inf')):
                        merged[k] = v
    # Deduplicate any list fields
    for k in list_fields:
        if k in merged:
            seen = set()
            uniq = []
            for x in merged[k]:
                h = hashlib.md5(str(x).encode()).hexdigest()
                if h not in seen:
                    seen.add(h)
                    uniq.append(x)
            merged[k] = uniq
    return merged

# ---------------------------------------------------------------------------
# Clustering per block
# ---------------------------------------------------------------------------

def process_block(block: List[Dict[str, Any]], thresh: int) -> List[Dict[str, Any]]:
    clusters: List[List[Dict[str, Any]]] = []
    rem = block.copy()
    while rem:
        seed = rem.pop()
        cluster = [seed]
        new_rem = []
        for item in rem:
            if are_similar(seed, item, thresh):
                cluster.append(item)
            else:
                new_rem.append(item)
        rem = new_rem
        clusters.append(cluster)
    return [merge_records(c) for c in clusters]

# ---------------------------------------------------------------------------
# Main dedupe driver
# ---------------------------------------------------------------------------

def dedupe(
    in_path: str,
    out_path: str,
    workers: int,
    thresh: int,
    chunksize: int | None
) -> None:
    dfs = (
        pd.read_parquet(in_path, chunksize=chunksize)
        if chunksize
        else [pd.read_parquet(in_path)]
    )
    result: List[Dict[str, Any]] = []
    for idx, df in enumerate(dfs, start=1):
        print(f"→ chunk {idx}: {len(df):,} rows")
        df["product_identifier"] = df["product_identifier"].apply(normalize_id)
        has_id = df[df["product_identifier"].notna()]
        no_id  = df[df["product_identifier"].isna()]
        # 1. exact merge
        for _, group in has_id.groupby("product_identifier", dropna=True):
            result.append(merge_records(group.to_dict("records")))
        # 2. block & fuzzy
        buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in no_id.to_dict("records"):
            buckets[blocking_key(rec)].append(rec)
        merged = Parallel(n_jobs=workers)(
            delayed(process_block)(b, thresh) for b in buckets.values()
        )
        for sub in merged:
            result.extend(sub)
    out_df = pd.DataFrame(result)
    out_df["product_identifier"] = out_df["product_identifier"].apply(
        lambda x: None
        if x is None
        else (json.dumps(x) if isinstance(x, (list, tuple)) else x)
    )
    out_df.to_parquet(out_path, index=False)
    print(f"✓ {len(out_df):,} unique products saved to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Efficient product deduplication."
    )
    parser.add_argument("input", help="input .parquet")
    parser.add_argument("output", help="output .parquet")
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 4
    )
    parser.add_argument(
        "--similarity",
        type=int,
        default=90
    )
    parser.add_argument("--chunksize", type=int)
    args = parser.parse_args()
    dedupe(
        args.input,
        args.output,
        args.workers,
        args.similarity,
        args.chunksize
    )