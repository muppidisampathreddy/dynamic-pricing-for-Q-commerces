import re
import pandas as pd
from .config import REQUIRED_COLUMNS, RARE_UNIT_TYPE_MIN_COUNT


class SchemaError(Exception):
    pass


def load_csv(path):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"Missing required columns: {missing}")
    return df


def coerce_types(df):
    df = df.copy()
    for col in ["price", "mrp", "unit_value", "rating"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["product_id", "merchant_id", "inventory", "unavail_qty"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col].isin(["", "nan", "None", "NaN"]), col] = pd.NA
    return df


def drop_constant_columns(df):
    dropped = []
    for col in df.columns:
        if df[col].nunique(dropna=False) <= 1:
            dropped.append(col)
    if dropped:
        df = df.drop(columns=dropped)
    return df, dropped


def dedupe_products(df):
    if "product_id" not in df.columns:
        return df, 0
    before = len(df)
    df = df.sort_values("scraped_at" if "scraped_at" in df.columns else "product_id")
    df = df.drop_duplicates(subset=["product_id"], keep="first").reset_index(drop=True)
    return df, before - len(df)


def normalize_unit_type(df):
    if "unit_type" not in df.columns:
        return df

    df = df.copy()
    df["unit_type"] = df["unit_type"].fillna("").str.lower().str.strip()

    multipack_re = re.compile(r"^x\s*(\d+\.?\d*)\s*([a-z]+)$")

    def parse_row(row):
        ut = row["unit_type"]
        uv = row["unit_value"]
        m = multipack_re.match(str(ut))
        if m:
            inner_qty = float(m.group(1))
            inner_unit = m.group(2)
            try:
                pack_count = float(uv) if pd.notna(uv) else 1.0
            except Exception:
                pack_count = 1.0
            return pd.Series({
                "unit_value": pack_count * inner_qty,
                "unit_type": inner_unit,
                "is_multipack": 1,
            })
        return pd.Series({
            "unit_value": uv,
            "unit_type": ut,
            "is_multipack": 0,
        })

    parsed = df.apply(parse_row, axis=1)
    df["unit_value"] = parsed["unit_value"]
    df["unit_type"] = parsed["unit_type"]
    df["is_multipack"] = parsed["is_multipack"].astype(int)

    aliases = {"piece": "pc", "pieces": "pc", "pcs": "pc"}
    df["unit_type"] = df["unit_type"].replace(aliases)

    counts = df["unit_type"].value_counts()
    rare = counts[counts < RARE_UNIT_TYPE_MIN_COUNT].index
    df.loc[df["unit_type"].isin(rare), "unit_type"] = "other"
    df.loc[df["unit_type"].eq(""), "unit_type"] = "other"
    return df


def impute_missing(df):
    df = df.copy()
    if "rating" in df.columns:
        median_rating = df["rating"].median()
        df["rating"] = df["rating"].fillna(median_rating)
    if "brand" in df.columns:
        df["brand"] = df["brand"].fillna("unbranded").replace("", "unbranded")
    return df


def clean(df):
    """Full clean pipeline. Returns (df, report_dict)."""
    report = {"input_rows": len(df), "input_cols": len(df.columns)}
    df = coerce_types(df)
    df, dropped = drop_constant_columns(df)
    report["dropped_constant_columns"] = dropped
    df, removed = dedupe_products(df)
    report["dedupe_removed"] = removed
    df = normalize_unit_type(df)
    df = impute_missing(df)
    df = df[df["price"].notna() & (df["price"] > 0)]
    df = df[df["mrp"].notna() & (df["mrp"] > 0)]
    df = df.reset_index(drop=True)
    report["output_rows"] = len(df)
    report["output_cols"] = len(df.columns)
    return df, report
