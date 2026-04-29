import re
import numpy as np
import pandas as pd
from .config import KEYWORD_TO_CATEGORY, HIGH_DISCOUNT_THRESHOLD_PCT


_SUFFIX_RE = re.compile(r"\s+(pack|discount|combo|offer)$", re.IGNORECASE)


def map_keyword_to_category(kw: str) -> str:
    if kw is None or (isinstance(kw, float) and pd.isna(kw)):
        return "misc"
    base = str(kw).lower().strip()
    if base in KEYWORD_TO_CATEGORY:
        return KEYWORD_TO_CATEGORY[base]
    stripped = _SUFFIX_RE.sub("", base).strip()
    if stripped in KEYWORD_TO_CATEGORY:
        return KEYWORD_TO_CATEGORY[stripped]
    for k, cat in KEYWORD_TO_CATEGORY.items():
        if k in base:
            return cat
    return "misc"


def add_features(df):
    df = df.copy()

    df["discount_pct"] = ((df["mrp"] - df["price"]) / df["mrp"] * 100).clip(lower=0, upper=100)
    df["is_discounted"] = (df["discount_pct"] > 0).astype(int)
    df["is_high_discount"] = (df["discount_pct"] >= HIGH_DISCOUNT_THRESHOLD_PCT).astype(int)

    df["price_per_unit"] = df["price"] / df["unit_value"].replace(0, np.nan)
    df["price_per_unit"] = df["price_per_unit"].replace([np.inf, -np.inf], np.nan)
    df["price_per_unit"] = df["price_per_unit"].fillna(df["price_per_unit"].median())

    df["log_price"] = np.log1p(df["price"])
    df["log_mrp"] = np.log1p(df["mrp"])

    if "inventory" in df.columns:
        df["is_oos"] = (df["inventory"].fillna(0) == 0).astype(int)
        df["inventory"] = df["inventory"].fillna(0).astype(int)

    df["category"] = df["keyword"].apply(map_keyword_to_category)

    if "brand" in df.columns:
        df["is_unbranded"] = (df["brand"].str.lower() == "unbranded").astype(int)

    if "is_multipack" not in df.columns:
        df["is_multipack"] = 0

    return df


NUMERIC_FEATURES_FOR_PRICE = [
    "mrp", "unit_value", "rating", "inventory",
    "is_multipack", "is_unbranded", "price_per_unit", "log_mrp",
]
CATEGORICAL_FEATURES_FOR_PRICE = ["unit_type", "category", "brand"]

NUMERIC_FEATURES_FOR_DISCOUNT = [
    "price", "mrp", "unit_value", "rating", "inventory",
    "is_multipack", "is_unbranded", "price_per_unit",
]
CATEGORICAL_FEATURES_FOR_DISCOUNT = ["unit_type", "category"]

CLUSTER_FEATURES = [
    "price", "mrp", "discount_pct", "unit_value", "rating", "inventory",
]


def build_feature_lists(df, intended_for):
    """Filter feature lists to those actually present in df."""
    if intended_for == "price":
        num = [c for c in NUMERIC_FEATURES_FOR_PRICE if c in df.columns]
        cat = [c for c in CATEGORICAL_FEATURES_FOR_PRICE if c in df.columns]
    elif intended_for == "discount":
        num = [c for c in NUMERIC_FEATURES_FOR_DISCOUNT if c in df.columns]
        cat = [c for c in CATEGORICAL_FEATURES_FOR_DISCOUNT if c in df.columns]
    elif intended_for == "cluster":
        num = [c for c in CLUSTER_FEATURES if c in df.columns]
        cat = []
    else:
        raise ValueError(intended_for)
    return num, cat
