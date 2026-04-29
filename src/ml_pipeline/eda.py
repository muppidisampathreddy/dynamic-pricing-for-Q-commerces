import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from .config import FIGURES_DIR

sns.set_theme(style="whitegrid", palette="deep")
plt.rcParams["axes.unicode_minus"] = False
RS = "Rs."


def _save(fig, name):
    path = FIGURES_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_price_distribution(df):
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.histplot(df["price"], bins=60, kde=True, ax=ax, color="#3b82f6")
    ax.set_xscale("log")
    ax.set_title("Product Price Distribution (log scale)")
    ax.set_xlabel("Price (Rs, log)")
    ax.set_ylabel("Count")
    return _save(fig, "price_distribution.png")


def chart_discount_distribution(df):
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.histplot(df["discount_pct"], bins=40, kde=True, ax=ax, color="#10b981")
    ax.axvline(df["discount_pct"].median(), color="red", linestyle="--",
               label=f"Median: {df['discount_pct'].median():.1f}%")
    ax.set_title("Discount Percentage Distribution")
    ax.set_xlabel("Discount %")
    ax.set_ylabel("Count")
    ax.legend()
    return _save(fig, "discount_distribution.png")


def chart_discount_by_category(df):
    if "category" not in df.columns:
        return None
    means = df.groupby("category")["discount_pct"].mean().sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(x=means.values, y=means.index, ax=ax, palette="viridis")
    ax.set_title("Average Discount % by Category")
    ax.set_xlabel("Mean Discount %")
    ax.set_ylabel("")
    return _save(fig, "discount_by_category.png")


def chart_top_brands(df, top_n=15):
    if "brand" not in df.columns:
        return None
    brands = df[df["brand"].str.lower() != "unbranded"]["brand"].value_counts().head(top_n)
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x=brands.values, y=brands.index, ax=ax, palette="rocket")
    ax.set_title(f"Top {top_n} Brands by Product Count")
    ax.set_xlabel("Number of Products")
    ax.set_ylabel("")
    return _save(fig, "top_brands.png")


def chart_price_vs_rating(df):
    if "rating" not in df.columns:
        return None
    sample = df.sample(min(len(df), 3000), random_state=42)
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.scatterplot(data=sample, x="rating", y="price", hue="category",
                    alpha=0.6, ax=ax, legend=False, s=20)
    ax.set_yscale("log")
    ax.set_title("Price vs Rating (sample, colored by category)")
    ax.set_xlabel("Rating")
    ax.set_ylabel("Price (Rs, log)")
    return _save(fig, "price_vs_rating.png")


def chart_oos_by_category(df):
    if "is_oos" not in df.columns or "category" not in df.columns:
        return None
    oos = df.groupby("category")["is_oos"].mean().sort_values(ascending=True) * 100
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(x=oos.values, y=oos.index, ax=ax, palette="flare")
    ax.set_title("Out-of-Stock Rate by Category")
    ax.set_xlabel("% Out of Stock")
    ax.set_ylabel("")
    return _save(fig, "out_of_stock_by_category.png")


def chart_price_box_by_category(df):
    if "category" not in df.columns:
        return None
    top_cats = df["category"].value_counts().head(15).index
    sub = df[df["category"].isin(top_cats)]
    fig, ax = plt.subplots(figsize=(11, 7))
    order = sub.groupby("category")["price"].median().sort_values().index
    sns.boxplot(data=sub, x="category", y="price", order=order, ax=ax)
    ax.set_yscale("log")
    ax.set_title("Price Distribution by Category (top 15)")
    plt.xticks(rotation=45, ha="right")
    ax.set_ylabel("Price (Rs, log)")
    return _save(fig, "price_box_by_category.png")


def chart_correlation_heatmap(df):
    num_cols = ["price", "mrp", "discount_pct", "unit_value", "rating", "inventory"]
    num_cols = [c for c in num_cols if c in df.columns]
    if len(num_cols) < 3:
        return None
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(df[num_cols].corr(), annot=True, fmt=".2f", cmap="coolwarm",
                center=0, ax=ax)
    ax.set_title("Numeric Feature Correlations")
    return _save(fig, "correlation_heatmap.png")


def run_all_eda(df):
    """Run every chart, skipping any that error or have missing columns."""
    charts = [
        chart_price_distribution,
        chart_discount_distribution,
        chart_discount_by_category,
        chart_top_brands,
        chart_price_vs_rating,
        chart_oos_by_category,
        chart_price_box_by_category,
        chart_correlation_heatmap,
    ]
    saved = []
    for fn in charts:
        try:
            path = fn(df)
            if path:
                saved.append(path.name)
        except Exception as e:
            print(f"  [EDA] {fn.__name__} failed: {e}")
    return saved
