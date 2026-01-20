"""
Customer × Item Demand Forecasting Pipeline
--------------------------------------------
Purpose:
- Read latest invoice data from SQL Server
- Recompute demand features and forecasts
- Persist outputs for Streamlit UI

Execution:
- Designed to be run as a scheduled batch job
"""


# Imports


import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

print("Imports loaded successfully")


# Paths & Configuration


# Project root = demand_forecasting_project/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = PROJECT_ROOT / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"Project root resolved to: {PROJECT_ROOT}")
print(f"Output directory: {OUTPUT_DIR}")


# Database Connection


print("Connecting to SQL Server...")

# IMPORTANT:
# - Keep credentials exactly as you already use them
# - Do NOT hardcode them differently here

username =  "sa"
password = quote_plus("sa(2017)")


engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@NOETICPC10\\SQL_2017/SSPLORBITDB_LIVE11DEC2025"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&TrustServerCertificate=yes"
)

# Lightweight connection test
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))

print("SQL Server connection established successfully")


# STEP 3 — Load Raw Staging Data


print("Loading raw staging data...")

df_raw = pd.read_sql(
    """
    SELECT
        InvoiceDate,
        CustId,
        ItemCode,
        Quantity
    FROM dbo.Stg_CustomerItemDispatch
    """,
    engine
)

print(f"Raw rows loaded: {len(df_raw):,}")

# Safety check
assert len(df_raw) > 0, "Staging table returned zero rows"


# STEP 4 — Data Type Normalization


print("Normalizing data types...")

df_raw["InvoiceDate"] = pd.to_datetime(df_raw["InvoiceDate"])
df_raw["CustId"] = df_raw["CustId"].astype(int)
df_raw["ItemCode"] = df_raw["ItemCode"].astype(str)
df_raw["Quantity"] = df_raw["Quantity"].astype(float)

print("Data type normalization complete")


# STEP 5 — Monthly Aggregation


print("Performing monthly aggregation...")

# Create Year-Month period
df_raw["YearMonth"] = df_raw["InvoiceDate"].dt.to_period("M")

monthly_df = (
    df_raw
    .groupby(["CustId", "ItemCode", "YearMonth"], as_index=False)
    .agg(TotalQty=("Quantity", "sum"))
)

# ---- Minimal validation ----
assert len(monthly_df) > 0, "Monthly aggregation produced zero rows"
assert (monthly_df["TotalQty"] >= 0).all(), "Negative quantities found after aggregation"

min_month = monthly_df["YearMonth"].min()
max_month = monthly_df["YearMonth"].max()

print(f"Monthly aggregation complete: {min_month} → {max_month}")



# STEP 6 — Full Month Expansion


print("Building full Customer × Item × Month grid...")

all_months = pd.period_range(
    start=monthly_df["YearMonth"].min(),
    end=monthly_df["YearMonth"].max(),
    freq="M"
)

customer_item_map = (
    monthly_df[["CustId", "ItemCode"]]
    .drop_duplicates()
)

full_grid = (
    customer_item_map
    .assign(key=1)
    .merge(
        pd.DataFrame({"YearMonth": all_months, "key": 1}),
        on="key"
    )
    .drop(columns="key")
)

monthly_full = (
    full_grid
    .merge(
        monthly_df,
        on=["CustId", "ItemCode", "YearMonth"],
        how="left"
    )
)

monthly_full["TotalQty"] = monthly_full["TotalQty"].fillna(0)

assert monthly_full["TotalQty"].isna().sum() == 0, "Null values found after expansion"

print(
    f"Full expansion complete | "
    f"Pairs: {customer_item_map.shape[0]:,} | "
    f"Months: {len(all_months)} | "
    f"Rows: {monthly_full.shape[0]:,}"
)


# STEP 8 — Historical Demand Segmentation (Layer 1)


print("Computing historical demand metrics (Layer 1)...")

layer1_metrics = (
    monthly_full
    .groupby(["CustId", "ItemCode"])
    .agg(
        avg_qty=("TotalQty", "mean"),
        std_qty=("TotalQty", "std"),
        active_months=("TotalQty", lambda x: (x > 0).sum()),
        total_months=("TotalQty", "count")
    )
    .reset_index()
)

layer1_metrics["std_qty"] = layer1_metrics["std_qty"].fillna(0)

layer1_metrics["cv"] = np.where(
    layer1_metrics["avg_qty"] > 0,
    layer1_metrics["std_qty"] / layer1_metrics["avg_qty"],
    0
)

layer1_metrics["zero_months"] = (
    layer1_metrics["total_months"] - layer1_metrics["active_months"]
)

layer1_metrics["zero_ratio"] = (
    layer1_metrics["zero_months"] / layer1_metrics["total_months"]
)

def classify_demand(row):
    if row["active_months"] <= 2:
        return "One-time"
    elif row["zero_ratio"] > 0.80:
        return "Intermittent"
    elif row["cv"] > 1.5:
        return "Lumpy"
    elif row["cv"] > 0.5:
        return "Moderate"
    else:
        return "Stable"

layer1_metrics["DemandSegment"] = layer1_metrics.apply(classify_demand, axis=1)

assert layer1_metrics["DemandSegment"].isna().sum() == 0, "Unassigned demand segments"

print("Layer 1 metrics & segmentation complete")


# STEP 9 — Temporal / Recency Features (Layer 2)


print("Computing temporal / recency features (Layer 2)...")

# Reference month for planning
current_month = monthly_full["YearMonth"].max()

# ---- Layer 2a: Long-term boundary ----
layer2_base = (
    monthly_full
    .loc[monthly_full["TotalQty"] > 0]
    .groupby(["CustId", "ItemCode"])
    .agg(
        first_order_month=("YearMonth", "min"),
        last_order_month=("YearMonth", "max")
    )
    .reset_index()
)

layer2_base["months_since_last_order"] = (
    (current_month.year - layer2_base["last_order_month"].dt.year) * 12
    +
    (current_month.month - layer2_base["last_order_month"].dt.month)
)

assert (layer2_base["months_since_last_order"] >= 0).all(), "Negative recency detected"

# ---- Layer 2b: Recent pulse (last 12 months) ----
recent_cutoff = current_month - 12

recent_activity = (
    monthly_full
    .loc[monthly_full["YearMonth"] > recent_cutoff]
    .groupby(["CustId", "ItemCode"])
    .agg(
        recent_active_months=("TotalQty", lambda x: (x > 0).sum()),
        recent_avg_qty=("TotalQty", "mean")
    )
    .reset_index()
)

print("Layer 2 features computed")


# STEP 10 — PlanningStatus & ForecastPolicy


print("Assigning PlanningStatus and ForecastPolicy...")

planning_df = (
    layer1_metrics
    .merge(layer2_base, on=["CustId", "ItemCode"], how="left")
    .merge(recent_activity, on=["CustId", "ItemCode"], how="left")
)

planning_df["recent_active_months"] = planning_df["recent_active_months"].fillna(0)
planning_df["recent_avg_qty"] = planning_df["recent_avg_qty"].fillna(0)
planning_df["months_since_last_order"] = planning_df["months_since_last_order"].fillna(
    planning_df["total_months"]
)

def assign_planning_status(row):
    if row["DemandSegment"] == "One-time":
        return "Ignore"
    elif row["recent_active_months"] == 0 and row["months_since_last_order"] > 12:
        return "Inactive"
    elif row["recent_active_months"] >= 6:
        return "Active"
    else:
        return "At-Risk"

planning_df["PlanningStatus"] = planning_df.apply(assign_planning_status, axis=1)

def assign_forecast_policy(row):
    if row["PlanningStatus"] == "Active":
        return "Auto-Forecast"
    elif row["PlanningStatus"] == "At-Risk":
        if (row["recent_active_months"] >= 2) or (row["recent_avg_qty"] > row["avg_qty"]):
            return "Advisory-Forecast"
        else:
            return "No-Forecast"
    else:
        return "No-Forecast"

planning_df["ForecastPolicy"] = planning_df.apply(assign_forecast_policy, axis=1)

assert planning_df["PlanningStatus"].isna().sum() == 0
assert planning_df["ForecastPolicy"].isna().sum() == 0

print("PlanningStatus & ForecastPolicy assigned successfully")


# STEP 11 — Forecast Computation


print("Computing next-month forecasts...")

def forecast_next_month(ts, segment, recent_active_months):
    ts_nonzero = ts[ts > 0]

    if segment in ("Stable", "Moderate"):
        return ts.tail(6).mean()

    if segment == "Lumpy":
        if len(ts_nonzero) == 0:
            return 0
        return ts_nonzero.tail(6).median()

    if segment == "Intermittent":
        if len(ts_nonzero) == 0:
            return 0
        avg_size = ts_nonzero.tail(6).mean()
        probability = recent_active_months / 12
        return avg_size * probability

    return 0



# STEP 12 — Forecast Output Table


forecast_rows = []

for _, row in planning_df.iterrows():

    if row["ForecastPolicy"] == "No-Forecast":
        continue

    ts = (
        monthly_full
        .loc[
            (monthly_full["CustId"] == row["CustId"]) &
            (monthly_full["ItemCode"] == row["ItemCode"])
        ]
        .sort_values("YearMonth")["TotalQty"]
    )

    forecast_qty = forecast_next_month(
        ts,
        row["DemandSegment"],
        row["recent_active_months"]
    )

    forecast_rows.append({
        "CustId": row["CustId"],
        "ItemCode": row["ItemCode"],
        "ForecastQty_NextMonth": round(forecast_qty, 0),
    })

forecast_df = pd.DataFrame(forecast_rows)

assert len(forecast_df) > 0, "No forecasts generated"

print(f"Forecasts generated for {len(forecast_df):,} Customer–Item pairs")



# STEP 13 — Persist Outputs


print("Persisting outputs to disk...")

# ---- Forecast Summary ----
forecast_summary = (
    planning_df
    .merge(
        forecast_df,
        on=["CustId", "ItemCode"],
        how="left"
    )[
        [
            "CustId",
            "ItemCode",
            "DemandSegment",
            "PlanningStatus",
            "ForecastPolicy",
            "ForecastQty_NextMonth",
            "avg_qty",
            "cv",
            "zero_ratio",
            "recent_active_months",
            "months_since_last_order",
        ]
    ]
)

forecast_summary.to_csv(
    OUTPUT_DIR / "forecast_summary.csv",
    index=False
)

# ---- Monthly History ----
monthly_history = monthly_full[
    ["CustId", "ItemCode", "YearMonth", "TotalQty"]
]

monthly_history.to_csv(
    OUTPUT_DIR / "monthly_history.csv",
    index=False
)

# ---- Customer–Item Map ----
customer_item_map = (
    planning_df[["CustId", "ItemCode"]]
    .drop_duplicates()
)

customer_item_map.to_csv(
    OUTPUT_DIR / "customer_item_map.csv",
    index=False
)

print("Pipeline completed successfully")

