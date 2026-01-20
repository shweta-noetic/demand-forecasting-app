import streamlit as st
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd

# App Configuration

st.set_page_config(
    page_title="Customer × Item Demand Forecast",
    layout="wide"
)

st.title("Customer × Item Demand Forecast")
st.caption("Advisory demand forecasts to support production & material planning")


# Resolve Project Paths

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "outputs"


# Load Persisted Outputs

forecast_summary = pd.read_csv(OUTPUT_DIR / "forecast_summary.csv")
monthly_history = pd.read_csv(OUTPUT_DIR / "monthly_history.csv")
customer_item_map = pd.read_csv(OUTPUT_DIR / "customer_item_map.csv")

customer_names = pd.read_csv(OUTPUT_DIR / "customer_names.csv")
item_names = pd.read_csv(OUTPUT_DIR / "item_names.csv")

# Ensure correct dtypes
customer_names["CustId"] = customer_names["CustId"].astype(int)
customer_names["CustName"] = customer_names["CustName"].astype(str)

item_names["ItemCode"] = item_names["ItemCode"].astype(str)
item_names["ItemName"] = item_names["ItemName"].astype(str)

# Create lookup dictionaries
cust_id_to_name = dict(
    zip(customer_names["CustId"], customer_names["CustName"])
)

item_code_to_name = dict(
    zip(item_names["ItemCode"], item_names["ItemName"])
)

# Ensure correct dtypes
forecast_summary["CustId"] = forecast_summary["CustId"].astype(int)
forecast_summary["ItemCode"] = forecast_summary["ItemCode"].astype(str)

monthly_history["CustId"] = monthly_history["CustId"].astype(int)
monthly_history["ItemCode"] = monthly_history["ItemCode"].astype(str)

customer_item_map["CustId"] = customer_item_map["CustId"].astype(int)
customer_item_map["ItemCode"] = customer_item_map["ItemCode"].astype(str)


# Filters — Main Page (UI-2)

st.subheader("🔎 Filters")

col1, col2 = st.columns(2)

with col1:
    customer_list = sorted(customer_item_map["CustId"].unique())
    # selected_customer = st.selectbox("Select Customer", customer_list)
    selected_customer = st.selectbox(
    "Select Customer",
    options=customer_list,
    format_func=lambda x: f"{cust_id_to_name.get(x, 'Unknown')} ({x})",
    key="customer_select"
)


with col2:
    items_for_customer = (
        customer_item_map
        .loc[customer_item_map["CustId"] == selected_customer, "ItemCode"]
        .sort_values()
    )
    # selected_item = st.selectbox("Select Item", items_for_customer)
    selected_item = st.selectbox(
    "Select Item",
    options=items_for_customer,
    format_func=lambda x: f"{item_code_to_name.get(x, 'Unknown')} ({x})",
    key="item_select"
    )


# Selected Context

st.markdown("---")
st.subheader("Selected Context")
# st.write(f"**Customer ID:** {selected_customer}")
# st.write(f"**Item Code:** {selected_item}")
st.write(
    f"**Customer:** {cust_id_to_name.get(selected_customer, 'Unknown')} "
    f"({selected_customer})"
)

st.write(
    f"**Item:** {item_code_to_name.get(selected_item, 'Unknown')} "
    f"({selected_item})"
)



# Forecast Summary (UI-3)

selected_summary = forecast_summary[
    (forecast_summary["CustId"] == selected_customer) &
    (forecast_summary["ItemCode"] == selected_item)
]

st.markdown("---")
st.subheader("📊 Forecast Summary")

if selected_summary.empty:
    st.warning("No forecast information available for this Customer × Item.")
else:
    row = selected_summary.iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Demand Segment", row["DemandSegment"])
    c2.metric("Planning Status", row["PlanningStatus"])
    c3.metric("Forecast Policy", row["ForecastPolicy"])

    st.markdown("")

    if pd.isna(row["ForecastQty_NextMonth"]):
        st.metric("📦 Forecast Quantity (Next Month)", "No Forecast")
    else:
        st.metric(
            "📦 Forecast Quantity (Next Month)",
            f"{int(row['ForecastQty_NextMonth']):,}"
        )


# Monthly History & Chart (UI-4 + UI-5)

st.markdown("---")
st.subheader("📈 Monthly Demand History")

selected_history = (
    monthly_history[
        (monthly_history["CustId"] == selected_customer) &
        (monthly_history["ItemCode"] == selected_item)
    ]
    .sort_values("YearMonth")
)

if selected_history.empty:
    st.info("No historical demand found for this Customer × Item.")
else:
    plot_df = selected_history.copy()
    plot_df["YearMonth"] = pd.to_datetime(plot_df["YearMonth"].astype(str))

    last_month = plot_df["YearMonth"].max()
    forecast_value = (
        None if selected_summary.empty
        else selected_summary.iloc[0]["ForecastQty_NextMonth"]
    )

    fig, ax = plt.subplots(figsize=(11, 4))

    # Actual demand line
    ax.plot(
        plot_df["YearMonth"],
        plot_df["TotalQty"],
        marker="o",
        label="Actual Demand"
    )

    # Forecast point (next month)
    if pd.notna(forecast_value):
        forecast_month = last_month + MonthEnd(1)
        ax.scatter(
            forecast_month,
            forecast_value,
            color="red",
            s=120,
            label="Next Month Forecast"
        )

    ax.set_title("Actual Demand with Next-Month Forecast")
    ax.set_xlabel("Month")
    ax.set_ylabel("Quantity")
    ax.legend()
    ax.grid(True)

    st.pyplot(fig)


# Monthly History Table (UI-7)

st.markdown("### 📅 Monthly Demand Table")
st.dataframe(
    selected_history,
    use_container_width=True
)


# Export Option (UI-8)

st.markdown("---")
st.subheader("⬇️ Export")

export_df = selected_history.copy()

st.download_button(
    label="Download Monthly History (CSV)",
    data=export_df.to_csv(index=False),
    file_name=f"monthly_history_{selected_customer}_{selected_item}.csv",
    mime="text/csv"
)

st.caption("Forecasts are advisory. Final planning decisions remain with users.")





