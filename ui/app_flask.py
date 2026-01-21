from flask import Flask, render_template, request, send_file
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd

app = Flask(__name__)

# PATHS (matches your project)

app = Flask(__name__)

# BASE_DIR = Path(__file__).resolve().parent.parent
# OUTPUT_DIR = BASE_DIR / "outputs"
# STATIC_DIR = Path(__file__).resolve().parent / "static" / "charts"

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR.parent / "outputs"
STATIC_DIR = BASE_DIR / "static" / "charts"

# BASE_DIR = Path(__file__).resolve().parent.parent
# OUTPUT_DIR = BASE_DIR / "outputs"
# STATIC_DIR = Path(__file__).resolve().parent / "static" / "charts"
# STATIC_DIR.mkdir(parents=True, exist_ok=True)


# LOAD DATA (same as Streamlit)

forecast_summary = pd.read_csv(OUTPUT_DIR / "forecast_summary.csv")
monthly_history = pd.read_csv(OUTPUT_DIR / "monthly_history.csv")
customer_item_map = pd.read_csv(OUTPUT_DIR / "customer_item_map.csv")
customer_names = pd.read_csv(OUTPUT_DIR / "customer_names.csv")
item_names = pd.read_csv(OUTPUT_DIR / "item_names.csv")

cust_id_to_name = dict(zip(customer_names.CustId, customer_names.CustName))
item_code_to_name = dict(zip(item_names.ItemCode, item_names.ItemName))


# MAIN PAGE (replaces Streamlit rerun)

@app.route("/", methods=["GET"])
def dashboard():
    customers = sorted(customer_item_map["CustId"].unique())

    selected_customer = request.args.get("customer", type=int)
    selected_item = request.args.get("item")

    items = []
    summary = None
    history = None
    chart_file = None

    if selected_customer:
        items = customer_item_map.loc[
            customer_item_map["CustId"] == selected_customer, "ItemCode"
        ].tolist()

    if selected_customer and selected_item:
        summary = forecast_summary[
            (forecast_summary.CustId == selected_customer) &
            (forecast_summary.ItemCode == selected_item)
        ]

        history = monthly_history[
            (monthly_history.CustId == selected_customer) &
            (monthly_history.ItemCode == selected_item)
        ].sort_values("YearMonth")

        chart_file = generate_chart(history, summary, selected_customer, selected_item)

    return render_template(
        "dashboard.html",
        customers=customers,
        items=items,
        selected_customer=selected_customer,
        selected_item=selected_item,
        summary=summary,
        history=history,
        chart_file=chart_file,
        cust_name=cust_id_to_name.get(selected_customer),
        item_name=item_code_to_name.get(selected_item)
    )


# CHART GENERATION

def generate_chart(history, summary, cust, item):
    if history.empty:
        return None

    history["YearMonth"] = pd.to_datetime(history["YearMonth"].astype(str))
    last_month = history["YearMonth"].max()

    forecast_val = (
        None if summary.empty else summary.iloc[0]["ForecastQty_NextMonth"]
    )

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(history["YearMonth"], history["TotalQty"], marker="o", label="Actual")

    if pd.notna(forecast_val):
        ax.scatter(last_month + MonthEnd(1), forecast_val, color="red", s=120)

    ax.set_title("Actual Demand with Next-Month Forecast")
    ax.grid(True)

    file_path = STATIC_DIR / f"{cust}_{item}.png"
    plt.savefig(file_path, bbox_inches="tight")
    plt.close()

    return f"charts/{cust}_{item}.png"


# CSV DOWNLOAD (replaces st.download_button)

@app.route("/download")
def download():
    cust = request.args.get("customer", type=int)
    item = request.args.get("item")

    df = monthly_history[
        (monthly_history.CustId == cust) &
        (monthly_history.ItemCode == item)
    ]

    file_path = STATIC_DIR / f"monthly_{cust}_{item}.csv"
    df.to_csv(file_path, index=False)

    return send_file(file_path, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
