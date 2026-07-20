import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

# ── Configuration ──────────────────────────────────────────────
st.set_page_config(page_title="DE PoC Dashboard", layout="wide", page_icon="📊")

# Custom CSS for dark theme look
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        text-align: center;
        margin-bottom: 20px;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        color: #4CAF50;
    }
    .metric-label {
        font-size: 1.1rem;
        color: #B0B0B0;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
</style>
""", unsafe_allow_html=True)

# ── Database Connection ────────────────────────────────────────
@st.cache_resource
def get_db_connection():
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
# ── Data Loading ───────────────────────────────────────────────
@st.cache_data
def load_data():
    try:
        data_dir = os.path.join(os.path.dirname(__file__), "data", "powerbi_data")
        
        # Load CSVs
        fact_sales = pd.read_csv(os.path.join(data_dir, "fact_sales.csv"))
        dim_customer = pd.read_csv(os.path.join(data_dir, "dim_customer.csv"))
        dim_product = pd.read_csv(os.path.join(data_dir, "dim_product.csv"))
        dim_date = pd.read_csv(os.path.join(data_dir, "dim_date.csv"))
        
        # Merge them like the original SQL query did
        df = fact_sales.merge(dim_date[["date_key", "full_date"]], left_on="order_date_key", right_on="date_key", how="left")
        df = df.merge(dim_product[["product_key", "product_category_name"]], on="product_key", how="left")
        df = df.merge(dim_customer[["customer_key", "customer_state"]], on="customer_key", how="left")
            
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# ── Main UI ────────────────────────────────────────────────────
st.title("🛒 E-Commerce Data Engineering PoC Dashboard")
st.markdown("Visualizing the **Gold Layer** Data (Star Schema)")

df = load_data()

if df.empty:
    st.warning("No data found in the Gold layer. Please run the ETL pipeline first.")
else:
    # ── Calculate KPIs
    total_revenue = df["payment_value"].sum()
    total_orders  = df["order_id"].nunique()
    aov           = total_revenue / total_orders if total_orders > 0 else 0
    total_freight = df["freight_value"].sum()

    # ── Display KPI Cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Total Revenue</div><div class="metric-value">${total_revenue:,.2f}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Total Orders</div><div class="metric-value">{total_orders:,}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Avg Order Value</div><div class="metric-value">${aov:,.2f}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Total Freight Cost</div><div class="metric-value">${total_freight:,.2f}</div></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── Charts ─────────────────────────────────────────────────
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("📈 Revenue Over Time")
        daily_rev = df.groupby("full_date")["payment_value"].sum().reset_index()
        fig_line = px.line(daily_rev, x="full_date", y="payment_value", 
                           labels={"full_date": "Date", "payment_value": "Revenue ($)"},
                           color_discrete_sequence=["#4CAF50"])
        fig_line.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_line, use_container_width=True)

    with row1_col2:
        st.subheader("📊 Sales by Product Category")
        cat_rev = df.groupby("product_category_name")["payment_value"].sum().reset_index()
        cat_rev = cat_rev.sort_values("payment_value", ascending=True).tail(10) # Top 10
        fig_bar = px.bar(cat_rev, x="payment_value", y="product_category_name", orientation='h',
                         labels={"payment_value": "Revenue ($)", "product_category_name": "Category"},
                         color_discrete_sequence=["#2196F3"])
        fig_bar.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_bar, use_container_width=True)

    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        st.subheader("🗺️ Orders by Customer State")
        state_orders = df.groupby("customer_state")["order_id"].nunique().reset_index()
        fig_map = px.treemap(state_orders, path=["customer_state"], values="order_id",
                             color="order_id", color_continuous_scale="Viridis",
                             labels={"order_id": "Number of Orders"})
        st.plotly_chart(fig_map, use_container_width=True)

    with row2_col2:
        st.subheader("🍩 Order Status Distribution")
        status_dist = df.groupby("order_status")["order_id"].nunique().reset_index()
        fig_pie = px.pie(status_dist, names="order_status", values="order_id", hole=0.5,
                         color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")
    st.markdown("Dashboard connected directly to PostgreSQL `gold` schema.")
