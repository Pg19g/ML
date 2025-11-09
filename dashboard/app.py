"""Main Streamlit dashboard for the Quant Platform."""

import streamlit as st
import requests
import os

# Configure page
st.set_page_config(
    page_title="Quant Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# API configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Store API URL in session state
if "api_url" not in st.session_state:
    st.session_state.api_url = API_URL


# Sidebar
with st.sidebar:
    st.title("📊 Quant Platform")
    st.markdown("---")

    # API connection status
    st.subheader("Connection Status")
    try:
        response = requests.get(f"{API_URL}/health", timeout=2)
        if response.status_code == 200:
            st.success("✅ API Connected")
        else:
            st.error("❌ API Error")
    except:
        st.error("❌ API Offline")

    st.markdown("---")

    # Quick stats (if API is available)
    st.subheader("Quick Stats")
    try:
        # Get strategies count
        response = requests.get(f"{API_URL}/api/strategy/list", timeout=2)
        if response.status_code == 200:
            strategies = response.json()
            st.metric("Strategies Available", len(strategies))

        # Get backtests count
        response = requests.get(f"{API_URL}/api/backtest/list", timeout=2)
        if response.status_code == 200:
            backtests = response.json()
            st.metric("Total Backtests", len(backtests))
    except:
        st.info("Stats unavailable")

    st.markdown("---")
    st.caption("v1.0.0 - Phase 1")


# Main content
st.title("📈 Quantitative Trading Platform")

st.markdown("""
Welcome to your **AI-powered backtesting platform**!

### Features
- 🌍 **Multi-Exchange Support** - US, LSE, WSE, XETRA and more
- 📊 **Market Data** - EOD and intraday data via EODHD
- 🎯 **Strategy Testing** - Built-in and custom strategies
- 📈 **Interactive Results** - Charts, metrics, and trade analysis
- 🤖 **AI Assistant** - Coming in Phase 2

### Getting Started

Use the sidebar to navigate between pages:

1. **📊 Data Explorer** - Browse and fetch market data
2. **⚡ Run Backtest** - Configure and launch backtests
3. **📈 Results** - Analyze backtest performance

### Quick Actions
""")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🔍 Explore Data", use_container_width=True):
        st.switch_page("pages/1_📊_Data_Explorer.py")

with col2:
    if st.button("🚀 Run Backtest", use_container_width=True):
        st.switch_page("pages/2_⚡_Run_Backtest.py")

with col3:
    if st.button("📊 View Results", use_container_width=True):
        st.switch_page("pages/3_📈_Results.py")


# Recent Activity
st.markdown("---")
st.subheader("Recent Backtests")

try:
    response = requests.get(f"{API_URL}/api/backtest/list?limit=5", timeout=2)
    if response.status_code == 200:
        backtests = response.json()

        if backtests:
            for bt in backtests:
                with st.expander(f"{bt['name']} - {bt['status'].upper()}"):
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Strategy", bt["strategy_name"])
                    col2.metric("Symbols", len(bt["symbols"]))

                    if bt.get("sharpe_ratio"):
                        col3.metric("Sharpe Ratio", f"{bt['sharpe_ratio']:.2f}")

                    if bt.get("total_return"):
                        st.metric("Total Return", f"{bt['total_return']:.2f}%")
        else:
            st.info("No backtests yet. Create your first backtest!")
    else:
        st.warning("Unable to load recent backtests")
except Exception as e:
    st.warning("Unable to connect to API")


# Footer
st.markdown("---")
st.caption("""
⚠️ **Disclaimer**: This platform is for educational and research purposes only.
Not financial advice. Past performance does not guarantee future results.
""")
