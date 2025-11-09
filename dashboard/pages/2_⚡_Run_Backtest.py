"""Run Backtest page - Configure and launch backtests."""

import streamlit as st
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="Run Backtest", page_icon="⚡", layout="wide")

API_URL = st.session_state.get("api_url", "http://localhost:8000")

st.title("⚡ Run Backtest")
st.markdown("Configure and launch backtesting jobs")

# Backtest configuration
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Strategy Configuration")

    # Load available strategies
    try:
        response = requests.get(f"{API_URL}/api/strategy/list")
        if response.status_code == 200:
            strategies = response.json()
            strategy_names = [s["name"] for s in strategies]

            if not strategy_names:
                st.error("No strategies available")
                st.stop()

            strategy_name = st.selectbox("Select Strategy", strategy_names)

            # Get strategy info
            strategy = next((s for s in strategies if s["name"] == strategy_name), None)

            if strategy:
                st.info(f"**Description:** {strategy['description']}")
                st.caption(f"Recommended timeframe: {strategy['timeframe']}")

                # Get parameter definitions
                response = requests.get(f"{API_URL}/api/strategy/{strategy_name}/params")
                if response.status_code == 200:
                    params_def = response.json()

                    st.subheader("Strategy Parameters")
                    strategy_params = {}

                    for param in params_def:
                        if param["type"] == "int":
                            strategy_params[param["name"]] = st.number_input(
                                param["description"],
                                min_value=param.get("min", 0),
                                max_value=param.get("max", 1000),
                                value=param["default"],
                                step=1,
                                key=param["name"],
                            )
                        elif param["type"] == "float":
                            strategy_params[param["name"]] = st.number_input(
                                param["description"],
                                min_value=float(param.get("min", 0)),
                                max_value=float(param.get("max", 100)),
                                value=float(param["default"]),
                                step=0.1,
                                key=param["name"],
                            )
                        elif param["type"] == "bool":
                            strategy_params[param["name"]] = st.checkbox(
                                param["description"],
                                value=param["default"],
                                key=param["name"],
                            )
        else:
            st.error("Failed to load strategies")
            st.stop()
    except Exception as e:
        st.error(f"Error connecting to API: {e}")
        st.stop()

with col2:
    st.subheader("Backtest Settings")

    # Initial cash
    initial_cash = st.number_input(
        "Initial Cash ($)",
        min_value=1000.0,
        max_value=1000000.0,
        value=10000.0,
        step=1000.0,
    )

    # Commission
    commission = st.number_input(
        "Commission (%)",
        min_value=0.0,
        max_value=1.0,
        value=0.1,
        step=0.01,
    ) / 100

    # Timeframe
    timeframe = st.selectbox(
        "Timeframe",
        ["1D", "1H"],
        index=0,
    )

# Symbol selection
st.markdown("---")
st.subheader("Symbol Selection")

col1, col2 = st.columns(2)

with col1:
    # Exchange
    try:
        response = requests.get(f"{API_URL}/api/data/exchanges")
        if response.status_code == 200:
            exchanges = response.json()["exchanges"]
            exchange = st.selectbox("Exchange", exchanges, index=0)
        else:
            exchange = "US"
    except:
        exchange = "US"

with col2:
    # Input method
    input_method = st.radio("Input Method", ["Manual Entry", "Search"])

if input_method == "Manual Entry":
    symbols_text = st.text_area(
        "Enter symbols (one per line)",
        placeholder="AAPL\nMSFT\nGOOGL",
        height=100,
    )
    symbols = [s.strip() for s in symbols_text.split("\n") if s.strip()]
else:
    # Search
    search_query = st.text_input("Search Symbol")
    symbols = []  # Simplified for Phase 1

# Date range
st.subheader("Date Range")
col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input(
        "Start Date",
        value=datetime.now() - timedelta(days=365),
    )

with col2:
    end_date = st.date_input(
        "End Date",
        value=datetime.now(),
    )

# Backtest name
backtest_name = st.text_input(
    "Backtest Name (optional)",
    placeholder=f"{strategy_name} - {datetime.now().strftime('%Y-%m-%d')}",
)

# Summary
st.markdown("---")
st.subheader("Summary")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Strategy", strategy_name)
col2.metric("Symbols", len(symbols))
col3.metric("Initial Cash", f"${initial_cash:,.0f}")
col4.metric("Timeframe", timeframe)

# Run button
if symbols:
    if st.button("🚀 Run Backtest", type="primary", use_container_width=True):
        with st.spinner("Launching backtest..."):
            try:
                # Submit backtest request
                request_data = {
                    "name": backtest_name or None,
                    "symbols": symbols,
                    "exchange": exchange,
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "strategy_name": strategy_name,
                    "strategy_params": strategy_params,
                    "timeframe": timeframe,
                    "initial_cash": initial_cash,
                    "commission": commission,
                }

                response = requests.post(
                    f"{API_URL}/api/backtest/run",
                    json=request_data,
                )

                if response.status_code == 200:
                    result = response.json()
                    backtest_id = result["backtest_id"]

                    st.success(f"✅ Backtest started! ID: {backtest_id}")

                    # Poll for status
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    for _ in range(120):  # Poll for up to 2 minutes
                        time.sleep(1)

                        status_response = requests.get(
                            f"{API_URL}/api/backtest/{backtest_id}"
                        )

                        if status_response.status_code == 200:
                            status_data = status_response.json()
                            progress = status_data.get("progress", 0)
                            status = status_data["status"]

                            progress_bar.progress(int(progress))
                            status_text.text(f"Status: {status} - {progress:.0f}%")

                            if status == "completed":
                                st.success("✅ Backtest completed successfully!")
                                st.balloons()

                                # Store backtest ID in session state
                                st.session_state.last_backtest_id = backtest_id

                                # Show quick results
                                metrics = status_data.get("metrics", {})
                                if metrics:
                                    st.subheader("Quick Results")
                                    col1, col2, col3, col4 = st.columns(4)
                                    col1.metric("Sharpe Ratio", f"{metrics.get('sharpe_ratio', 0):.2f}")
                                    col2.metric("Total Return", f"{metrics.get('total_return', 0):.2f}%")
                                    col3.metric("Max Drawdown", f"{metrics.get('max_drawdown', 0):.2f}%")
                                    col4.metric("Trades", metrics.get('num_trades', 0))

                                # Button to view full results
                                if st.button("📊 View Full Results"):
                                    st.switch_page("pages/3_📈_Results.py")

                                break
                            elif status == "failed":
                                st.error(f"❌ Backtest failed: {status_data.get('error')}")
                                break
                    else:
                        st.info("Backtest is taking longer than expected. Check Results page.")

                else:
                    st.error(f"Failed to start backtest: {response.text}")

            except Exception as e:
                st.error(f"Error: {e}")
else:
    st.warning("👆 Enter symbols to run backtest")
