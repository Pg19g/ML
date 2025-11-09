"""Data Explorer page - Browse and fetch market data."""

import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="Data Explorer", page_icon="📊", layout="wide")

API_URL = st.session_state.get("api_url", "http://localhost:8000")

st.title("📊 Data Explorer")
st.markdown("Browse and fetch market data from EODHD API")

# Create tabs
tab1, tab2 = st.tabs(["Fetch Data", "Cached Data"])

with tab1:
    st.subheader("Fetch Market Data")

    col1, col2 = st.columns(2)

    with col1:
        # Exchange selector
        try:
            response = requests.get(f"{API_URL}/api/data/exchanges")
            if response.status_code == 200:
                exchanges = response.json()["exchanges"]
                exchange = st.selectbox("Exchange", exchanges, index=0)
            else:
                st.error("Failed to load exchanges")
                exchange = "US"
        except:
            st.error("Unable to connect to API")
            exchange = "US"

        # Symbol input methods
        input_method = st.radio("Symbol Input Method", ["Search", "Manual Entry"])

        if input_method == "Search":
            # Search symbols
            search_query = st.text_input("Search Symbol", placeholder="e.g., AAPL, MSFT")

            if search_query:
                try:
                    with st.spinner("Searching symbols..."):
                        response = requests.get(f"{API_URL}/api/data/symbols/{exchange}")
                        if response.status_code == 200:
                            symbols_data = response.json()
                            symbols_list = symbols_data["symbols"]

                            # Filter by search query
                            filtered = [
                                s for s in symbols_list
                                if search_query.upper() in s["code"].upper()
                                or (s.get("name") and search_query.upper() in s["name"].upper())
                            ]

                            if filtered:
                                st.write(f"Found {len(filtered)} matches:")
                                selected_symbols = st.multiselect(
                                    "Select Symbols",
                                    options=[s["code"] for s in filtered[:20]],
                                    format_func=lambda x: f"{x} - {next((s['name'] for s in filtered if s['code'] == x), '')}",
                                )
                            else:
                                st.warning("No symbols found")
                                selected_symbols = []
                        else:
                            st.error("Failed to fetch symbols")
                            selected_symbols = []
                except Exception as e:
                    st.error(f"Error: {e}")
                    selected_symbols = []
            else:
                selected_symbols = []
        else:
            # Manual entry
            symbols_text = st.text_area(
                "Enter symbols (one per line)",
                placeholder="AAPL\nMSFT\nGOOGL",
                height=100,
            )
            selected_symbols = [s.strip() for s in symbols_text.split("\n") if s.strip()]

    with col2:
        # Date range
        st.subheader("Date Range")
        end_date = st.date_input("End Date", value=datetime.now())
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=365),
        )

        # Data type
        data_type = st.selectbox(
            "Data Type",
            ["EOD (End of Day)", "Intraday (1h)"],
        )

        timeframe = "1D" if "EOD" in data_type else "1H"

    # Fetch button
    st.markdown("---")

    if selected_symbols:
        st.write(f"**Selected symbols ({len(selected_symbols)}):** {', '.join(selected_symbols[:10])}")
        if len(selected_symbols) > 10:
            st.caption(f"...and {len(selected_symbols) - 10} more")

        if st.button("🚀 Fetch Data", type="primary", use_container_width=True):
            with st.spinner("Fetching data..."):
                try:
                    # Submit fetch request
                    request_data = {
                        "symbols": selected_symbols,
                        "exchange": exchange,
                        "start_date": start_date.strftime("%Y-%m-%d"),
                        "end_date": end_date.strftime("%Y-%m-%d"),
                        "timeframe": timeframe,
                        "data_type": "eod" if "EOD" in data_type else "intraday",
                    }

                    response = requests.post(
                        f"{API_URL}/api/data/fetch",
                        json=request_data,
                    )

                    if response.status_code == 200:
                        result = response.json()
                        task_id = result["task_id"]

                        st.success(f"✅ Data fetch started! Task ID: {task_id}")

                        # Poll for status
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        for _ in range(60):  # Poll for up to 60 seconds
                            time.sleep(1)

                            status_response = requests.get(
                                f"{API_URL}/api/data/fetch/status/{task_id}"
                            )

                            if status_response.status_code == 200:
                                status_data = status_response.json()
                                progress = status_data["progress"]
                                status = status_data["status"]

                                progress_bar.progress(int(progress))
                                status_text.text(f"Status: {status} - {progress:.0f}%")

                                if status == "completed":
                                    st.success("✅ Data fetched successfully!")
                                    st.balloons()
                                    break
                                elif status == "failed":
                                    st.error(f"❌ Fetch failed: {status_data.get('error')}")
                                    break
                        else:
                            st.info("Fetch is taking longer than expected. Check back later.")

                    else:
                        st.error(f"Failed to start fetch: {response.text}")

                except Exception as e:
                    st.error(f"Error: {e}")
    else:
        st.info("👆 Select symbols to fetch data")

with tab2:
    st.subheader("Cached Data")
    st.info("🚧 Cached data browser coming soon! For now, use the API directly or check the database.")

    st.code("""
# Query cached data via API
import requests

response = requests.get("http://localhost:8000/api/data/symbols/US")
symbols = response.json()
    """)
