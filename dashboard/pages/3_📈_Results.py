"""Results page - View backtest results and analysis."""

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Results", page_icon="📈", layout="wide")

API_URL = st.session_state.get("api_url", "http://localhost:8000")

st.title("📈 Backtest Results")
st.markdown("Analyze backtest performance and trades")

# Load backtests list
try:
    response = requests.get(f"{API_URL}/api/backtest/list?limit=50")
    if response.status_code == 200:
        backtests = response.json()

        if not backtests:
            st.info("No backtests found. Run your first backtest!")
            st.stop()

        # Backtest selector
        backtest_options = {
            f"{bt['name']} ({bt['status']})": bt['id']
            for bt in backtests
        }

        # Use last backtest if available
        default_selection = None
        if "last_backtest_id" in st.session_state:
            for name, bt_id in backtest_options.items():
                if bt_id == st.session_state.last_backtest_id:
                    default_selection = name
                    break

        selected_name = st.selectbox(
            "Select Backtest",
            options=list(backtest_options.keys()),
            index=list(backtest_options.keys()).index(default_selection) if default_selection else 0,
        )

        backtest_id = backtest_options[selected_name]

        # Load backtest details
        response = requests.get(f"{API_URL}/api/backtest/{backtest_id}")
        if response.status_code == 200:
            backtest = response.json()

            # Show backtest info
            st.subheader("Backtest Information")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Strategy", backtest["strategy_name"])
            col2.metric("Exchange", backtest["exchange"])
            col3.metric("Timeframe", backtest["timeframe"])
            col4.metric("Symbols", len(backtest["symbols"]))

            st.caption(f"Period: {backtest['start_date'][:10]} to {backtest['end_date'][:10]}")

            if backtest["status"] != "completed":
                st.warning(f"Backtest status: {backtest['status']}")
                if backtest.get("error"):
                    st.error(f"Error: {backtest['error']}")
                st.stop()

            # Performance metrics
            st.markdown("---")
            st.subheader("Performance Metrics")

            metrics = backtest.get("metrics", {})

            col1, col2, col3, col4, col5 = st.columns(5)

            col1.metric(
                "Sharpe Ratio",
                f"{metrics.get('sharpe_ratio', 0):.2f}",
                help="Risk-adjusted return metric. > 1 is good, > 2 is excellent.",
            )

            total_return = metrics.get('total_return', 0)
            col2.metric(
                "Total Return",
                f"{total_return:.2f}%",
                delta=f"{total_return:.2f}%",
            )

            col3.metric(
                "Max Drawdown",
                f"{metrics.get('max_drawdown', 0):.2f}%",
                help="Maximum peak-to-trough decline",
            )

            col4.metric(
                "Number of Trades",
                metrics.get('num_trades', 0),
            )

            col5.metric(
                "Win Rate",
                f"{metrics.get('win_rate', 0):.1f}%",
            )

            # Additional metrics
            with st.expander("📊 Additional Metrics"):
                col1, col2, col3 = st.columns(3)
                col1.metric("Avg Trade", f"{metrics.get('avg_trade', 0):.2f}%")
                col2.metric("Profit Factor", f"{metrics.get('profit_factor', 0):.2f}")
                col3.metric("Expectancy", f"{metrics.get('expectancy', 0):.2f}%")

            # Charts
            st.markdown("---")
            st.subheader("Equity Curve")

            results = backtest.get("results", {})
            equity_curve = results.get("equity_curve", {})

            if equity_curve and equity_curve.get("dates"):
                # Create subplot with equity and drawdown
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.05,
                    subplot_titles=("Equity Curve", "Drawdown"),
                    row_heights=[0.7, 0.3],
                )

                # Equity curve
                fig.add_trace(
                    go.Scatter(
                        x=equity_curve["dates"],
                        y=equity_curve["equity"],
                        mode="lines",
                        name="Equity",
                        line=dict(color="#2E86AB", width=2),
                    ),
                    row=1, col=1,
                )

                # Drawdown
                if equity_curve.get("drawdown"):
                    fig.add_trace(
                        go.Scatter(
                            x=equity_curve["dates"],
                            y=equity_curve["drawdown"],
                            mode="lines",
                            name="Drawdown",
                            fill="tozeroy",
                            line=dict(color="#A23B72", width=1),
                        ),
                        row=2, col=1,
                    )

                fig.update_xaxes(title_text="Date", row=2, col=1)
                fig.update_yaxes(title_text="Equity ($)", row=1, col=1)
                fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1)

                fig.update_layout(
                    height=600,
                    showlegend=True,
                    hovermode="x unified",
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Equity curve data not available")

            # Trades table
            st.markdown("---")
            st.subheader("Trade History")

            trades = results.get("trades", [])

            if trades:
                st.write(f"**Total trades:** {len(trades)}")

                # Convert to DataFrame
                trades_df = pd.DataFrame(trades)

                # Show summary stats
                with st.expander("📊 Trade Statistics"):
                    if "PnL" in trades_df.columns or "ReturnPct" in trades_df.columns:
                        col1, col2, col3, col4 = st.columns(4)

                        pnl_col = "PnL" if "PnL" in trades_df.columns else "ReturnPct"

                        winning_trades = trades_df[trades_df[pnl_col] > 0]
                        losing_trades = trades_df[trades_df[pnl_col] < 0]

                        col1.metric("Winning Trades", len(winning_trades))
                        col2.metric("Losing Trades", len(losing_trades))
                        col3.metric("Avg Win", f"{winning_trades[pnl_col].mean():.2f}%" if len(winning_trades) > 0 else "N/A")
                        col4.metric("Avg Loss", f"{losing_trades[pnl_col].mean():.2f}%" if len(losing_trades) > 0 else "N/A")

                # Display trades table
                st.dataframe(
                    trades_df,
                    use_container_width=True,
                    height=400,
                )

                # Download button
                csv = trades_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Trades (CSV)",
                    data=csv,
                    file_name=f"backtest_{backtest_id}_trades.csv",
                    mime="text/csv",
                )
            else:
                st.info("No trades executed in this backtest")

            # Strategy parameters
            with st.expander("⚙️ Strategy Parameters"):
                st.json(backtest.get("strategy_params", {}))

        else:
            st.error("Failed to load backtest details")
    else:
        st.error("Failed to load backtests list")
except Exception as e:
    st.error(f"Error connecting to API: {e}")
