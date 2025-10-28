"""Report generation with plots and explanations."""

from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from loguru import logger

try:
    import shap
except ImportError:
    shap = None
    logger.warning("SHAP not available")

from src.backtest.runner import BacktestResult


class ReportGenerator:
    """
    Generate comprehensive HTML and Markdown reports.

    Includes:
    - Performance charts
    - Factor analysis
    - Model explanations (SHAP)
    - Exposure analysis
    """

    def __init__(self, out_dir: str = "reports/latest"):
        """
        Initialize report generator.

        Args:
            out_dir: Output directory for reports
        """
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized ReportGenerator at {self.out_dir}")

    def generate_full_report(
        self,
        backtest_result: BacktestResult,
        cv_results: Optional[pd.DataFrame] = None,
        model: Optional[any] = None,
        feature_importance: Optional[pd.DataFrame] = None,
        config: Optional[Dict] = None,
        X_train: Optional[pd.DataFrame] = None,
    ) -> str:
        """
        Generate complete HTML report.

        Args:
            backtest_result: Backtest results
            cv_results: Cross-validation results
            model: Trained model (for SHAP)
            feature_importance: Feature importance DataFrame
            config: Configuration dictionary
            X_train: Training features (for SHAP)

        Returns:
            Path to generated report
        """
        logger.info("Generating full report...")

        html_parts = []

        # Header
        html_parts.append(self._generate_header(config))

        # Executive summary
        html_parts.append(self._generate_summary(backtest_result))

        # Performance charts
        html_parts.append(self._generate_performance_section(backtest_result))

        # Cross-validation results
        if cv_results is not None:
            html_parts.append(self._generate_cv_section(cv_results))

        # Feature importance
        if feature_importance is not None:
            html_parts.append(self._generate_feature_importance_section(feature_importance))

        # SHAP analysis
        if shap is not None and model is not None and X_train is not None:
            html_parts.append(self._generate_shap_section(model, X_train))

        # Exposure analysis
        html_parts.append(self._generate_exposure_section(backtest_result))

        # Footer
        html_parts.append(self._generate_footer())

        # Combine and write
        html_content = "\n".join(html_parts)
        report_path = self.out_dir / "backtest_report.html"

        with open(report_path, "w") as f:
            f.write(html_content)

        logger.info(f"Report generated: {report_path}")

        # Also generate markdown summary
        self._generate_markdown_summary(backtest_result, cv_results)

        return str(report_path)

    def _generate_header(self, config: Optional[Dict]) -> str:
        """Generate HTML header."""
        title = "Quant Equity Alpha Platform - Backtest Report"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 40px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: auto;
                    background-color: white;
                    padding: 30px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #2c3e50;
                    border-bottom: 3px solid #3498db;
                    padding-bottom: 10px;
                }}
                h2 {{
                    color: #34495e;
                    margin-top: 30px;
                    border-bottom: 2px solid #ecf0f1;
                    padding-bottom: 5px;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin: 20px 0;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                }}
                th {{
                    background-color: #3498db;
                    color: white;
                    font-weight: bold;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                .metric-good {{
                    color: #27ae60;
                    font-weight: bold;
                }}
                .metric-bad {{
                    color: #e74c3c;
                    font-weight: bold;
                }}
                .timestamp {{
                    color: #7f8c8d;
                    font-style: italic;
                }}
                img {{
                    max-width: 100%;
                    height: auto;
                    margin: 20px 0;
                }}
            </style>
        </head>
        <body>
        <div class="container">
        <h1>{title}</h1>
        <p class="timestamp">Generated: {timestamp}</p>
        """

        return html

    def _generate_footer(self) -> str:
        """Generate HTML footer."""
        return """
        </div>
        </body>
        </html>
        """

    def _generate_summary(self, result: BacktestResult) -> str:
        """Generate executive summary section."""
        metrics = result.metrics

        # Format key metrics
        total_return = f"{metrics.get('total_return', 0) * 100:.2f}%"
        cagr = f"{metrics.get('cagr', 0) * 100:.2f}%"
        sharpe = f"{metrics.get('sharpe', 0):.2f}"
        max_dd = f"{metrics.get('max_drawdown', 0) * 100:.2f}%"

        html = f"""
        <h2>Executive Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Total Return</td><td>{total_return}</td></tr>
            <tr><td>CAGR</td><td>{cagr}</td></tr>
            <tr><td>Sharpe Ratio</td><td>{sharpe}</td></tr>
            <tr><td>Max Drawdown</td><td>{max_dd}</td></tr>
            <tr><td>Hit Rate</td><td>{metrics.get('hit_rate', 0) * 100:.1f}%</td></tr>
            <tr><td>Avg Turnover / Rebalance</td><td>{metrics.get('avg_turnover_per_rebalance', 0):.2f}</td></tr>
        </table>
        """

        return html

    def _generate_performance_section(self, result: BacktestResult) -> str:
        """Generate performance charts section."""
        # Equity curve
        fig_equity = self._plot_equity_curve(result.portfolio_values, result.returns)
        equity_path = self.out_dir / "equity_curve.png"
        fig_equity.savefig(equity_path, dpi=150, bbox_inches="tight")
        plt.close(fig_equity)

        # Drawdown chart
        fig_dd = self._plot_drawdown(result.returns)
        dd_path = self.out_dir / "drawdown.png"
        fig_dd.savefig(dd_path, dpi=150, bbox_inches="tight")
        plt.close(fig_dd)

        # Returns distribution
        fig_dist = self._plot_returns_distribution(result.returns)
        dist_path = self.out_dir / "returns_distribution.png"
        fig_dist.savefig(dist_path, dpi=150, bbox_inches="tight")
        plt.close(fig_dist)

        html = f"""
        <h2>Performance Analysis</h2>
        <h3>Equity Curve</h3>
        <img src="equity_curve.png" alt="Equity Curve">
        <h3>Drawdown</h3>
        <img src="drawdown.png" alt="Drawdown">
        <h3>Returns Distribution</h3>
        <img src="returns_distribution.png" alt="Returns Distribution">
        """

        return html

    def _plot_equity_curve(self, portfolio_values: pd.Series, returns: pd.Series) -> plt.Figure:
        """Plot equity curve."""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

        # Portfolio value
        ax1.plot(portfolio_values.index, portfolio_values.values, linewidth=2, color="#3498db")
        ax1.set_title("Portfolio Value", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Value ($)", fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${x/1e6:.1f}M"))

        # Cumulative returns
        cum_returns = (1 + returns).cumprod()
        ax2.plot(cum_returns.index, cum_returns.values, linewidth=2, color="#27ae60")
        ax2.set_title("Cumulative Returns", fontsize=14, fontweight="bold")
        ax2.set_ylabel("Cumulative Return", fontsize=12)
        ax2.set_xlabel("Date", fontsize=12)
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def _plot_drawdown(self, returns: pd.Series) -> plt.Figure:
        """Plot drawdown chart."""
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.fill_between(drawdown.index, drawdown.values, 0, color="#e74c3c", alpha=0.3)
        ax.plot(drawdown.index, drawdown.values, linewidth=2, color="#c0392b")
        ax.set_title("Drawdown", fontsize=14, fontweight="bold")
        ax.set_ylabel("Drawdown", fontsize=12)
        ax.set_xlabel("Date", fontsize=12)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x*100:.1f}%"))
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def _plot_returns_distribution(self, returns: pd.Series) -> plt.Figure:
        """Plot returns distribution."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # Histogram
        ax1.hist(returns, bins=50, color="#3498db", alpha=0.7, edgecolor="black")
        ax1.axvline(returns.mean(), color="#e74c3c", linestyle="--", linewidth=2, label="Mean")
        ax1.axvline(returns.median(), color="#27ae60", linestyle="--", linewidth=2, label="Median")
        ax1.set_title("Returns Distribution", fontsize=14, fontweight="bold")
        ax1.set_xlabel("Return", fontsize=12)
        ax1.set_ylabel("Frequency", fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # Q-Q plot
        from scipy import stats
        stats.probplot(returns, dist="norm", plot=ax2)
        ax2.set_title("Q-Q Plot", fontsize=14, fontweight="bold")
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    def _generate_cv_section(self, cv_results: pd.DataFrame) -> str:
        """Generate cross-validation section."""
        # CV metrics table
        summary = cv_results[["fold", "test_ic", "test_rank_ic", "test_mse", "train_samples", "test_samples"]].copy()
        summary_html = summary.to_html(index=False, classes="table")

        # IC over time chart
        fig = plt.figure(figsize=(12, 5))
        plt.plot(cv_results["fold"], cv_results["test_ic"], marker="o", linewidth=2, markersize=8, label="Test IC")
        plt.axhline(cv_results["test_ic"].mean(), color="red", linestyle="--", label=f"Mean: {cv_results['test_ic'].mean():.4f}")
        plt.title("Information Coefficient by Fold", fontsize=14, fontweight="bold")
        plt.xlabel("Fold", fontsize=12)
        plt.ylabel("IC", fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)

        cv_chart_path = self.out_dir / "cv_ic.png"
        plt.savefig(cv_chart_path, dpi=150, bbox_inches="tight")
        plt.close()

        html = f"""
        <h2>Cross-Validation Results</h2>
        <p><strong>Mean Test IC:</strong> {cv_results['test_ic'].mean():.4f}</p>
        <p><strong>Mean Rank IC:</strong> {cv_results['test_rank_ic'].mean():.4f}</p>
        {summary_html}
        <img src="cv_ic.png" alt="CV IC Chart">
        """

        return html

    def _generate_feature_importance_section(self, feature_importance: pd.DataFrame) -> str:
        """Generate feature importance section."""
        # Top 20 features
        top_features = feature_importance.head(20)

        fig, ax = plt.subplots(figsize=(10, 8))
        ax.barh(top_features["feature"], top_features["importance"], color="#3498db")
        ax.set_title("Top 20 Feature Importances", fontsize=14, fontweight="bold")
        ax.set_xlabel("Importance", fontsize=12)
        ax.invert_yaxis()
        ax.grid(True, alpha=0.3, axis="x")

        importance_path = self.out_dir / "feature_importance.png"
        plt.savefig(importance_path, dpi=150, bbox_inches="tight")
        plt.close()

        html = f"""
        <h2>Feature Importance</h2>
        <img src="feature_importance.png" alt="Feature Importance">
        """

        return html

    def _generate_shap_section(self, model: any, X_train: pd.DataFrame) -> str:
        """Generate SHAP analysis section."""
        if shap is None:
            return "<h2>SHAP Analysis</h2><p>SHAP not available</p>"

        try:
            # Sample data for SHAP (use subset for speed)
            X_sample = X_train.sample(min(1000, len(X_train)), random_state=42)

            # Create explainer
            explainer = shap.Explainer(model, X_sample)
            shap_values = explainer(X_sample)

            # Summary plot
            shap.summary_plot(shap_values, X_sample, show=False)
            shap_path = self.out_dir / "shap_summary.png"
            plt.savefig(shap_path, dpi=150, bbox_inches="tight")
            plt.close()

            html = f"""
            <h2>SHAP Analysis</h2>
            <p>SHAP (SHapley Additive exPlanations) values show feature contributions to model predictions.</p>
            <img src="shap_summary.png" alt="SHAP Summary">
            """

            return html

        except Exception as e:
            logger.warning(f"SHAP analysis failed: {e}")
            return f"<h2>SHAP Analysis</h2><p>SHAP analysis failed: {e}</p>"

    def _generate_exposure_section(self, result: BacktestResult) -> str:
        """Generate exposure analysis section."""
        html = """
        <h2>Exposure Analysis</h2>
        <p>Turnover and cost analysis</p>
        """

        if not result.costs_history.empty:
            fig, ax = plt.subplots(figsize=(12, 5))
            ax.plot(result.costs_history.index, result.costs_history.values, marker="o", linewidth=2)
            ax.set_title("Transaction Costs Over Time", fontsize=14, fontweight="bold")
            ax.set_ylabel("Cost ($)", fontsize=12)
            ax.set_xlabel("Date", fontsize=12)
            ax.grid(True, alpha=0.3)

            costs_path = self.out_dir / "costs_over_time.png"
            plt.savefig(costs_path, dpi=150, bbox_inches="tight")
            plt.close()

            html += '<img src="costs_over_time.png" alt="Costs Over Time">'

        return html

    def _generate_markdown_summary(
        self,
        backtest_result: BacktestResult,
        cv_results: Optional[pd.DataFrame] = None,
    ) -> None:
        """Generate markdown summary."""
        metrics = backtest_result.metrics

        md = f"""# Backtest Summary

## Performance Metrics

- **Total Return**: {metrics.get('total_return', 0) * 100:.2f}%
- **CAGR**: {metrics.get('cagr', 0) * 100:.2f}%
- **Sharpe Ratio**: {metrics.get('sharpe', 0):.2f}
- **Sortino Ratio**: {metrics.get('sortino', 0):.2f}
- **Max Drawdown**: {metrics.get('max_drawdown', 0) * 100:.2f}%
- **Hit Rate**: {metrics.get('hit_rate', 0) * 100:.1f}%

## Trading Metrics

- **Number of Rebalances**: {metrics.get('num_rebalances', 0):.0f}
- **Total Costs**: ${metrics.get('total_costs', 0):,.0f}
- **Avg Turnover / Rebalance**: {metrics.get('avg_turnover_per_rebalance', 0):.2f}

"""

        if cv_results is not None:
            md += f"""
## Cross-Validation Results

- **Mean Test IC**: {cv_results['test_ic'].mean():.4f}
- **Mean Rank IC**: {cv_results['test_rank_ic'].mean():.4f}
- **Number of Folds**: {len(cv_results)}

"""

        md_path = self.out_dir / "summary.md"
        with open(md_path, "w") as f:
            f.write(md)

        logger.info(f"Markdown summary generated: {md_path}")
