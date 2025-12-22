"""
Stock Market Dashboard
Interactive Streamlit dashboard for stock market analytics
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os

# Page configuration
st.set_page_config(
    page_title="Stock Market Analytics Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_database_connection():
    """Create database connection"""
    db_url = os.getenv(
        'DATABASE_URL',
        'postgresql://postgres:postgres@localhost:5432/stock_market'
    )
    return create_engine(db_url)

engine = get_database_connection()

# Data loading functions
@st.cache_data(ttl=300)
def load_portfolio_analytics():
    """Load portfolio analytics data"""
    try:
        query = """
        SELECT * FROM public.fct_portfolio_analytics
        ORDER BY return_20d DESC NULLS LAST
        """
        df = pd.read_sql(query, engine)
        
        if df.empty:
            st.warning("No portfolio analytics data found. Please run the data pipeline first.")
            return pd.DataFrame()
        
        # Ensure risk_category column exists (fallback if not in dbt model yet)
        if 'risk_category' not in df.columns:
            # Calculate risk_category from volatility
            df['risk_category'] = df['volatility_30d'].apply(
                lambda x: 'Low Risk' if x < 15 else ('Medium Risk' if x < 30 else 'High Risk')
                if pd.notna(x) else 'Medium Risk'
            )
        
        # Fill NaN values with defaults
        df = df.fillna({
            'return_1d': 0,
            'return_20d': 0,
            'return_ytd': 0,
            'return_90d': 0,
            'volatility_30d': 0,
            'sharpe_ratio': 0,
            'rsi_14': 50,
            'current_volume': 0,
            'avg_volume_30d': 0,
            'max_drawdown_20d': 0,
            'recommendation': 'Hold',
            'performance_grade': 'C',
            'risk_category': 'Medium Risk',
            'sector': 'Unknown',
            'company_name': 'Unknown'
        })
        return df
    except Exception as e:
        st.error(f"Error loading portfolio analytics: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_daily_performance(ticker=None, days=90):
    """Load daily performance data"""
    try:
        if ticker:
            query = f"""
            SELECT * FROM public.fct_daily_performance
            WHERE ticker = '{ticker}'
            ORDER BY date DESC
            LIMIT {days}
            """
        else:
            query = f"""
            SELECT * FROM public.fct_daily_performance
            WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY date DESC
            """
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading daily performance: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_technical_indicators(ticker, days=90):
    """Load technical indicators for a specific stock"""
    try:
        query = f"""
        SELECT * FROM public.fct_technical_indicators
        WHERE ticker = '{ticker}'
        AND date >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY date DESC
        """
        df = pd.read_sql(query, engine)
        
        # Fill missing technical indicator columns with defaults
        default_columns = {
            'close': 0, 'volume': 0, 'sma_20': 0, 'sma_50': 0,
            'bollinger_upper': 0, 'bollinger_lower': 0,
            'rsi_14': 50, 'macd': 0, 'daily_return_pct': 0,
            'volatility_30d': 0, 'trend_signal': 'Neutral', 'rsi_signal': 'Neutral'
        }
        for col, default in default_columns.items():
            if col not in df.columns:
                df[col] = default
        
        df = df.fillna(0)
        return df
    except Exception as e:
        st.error(f"Error loading technical indicators: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_available_tickers():
    """Get list of available tickers"""
    try:
        query = "SELECT DISTINCT ticker, company_name FROM public.stg_stock_metadata ORDER BY ticker"
        df = pd.read_sql(query, engine)
        if df.empty:
            # Return default tickers if table is empty
            return pd.DataFrame({
                'ticker': ['AAPL', 'MSFT', 'GOOGL'],
                'company_name': ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.']
            })
        return df
    except Exception as e:
        st.error(f"Error loading tickers: {str(e)}")
        return pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'company_name': ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.']
        })

# Dashboard Header
st.title("📈 Stock Market Analytics Dashboard")
st.markdown("### Real-time stock market data with technical analysis")

# Sidebar
st.sidebar.header("Dashboard Controls")

# Refresh data button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Date range selector
days_back = st.sidebar.selectbox(
    "Time Period",
    options=[30, 60, 90, 180, 365],
    index=2,
    format_func=lambda x: f"Last {x} days"
)

# Load data
with st.spinner("Loading portfolio data..."):
    portfolio_df = load_portfolio_analytics()
    tickers_df = get_available_tickers()

# Main Dashboard Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Portfolio Overview",
    "📈 Stock Analysis", 
    "🏆 Top Performers",
    "📉 Risk Analysis"
])

# Tab 1: Portfolio Overview
with tab1:
    st.header("Portfolio Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Fill NaN values for display
    portfolio_df = portfolio_df.fillna(0)
    
    with col1:
        avg_return = portfolio_df['return_20d'].mean()
        st.metric(
            "Avg 20-Day Return",
            f"{avg_return:.2f}%",
            delta=f"{avg_return:.2f}%"
        )
    
    with col2:
        top_performer = portfolio_df.iloc[0]
        return_val = top_performer['return_20d'] if top_performer['return_20d'] is not None else 0
        st.metric(
            "Top Performer",
            top_performer['ticker'],
            delta=f"{return_val:.2f}%"
        )
    
    with col3:
        avg_volatility = portfolio_df['volatility_30d'].mean()
        st.metric(
            "Avg Volatility",
            f"{avg_volatility:.2f}%"
        )
    
    with col4:
        strong_buys = len(portfolio_df[portfolio_df['recommendation'] == 'Strong Buy'])
        st.metric(
            "Strong Buy Signals",
            strong_buys
        )
    
    # Sector Performance
    st.subheader("Sector Performance")
    sector_perf = portfolio_df.groupby('sector').agg({
        'return_20d': 'mean',
        'volatility_30d': 'mean',
        'ticker': 'count'
    }).reset_index()
    sector_perf.columns = ['Sector', 'Avg Return (%)', 'Avg Volatility (%)', 'Stock Count']
    
    fig_sector = px.bar(
        sector_perf,
        x='Sector',
        y='Avg Return (%)',
        color='Avg Return (%)',
        color_continuous_scale=['red', 'yellow', 'green'],
        title="Average 20-Day Return by Sector"
    )
    st.plotly_chart(fig_sector, use_container_width=True)
    
    # Portfolio Table
    st.subheader("Portfolio Holdings")
    display_cols = [
        'ticker', 'company_name', 'current_price', 'return_1d', 'return_20d',
        'volatility_30d', 'rsi_14', 'recommendation', 'performance_grade'
    ]
    
    # Format the dataframe
    portfolio_display = portfolio_df[display_cols].copy()
    portfolio_display = portfolio_display.fillna(0)
    portfolio_display['current_price'] = portfolio_display['current_price'].apply(lambda x: f"${x:.2f}" if x else "$0.00")
    portfolio_display['return_1d'] = portfolio_display['return_1d'].apply(lambda x: f"{x:.2f}%" if x else "0.00%")
    portfolio_display['return_20d'] = portfolio_display['return_20d'].apply(lambda x: f"{x:.2f}%" if x else "0.00%")
    portfolio_display['volatility_30d'] = portfolio_display['volatility_30d'].apply(lambda x: f"{x:.2f}%" if x else "0.00%")
    portfolio_display['rsi_14'] = portfolio_display['rsi_14'].apply(lambda x: f"{x:.1f}" if x else "0.0")
    
    st.dataframe(
        portfolio_display,
        use_container_width=True,
        height=400
    )

# Tab 2: Stock Analysis
with tab2:
    st.header("Individual Stock Analysis")
    
    # Stock selector
    selected_ticker = st.selectbox(
        "Select Stock",
        options=tickers_df['ticker'].tolist(),
        format_func=lambda x: f"{x} - {tickers_df[tickers_df['ticker']==x]['company_name'].values[0]}"
    )
    
    if selected_ticker:
        # Load stock data
        tech_data = load_technical_indicators(selected_ticker, days_back)
        
        if not tech_data.empty:
            # Stock info
            stock_info = portfolio_df[portfolio_df['ticker'] == selected_ticker].iloc[0]
            
            # Handle None values
            current_price = stock_info['current_price'] or 0
            return_1d = stock_info['return_1d'] or 0
            return_20d = stock_info['return_20d'] or 0
            rsi_14 = stock_info['rsi_14'] or 0
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Current Price", f"${current_price:.2f}")
            with col2:
                st.metric("1-Day Return", f"{return_1d:.2f}%", delta=f"{return_1d:.2f}%")
            with col3:
                st.metric("20-Day Return", f"{return_20d:.2f}%", delta=f"{return_20d:.2f}%")
            with col4:
                st.metric("RSI", f"{rsi_14:.1f}")
            with col5:
                st.metric("Recommendation", stock_info['recommendation'] or "N/A")
            
            # Price chart with moving averages
            st.subheader("Price Chart & Moving Averages")
            
            fig_price = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.5, 0.25, 0.25],
                subplot_titles=('Price & Moving Averages', 'Volume', 'RSI')
            )
            
            # Candlestick chart
            fig_price.add_trace(
                go.Scatter(
                    x=tech_data['date'],
                    y=tech_data['close'],
                    name='Close Price',
                    line=dict(color='blue', width=2)
                ),
                row=1, col=1
            )
            
            # Moving averages
            fig_price.add_trace(
                go.Scatter(
                    x=tech_data['date'],
                    y=tech_data['sma_20'],
                    name='SMA 20',
                    line=dict(color='orange', width=1)
                ),
                row=1, col=1
            )
            
            fig_price.add_trace(
                go.Scatter(
                    x=tech_data['date'],
                    y=tech_data['sma_50'],
                    name='SMA 50',
                    line=dict(color='red', width=1)
                ),
                row=1, col=1
            )
            
            # Bollinger Bands
            fig_price.add_trace(
                go.Scatter(
                    x=tech_data['date'],
                    y=tech_data['bollinger_upper'],
                    name='BB Upper',
                    line=dict(color='gray', width=1, dash='dash'),
                    showlegend=False
                ),
                row=1, col=1
            )
            
            fig_price.add_trace(
                go.Scatter(
                    x=tech_data['date'],
                    y=tech_data['bollinger_lower'],
                    name='BB Lower',
                    line=dict(color='gray', width=1, dash='dash'),
                    fill='tonexty',
                    fillcolor='rgba(128,128,128,0.1)',
                    showlegend=False
                ),
                row=1, col=1
            )
            
            # Volume - color based on daily return
            colors = ['green' if row['daily_return_pct'] >= 0 else 'red' 
                     for idx, row in tech_data.iterrows()]
            
            fig_price.add_trace(
                go.Bar(
                    x=tech_data['date'],
                    y=tech_data['volume'],
                    name='Volume',
                    marker_color=colors
                ),
                row=2, col=1
            )
            
            # RSI
            fig_price.add_trace(
                go.Scatter(
                    x=tech_data['date'],
                    y=tech_data['rsi_14'],
                    name='RSI',
                    line=dict(color='purple', width=2)
                ),
                row=3, col=1
            )
            
            # RSI reference lines
            fig_price.add_hline(y=70, line_dash="dash", line_color="red", row=3, col=1)
            fig_price.add_hline(y=30, line_dash="dash", line_color="green", row=3, col=1)
            fig_price.update_xaxes(title_text="Date", row=3, col=1)
            fig_price.update_yaxes(title_text="Price ($)", row=1, col=1)
            fig_price.update_yaxes(title_text="Volume", row=2, col=1)
            fig_price.update_yaxes(title_text="RSI", row=3, col=1)
            
            fig_price.update_layout(height=800, showlegend=True)
            
            st.plotly_chart(fig_price, use_container_width=True)
            
            # Technical Indicators Table
            st.subheader("Recent Technical Indicators")
            recent_data = tech_data.head(10)[
                ['date', 'close', 'sma_20', 'sma_50', 'rsi_14', 'macd',
                 'volatility_30d', 'trend_signal', 'rsi_signal']
            ].copy()
            
            st.dataframe(recent_data, use_container_width=True)
        else:
            st.warning(f"No data available for {selected_ticker}")

# Tab 3: Top Performers
with tab3:
    st.header("Top Performers & Movers")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top Gainers (20-Day)")
        top_gainers = portfolio_df.nlargest(10, 'return_20d')[
            ['ticker', 'company_name', 'return_20d', 'current_price', 'recommendation']
        ]
        
        fig_gainers = px.bar(
            top_gainers,
            x='ticker',
            y='return_20d',
            color='return_20d',
            color_continuous_scale='Greens',
            title="Top 10 Gainers"
        )
        st.plotly_chart(fig_gainers, use_container_width=True)
        st.dataframe(top_gainers, use_container_width=True)
    
    with col2:
        st.subheader("📉 Top Losers (20-Day)")
        top_losers = portfolio_df.nsmallest(10, 'return_20d')[
            ['ticker', 'company_name', 'return_20d', 'current_price', 'recommendation']
        ]
        
        fig_losers = px.bar(
            top_losers,
            x='ticker',
            y='return_20d',
            color='return_20d',
            color_continuous_scale='Reds_r',
            title="Top 10 Losers"
        )
        st.plotly_chart(fig_losers, use_container_width=True)
        st.dataframe(top_losers, use_container_width=True)
    
    # Recommendations
    st.subheader("Investment Recommendations")
    
    rec_col1, rec_col2, rec_col3 = st.columns(3)
    
    with rec_col1:
        strong_buys = portfolio_df[portfolio_df['recommendation'] == 'Strong Buy'][
            ['ticker', 'company_name', 'return_20d', 'rsi_14', 'performance_grade']
        ]
        st.markdown("#### 🟢 Strong Buy")
        st.dataframe(strong_buys, use_container_width=True)
    
    with rec_col2:
        buys = portfolio_df[portfolio_df['recommendation'] == 'Buy'][
            ['ticker', 'company_name', 'return_20d', 'rsi_14', 'performance_grade']
        ]
        st.markdown("#### 🔵 Buy")
        st.dataframe(buys, use_container_width=True)
    
    with rec_col3:
        sells = portfolio_df[portfolio_df['recommendation'].isin(['Sell', 'Strong Sell'])][
            ['ticker', 'company_name', 'return_20d', 'rsi_14', 'performance_grade']
        ]
        st.markdown("#### 🔴 Sell")
        st.dataframe(sells, use_container_width=True)

# Tab 4: Risk Analysis
with tab4:
    st.header("Risk Analysis")
    
    # Risk classification
    st.subheader("Risk Distribution")
    
    # Ensure risk_category exists and handle empty data
    if 'risk_category' in portfolio_df.columns and not portfolio_df['risk_category'].isna().all():
        risk_dist = portfolio_df['risk_category'].value_counts().reset_index()
        risk_dist.columns = ['Risk Category', 'Count']
    else:
        # Fallback: calculate risk_category from volatility
        portfolio_df['risk_category'] = portfolio_df['volatility_30d'].apply(
            lambda x: 'Low Risk' if x < 15 else ('Medium Risk' if x < 30 else 'High Risk')
            if pd.notna(x) else 'Medium Risk'
        )
        risk_dist = portfolio_df['risk_category'].value_counts().reset_index()
        risk_dist.columns = ['Risk Category', 'Count']
    
    fig_risk = px.pie(
        risk_dist,
        values='Count',
        names='Risk Category',
        title="Portfolio Risk Distribution",
        color_discrete_map={
            'Low Risk': 'green',
            'Medium Risk': 'yellow',
            'High Risk': 'red'
        }
    )
    st.plotly_chart(fig_risk, use_container_width=True)
    
    # Volatility vs Return scatter
    st.subheader("Risk-Return Analysis")
    
    fig_scatter = px.scatter(
        portfolio_df,
        x='volatility_30d',
        y='return_20d',
        size='current_volume',
        color='sector',
        hover_data=['ticker', 'company_name'],
        title="Volatility vs Return (20-Day)",
        labels={
            'volatility_30d': '30-Day Volatility (%)',
            'return_20d': '20-Day Return (%)'
        }
    )
    
    # Add quadrant lines
    fig_scatter.add_hline(y=0, line_dash="dash", line_color="gray")
    fig_scatter.add_vline(x=portfolio_df['volatility_30d'].median(), 
                         line_dash="dash", line_color="gray")
    
    st.plotly_chart(fig_scatter, use_container_width=True)
    
    # High risk stocks
    st.subheader("High Risk Stocks")
    high_risk = portfolio_df[portfolio_df['volatility_30d'] > portfolio_df['volatility_30d'].quantile(0.75)][
        ['ticker', 'company_name', 'volatility_30d', 'return_20d', 
         'sharpe_ratio', 'max_drawdown_20d', 'risk_category']
    ].sort_values('volatility_30d', ascending=False)
    
    st.dataframe(high_risk, use_container_width=True)
    
    # Sharpe Ratio Analysis
    st.subheader("Risk-Adjusted Returns (Sharpe Ratio)")
    
    sharpe_data = portfolio_df.nlargest(15, 'sharpe_ratio')[
        ['ticker', 'company_name', 'sharpe_ratio', 'return_20d', 'volatility_30d']
    ]
    
    fig_sharpe = px.bar(
        sharpe_data,
        x='ticker',
        y='sharpe_ratio',
        color='sharpe_ratio',
        color_continuous_scale='RdYlGn',
        title="Top 15 Stocks by Sharpe Ratio"
    )
    st.plotly_chart(fig_sharpe, use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Stock Market Analytics Dashboard | Data updated daily via Airflow | 
        Built with Streamlit, dbt, PostgreSQL</p>
    </div>
    """,
    unsafe_allow_html=True
)
