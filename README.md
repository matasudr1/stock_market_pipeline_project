# 📈 Stock Market Data Pipeline

A production-ready, end-to-end data pipeline for stock market analytics leveraging financial data from Alpha Vantage API. Built with modern data engineering best practices, this project demonstrates ETL orchestration, data transformation, and interactive visualization.

![Python](https://img.shields.io/badge/python-3.11-blue)
![Airflow](https://img.shields.io/badge/airflow-2.8.0-orange)
![dbt](https://img.shields.io/badge/dbt-1.7.4-green)
![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue)
![Docker](https://img.shields.io/badge/docker-compose-blue)

## 🎯 Project Overview

This pipeline extracts stock market data, calculates sophisticated technical indicators, and provides actionable investment insights through an interactive dashboard.

### Key Features

- **Real-time Data Extraction**: Automated daily stock price collection via Alpha Vantage API
- **Advanced Analytics**: Moving averages (SMA/EMA), RSI, MACD, Bollinger Bands, volatility metrics
- **Scalable Architecture**: Containerized services with Docker Compose
- **Data Quality**: Comprehensive validation and testing with dbt
- **Interactive Dashboard**: Real-time Streamlit visualization with 4 analytical views

## 🏗️ Architecture
<img width="3712" height="1152" alt="Gemini_Generated_Image_m3kyokm3kyokm3ky" src="https://github.com/user-attachments/assets/0f5c17cf-6741-44be-b8b9-3e5ff581587a" />

### Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Extraction** | Alpha Vantage API, Python | Fetch stock data from Alpha Vantage |
| **Orchestration** | Apache Airflow | Schedule and monitor pipeline runs |
| **Storage** | PostgreSQL | Store raw and transformed data |
| **Transformation** | dbt | Calculate technical indicators & metrics |
| **Visualization** | Streamlit, Plotly | Interactive analytics dashboard |
| **Containerization** | Docker, Docker Compose | Service orchestration |

## 📊 Data Pipeline

### 1. Extract (Ingestion Service)
- Fetches OHLCV data for configurable stock tickers
- Supports incremental and full backfill loads
- Error handling and retry logic
- Pipeline execution logging

### 2. Transform (dbt Models)

**Staging Models:**
- `stg_stock_prices`: Clean raw price data with quality checks
- `stg_stock_metadata`: Company and ticker reference data

**Mart Models:**
- `fct_technical_indicators`: Calculate 15+ technical indicators
  - Simple Moving Averages (20, 50, 200-day)
  - Exponential Moving Averages (12, 26-day)
  - MACD (Moving Average Convergence Divergence)
  - RSI (Relative Strength Index)
  - Bollinger Bands
  - Historical Volatility (30-day)
  - Price momentum metrics
  
- `fct_daily_performance`: Daily stock rankings and signals
  - Performance rankings (volume, gains, volatility)
  - YTD returns
  - Trading signals (Buy/Sell/Hold)
  - Risk categorization
  
- `fct_portfolio_analytics`: Portfolio-level insights
  - Period returns (1D, 20D, 90D, YTD)
  - Sharpe ratio (risk-adjusted returns)
  - Sector performance benchmarking
  - Investment recommendations

### 3. Load & Serve
- PostgreSQL with optimized indexes
- Streamlit dashboard with 4 analytical views
- Real-time data refresh

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Git
- 8GB RAM minimum

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/stock-market-pipeline.git
cd stock-market-pipeline
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. **Build and start services**
```bash
make build
make up
```

4. **Access the applications**
- Airflow UI: http://localhost:8080 (admin/admin)
- Streamlit Dashboard: http://localhost:8501
- PostgreSQL: localhost:5432

### First-Time Setup

1. **Initialize the database** (auto-runs on first start)
```bash
make init-db
```

2. **Backfill historical data** (2 years)
- Open Airflow UI (http://localhost:8080)
- Trigger the `stock_market_backfill` DAG
- Or run: `make extract`

3. **Run dbt models**
```bash
make dbt-run
make dbt-test
```

4. **View the dashboard**
- Navigate to http://localhost:8501
- Explore portfolio analytics!

## 📈 Dashboard Features

### 1. Portfolio Overview
- Key metrics: average returns, top performers, volatility
- Sector performance comparison
- Portfolio holdings table with real-time data

### 2. Stock Analysis
- Interactive price charts with moving averages
- Bollinger Bands visualization
- Volume analysis
- RSI indicator with overbought/oversold zones
- MACD for trend identification

### 3. Top Performers
- Top 10 gainers and losers
- Investment recommendations (Strong Buy/Buy/Sell)
- Performance rankings

### 4. Risk Analysis
- Risk distribution (Low/Medium/High)
- Volatility vs Return scatter plot
- Sharpe ratio analysis
- Maximum drawdown metrics

## 🧪 Testing

```bash
# Run all tests
make test

# Run dbt tests
make dbt-test

# Check data quality
make dbt-run
```

## 📋 Available Commands

```bash
make help           # Show all available commands
make build          # Build Docker images
make up             # Start all services
make down           # Stop all services
make logs           # View all logs
make clean          # Clean up volumes and containers
make dbt-run        # Run dbt transformations
make dbt-test       # Run dbt tests
make extract        # Run data extraction
make shell-airflow  # Access Airflow container
make shell-db       # Access PostgreSQL CLI
make backup-db      # Backup database
```

## ☁️ Future Enhancements

- CI/CD pipeline with GitHub Actions
- Cloud deployment (AWS/Railway)
- Additional technical indicators
- Real-time streaming data
- More stock tickers and sectors

## 📊 Sample Data

The pipeline includes 10 pre-configured stocks:
- **Tech**: AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META
- **Finance**: JPM, V
- **Retail**: WMT

Easily add more tickers by updating the `stock_metadata` table.

## 🎓 Skills Demonstrated

✅ **ETL Pipeline Design**: Production-ready data extraction, transformation, and loading  
✅ **Workflow Orchestration**: Airflow DAG development with error handling  
✅ **Database Design**: Schema design, indexing, query optimization  
✅ **Data Transformation**: dbt models with complex SQL analytics  
✅ **Financial Analytics**: Technical indicators, risk metrics, portfolio analysis  
✅ **Data Visualization**: Interactive dashboards with Plotly/Streamlit  
✅ **Containerization**: Multi-service Docker Compose orchestration  
✅ **API Integration**: Alpha Vantage API with rate limiting  
✅ **Finance Domain**: Stock market analytics and portfolio management  

## 🔐 Security Notes

- Store your Alpha Vantage API key in `.env` (not in code)
- Change default database passwords in `.env` for production
- The `.env` file is gitignored and won't be committed
- Alpha Vantage free tier: 5 calls/min, 500 calls/day
- 
## 📧 Contact

For questions or feedback:
- GitHub: matasudr1
- LinkedIn: Matas Udrėnas
- Email: matas.udrenas@gmail.com

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

## 🙏 Acknowledgments

- [Alpha Vantage](https://www.alphavantage.co/) for stock market data API
- [Apache Airflow](https://airflow.apache.org/) for orchestration
- [dbt](https://www.getdbt.com/) for data transformation
- [Streamlit](https://streamlit.io/) for dashboard framework

---

**⭐ Star this repo if you find it helpful!**

Built with ❤️ as a data engineering portfolio project.

## 📝 Project Structure

```
stock-market-pipeline/
├── airflow/
│   └── dags/
│       ├── stock_market_dag.py      # Daily extraction DAG
│       └── stock_market_backfill.py # Historical backfill DAG
├── dbt_stock/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_stock_prices.sql
│   │   │   └── stg_stock_metadata.sql
│   │   └── marts/
│   │       ├── fct_technical_indicators.sql
│   │       ├── fct_daily_performance.sql
│   │       └── fct_portfolio_analytics.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── dashboard/
│   └── app.py                 # Streamlit dashboard
├── ingestion/
│   └── stock_extractor.py     # Alpha Vantage extraction
├── scripts/
│   └── init_db.sql            # Database initialization
├── docker-compose.yml         # Service orchestration
├── Dockerfile.airflow         # Airflow container
├── Dockerfile.streamlit       # Dashboard container
├── requirements.txt           # Python dependencies
├── Makefile                   # Helper commands
└── README.md

