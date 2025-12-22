# Stock Market Pipeline - Getting Started Guide

## Step-by-Step Setup

### 1. Prerequisites Installation

#### Windows
```powershell
# Install Docker Desktop for Windows
# Download from: https://www.docker.com/products/docker-desktop

# Install Git
# Download from: https://git-scm.com/download/win

# Verify installations
docker --version
docker-compose --version
git --version
```

#### Mac
```bash
# Install Docker Desktop for Mac
# Download from: https://www.docker.com/products/docker-desktop

# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Git
brew install git
```

#### Linux (Ubuntu/Debian)
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo apt-get update
sudo apt-get install docker-compose-plugin

# Install Git
sudo apt-get install git
```

### 2. Clone and Configure

```bash
# Clone the repository
git clone https://github.com/yourusername/stock-market-pipeline.git
cd stock-market-pipeline

# Create environment file
cp .env.example .env

# Edit .env file (use your preferred editor)
nano .env  # or code .env for VS Code
```

### 3. Build the Project

```bash
# Build all Docker images (this may take 5-10 minutes first time)
docker-compose build

# Verify images are built
docker images | grep stock
```

### 4. Start Services

```bash
# Start all services in detached mode
docker-compose up -d

# Wait for services to be healthy (check status)
docker-compose ps

# View logs to monitor startup
docker-compose logs -f
```

### 5. Verify Services

1. **PostgreSQL** (should be running)
```bash
docker-compose exec postgres pg_isready -U postgres
```

2. **Airflow UI** (http://localhost:8080)
   - Username: `admin`
   - Password: `admin`

3. **Streamlit Dashboard** (http://localhost:8501)
   - Should load with "No data" message initially

### 6. Load Initial Data

#### Option A: Using Airflow UI (Recommended)

1. Open http://localhost:8080
2. Login with admin/admin
3. Find `stock_market_backfill` DAG
4. Click the "play" button to trigger
5. Monitor progress in the Graph View
6. Wait for completion (15-30 minutes for 2 years of data)

#### Option B: Using Command Line

```bash
# Trigger backfill DAG
docker-compose exec airflow_scheduler airflow dags trigger stock_market_backfill

# Monitor progress
docker-compose logs -f airflow_worker
```

#### Option C: Manual Data Load

```bash
# Run extraction script directly
docker-compose exec airflow_worker python /opt/airflow/ingestion/stock_extractor.py
```

### 7. Run dbt Transformations

```bash
# Install dbt packages
docker-compose exec airflow_worker bash -c "cd /opt/airflow/dbt_stock && dbt deps --profiles-dir . --project-dir ."

# Run all dbt models
docker-compose exec airflow_worker bash -c "cd /opt/airflow/dbt_stock && dbt run --profiles-dir . --project-dir ."

# Run dbt tests
docker-compose exec airflow_worker bash -c "cd /opt/airflow/dbt_stock && dbt test --profiles-dir . --project-dir ."

# Or use Makefile shortcuts
make dbt-run
make dbt-test
```

### 8. View the Dashboard

1. Open http://localhost:8501
2. Explore the 4 tabs:
   - Portfolio Overview
   - Stock Analysis
   - Top Performers
   - Risk Analysis

### 9. Enable Daily Updates

1. Open Airflow UI (http://localhost:8080)
2. Find `stock_market_pipeline` DAG
3. Toggle the DAG to "On" (unpause)
4. Pipeline will run daily at 6 PM weekdays

## Common Issues & Solutions

### Issue: Services won't start

**Solution:**
```bash
# Check Docker is running
docker info

# Check port conflicts
netstat -an | grep 8080  # Check if 8080 is in use
netstat -an | grep 5432  # Check if 5432 is in use

# If ports are in use, stop conflicting services or modify docker-compose.yml
```

### Issue: Database connection errors

**Solution:**
```bash
# Restart PostgreSQL
docker-compose restart postgres

# Check PostgreSQL logs
docker-compose logs postgres

# Verify database initialization
docker-compose exec postgres psql -U postgres -d stock_market -c "\dt"
```

### Issue: Airflow UI not accessible

**Solution:**
```bash
# Check Airflow webserver logs
docker-compose logs airflow_webserver

# Restart Airflow services
docker-compose restart airflow_webserver airflow_scheduler

# Check if port 8080 is available
```

### Issue: No data in dashboard

**Solution:**
```bash
# Verify data exists in database
docker-compose exec postgres psql -U postgres -d stock_market -c "SELECT COUNT(*) FROM raw_stock_prices;"

# If no data, run backfill
docker-compose exec airflow_scheduler airflow dags trigger stock_market_backfill

# Run dbt models
make dbt-run
```

### Issue: Out of memory errors

**Solution:**
```bash
# Increase Docker memory allocation
# Docker Desktop -> Settings -> Resources -> Memory (set to 8GB+)

# Reduce concurrent workers in docker-compose.yml
# Modify AIRFLOW__CELERY__WORKER_CONCURRENCY environment variable
```

## Daily Operations

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow_scheduler
docker-compose logs -f streamlit
docker-compose logs -f postgres
```

### Execute Commands in Containers
```bash
# Access Airflow shell
docker-compose exec airflow_worker bash

# Access PostgreSQL CLI
docker-compose exec postgres psql -U postgres -d stock_market

# Run Python script
docker-compose exec airflow_worker python /opt/airflow/ingestion/stock_extractor.py
```

### Update Code
```bash
# Pull latest changes
git pull origin main

# Rebuild and restart services
docker-compose down
docker-compose build
docker-compose up -d
```

### Backup Database
```bash
# Create backup
docker-compose exec postgres pg_dump -U postgres stock_market > backup_$(date +%Y%m%d).sql

# Restore from backup
cat backup_20240101.sql | docker-compose exec -T postgres psql -U postgres -d stock_market
```

### Clean Up
```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Clean up Docker system
docker system prune -a
```

## Next Steps

1. **Customize Tickers**: Add your favorite stocks to `stock_metadata` table
2. **Adjust Schedule**: Modify `schedule_interval` in `stock_market_dag.py`
3. **Add Indicators**: Create new dbt models for custom metrics
4. **Extend Dashboard**: Add new visualizations in `dashboard/app.py`
5. **Deploy to Cloud**: Follow deployment guides for AWS or Railway

## Support

- **Documentation**: See [README.md](README.md)
- **Issues**: Open a GitHub issue
- **Questions**: Contact via email or LinkedIn

Happy analyzing! 📈
