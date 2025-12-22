"""
Stock Market Data Pipeline - Airflow DAG
Orchestrates daily stock data extraction and transformation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import sys
import os

# Add ingestion module to path
sys.path.insert(0, '/opt/airflow/ingestion')

from stock_extractor import StockDataExtractor

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    'stock_market_pipeline',
    default_args=default_args,
    description='Daily stock market data ETL pipeline',
    schedule_interval='0 18 * * 1-5',  # 6 PM on weekdays (after market close)
    start_date=days_ago(1),
    catchup=False,
    tags=['finance', 'stock_market', 'etl'],
)


def extract_stock_data(**context):
    """Extract stock data from Yahoo Finance"""
    execution_date = context['ds']
    
    # Initialize extractor
    extractor = StockDataExtractor()
    
    try:
        # Log pipeline start
        extractor.log_pipeline_execution(
            'stock_data_extraction',
            execution_date,
            'RUNNING'
        )
        
        # Get active tickers from database
        tickers = extractor.get_active_tickers()
        
        if not tickers:
            raise ValueError("No active tickers found in database")
        
        # Calculate date range (last 5 trading days for incremental load)
        end_date = datetime.strptime(execution_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=7)
        
        # Fetch data for all tickers
        df = extractor.fetch_multiple_stocks(
            tickers,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # Load to database
        extractor.load_to_database(df)
        
        # Log success
        extractor.log_pipeline_execution(
            'stock_data_extraction',
            execution_date,
            'SUCCESS',
            records_processed=len(df)
        )
        
        # Push metrics to XCom for monitoring
        context['task_instance'].xcom_push(
            key='records_extracted',
            value=len(df)
        )
        context['task_instance'].xcom_push(
            key='tickers_processed',
            value=len(tickers)
        )
        
        return f"Successfully extracted {len(df)} records for {len(tickers)} tickers"
        
    except Exception as e:
        # Log failure
        extractor.log_pipeline_execution(
            'stock_data_extraction',
            execution_date,
            'FAILED',
            error_message=str(e)
        )
        raise


def validate_data_quality(**context):
    """Validate data quality of extracted data"""
    execution_date = context['ds']
    
    hook = PostgresHook(postgres_conn_id='postgres_stock_market')
    
    # Check for data in the last 7 days
    query = f"""
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT ticker) as ticker_count,
        COUNT(CASE WHEN close IS NULL THEN 1 END) as null_close_count,
        COUNT(CASE WHEN volume = 0 THEN 1 END) as zero_volume_count
    FROM raw_stock_prices
    WHERE date >= '{execution_date}'::date - INTERVAL '7 days'
    """
    
    result = hook.get_first(query)
    
    if not result or result[0] == 0:
        raise ValueError("No data found in raw_stock_prices table")
    
    record_count, ticker_count, null_close, zero_volume = result
    
    # Data quality checks
    if null_close > 0:
        raise ValueError(f"Found {null_close} records with NULL close prices")
    
    # Push metrics
    context['task_instance'].xcom_push(key='validated_records', value=record_count)
    context['task_instance'].xcom_push(key='validated_tickers', value=ticker_count)
    
    return f"Data quality validated: {record_count} records for {ticker_count} tickers"


def send_pipeline_summary(**context):
    """Send pipeline execution summary"""
    records_extracted = context['task_instance'].xcom_pull(
        task_ids='extract_stock_data',
        key='records_extracted'
    )
    tickers_processed = context['task_instance'].xcom_pull(
        task_ids='extract_stock_data',
        key='tickers_processed'
    )
    
    summary = f"""
    Stock Market Pipeline Execution Summary
    ========================================
    Date: {context['ds']}
    Tickers Processed: {tickers_processed}
    Records Extracted: {records_extracted}
    Status: SUCCESS
    """
    
    print(summary)
    return summary


# Task 1: Extract stock data from Yahoo Finance
extract_data_task = PythonOperator(
    task_id='extract_stock_data',
    python_callable=extract_stock_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Validate data quality
validate_data_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
)

# Task 3: Run dbt models for transformations
dbt_run_task = BashOperator(
    task_id='dbt_transform',
    bash_command='cd /opt/airflow/dbt_stock && dbt run --profiles-dir . --project-dir .',
    dag=dag,
)

# Task 4: Run dbt tests
dbt_test_task = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt_stock && dbt test --profiles-dir . --project-dir .',
    dag=dag,
)

# Task 5: Generate dbt documentation
dbt_docs_task = BashOperator(
    task_id='dbt_generate_docs',
    bash_command='cd /opt/airflow/dbt_stock && dbt docs generate --profiles-dir . --project-dir .',
    dag=dag,
)

# Task 6: Send pipeline summary
summary_task = PythonOperator(
    task_id='send_pipeline_summary',
    python_callable=send_pipeline_summary,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_data_task >> validate_data_task >> dbt_run_task >> dbt_test_task >> dbt_docs_task >> summary_task
