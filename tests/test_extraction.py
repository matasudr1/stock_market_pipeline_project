# Tests for Stock Market Pipeline

import pytest
from datetime import datetime, timedelta
from ingestion.stock_extractor import StockDataExtractor


def test_stock_extractor_initialization():
    """Test StockDataExtractor can be initialized"""
    extractor = StockDataExtractor()
    assert extractor is not None


def test_date_range_calculation():
    """Test date range calculation"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    assert start_date < end_date
    assert (end_date - start_date).days == 30
