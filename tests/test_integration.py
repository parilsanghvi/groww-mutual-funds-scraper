"""
Integration tests for worker function and Excel output generation.
"""

import pytest
import queue
import threading
import pandas as pd
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_mutual_fund_details import worker, FundScraper


class TestWorkerFunction:
    """Test the worker thread function."""
    
    @pytest.mark.integration
    def test_worker_processes_urls(self):
        """Test that worker processes URLs from queue correctly."""
        # Create test data
        url_queue = queue.Queue()
        results_list = []
        failed_list = []
        
        test_url = "https://example.com/test-fund"
        url_queue.put(test_url)
        
        # Mock the scraper
        with patch('get_mutual_fund_details.FundScraper') as MockScraper:
            mock_scraper_instance = MagicMock()
            mock_result = {
                "Fund Name": "Test Fund",
                "Fund Type": "Equity",
                "AUM": "1000 Cr"
            }
            mock_scraper_instance.scrape_url.return_value = mock_result
            MockScraper.return_value.__enter__.return_value = mock_scraper_instance
            MockScraper.return_value.__exit__.return_value = None
            
            # Run worker
            worker(url_queue, results_list, failed_list)
            
            # Verify results
            assert len(results_list) == 1
            assert results_list[0]["Fund Name"] == "Test Fund"
            assert len(failed_list) == 0
            assert url_queue.empty()
    
    @pytest.mark.integration
    def test_worker_handles_failures(self):
        """Test that worker handles scraping failures gracefully."""
        url_queue = queue.Queue()
        results_list = []
        failed_list = []
        
        test_url = "https://example.com/test-fund"
        url_queue.put(test_url)
        
        # Mock scraper to return None (failure)
        with patch('get_mutual_fund_details.FundScraper') as MockScraper:
            mock_scraper_instance = MagicMock()
            mock_scraper_instance.scrape_url.return_value = None
            MockScraper.return_value.__enter__.return_value = mock_scraper_instance
            MockScraper.return_value.__exit__.return_value = None
            
            # Run worker
            worker(url_queue, results_list, failed_list)
            
            # Verify results
            assert len(results_list) == 0
            assert len(failed_list) == 1
            assert failed_list[0] == test_url
    
    @pytest.mark.integration
    def test_worker_processes_multiple_urls(self):
        """Test that worker processes multiple URLs."""
        url_queue = queue.Queue()
        results_list = []
        failed_list = []
        
        urls = [
            "https://example.com/fund1",
            "https://example.com/fund2",
            "https://example.com/fund3"
        ]
        
        for url in urls:
            url_queue.put(url)
        
        # Mock scraper
        with patch('get_mutual_fund_details.FundScraper') as MockScraper:
            mock_scraper_instance = MagicMock()
            
            def mock_scrape(url):
                return {
                    "Fund Name": f"Fund from {url}",
                    "URL": url
                }
            
            mock_scraper_instance.scrape_url.side_effect = mock_scrape
            MockScraper.return_value.__enter__.return_value = mock_scraper_instance
            MockScraper.return_value.__exit__.return_value = None
            
            # Run worker
            worker(url_queue, results_list, failed_list)
            
            # Verify all URLs were processed
            assert len(results_list) == 3
            assert len(failed_list) == 0
            assert url_queue.empty()


class TestExcelOutput:
    """Test Excel output generation."""
    
    @pytest.mark.integration
    def test_dataframe_creation(self):
        """Test that DataFrame is created correctly from results."""
        # Sample results
        results = [
            {
                "Fund Name": "Fund A",
                "Fund Type": "Equity",
                "AUM": "1000 Cr",
                "1Y Fund Return": "15%",
                "Expense Ratio": "0.5%"
            },
            {
                "Fund Name": "Fund B",
                "Fund Type": "Debt",
                "AUM": "500 Cr",
                "1Y Fund Return": "7%",
                "Expense Ratio": "0.3%"
            }
        ]
        
        df = pd.DataFrame(results)
        
        assert len(df) == 2
        assert "Fund Name" in df.columns
        assert "Fund Type" in df.columns
        assert df.iloc[0]["Fund Name"] == "Fund A"
        assert df.iloc[1]["Fund Type"] == "Debt"
    
    @pytest.mark.integration
    def test_dataframe_with_missing_columns(self):
        """Test DataFrame creation when some fields are missing."""
        results = [
            {
                "Fund Name": "Fund A",
                "Fund Type": "Equity",
            }
        ]
        
        expected_columns = [
            'Fund Name', 'Fund Type', 'AUM',
            '1Y Fund Return', '1Y Category Avg', '1Y Rank',
            'Expense Ratio', 'Exit Load'
        ]
        
        df = pd.DataFrame(results)
        
        # Add missing columns with "NA"
        for col in expected_columns:
            if col not in df.columns:
                df[col] = "NA"
        
        df = df[expected_columns]
        
        assert len(df) == 1
        assert df.iloc[0]["Fund Name"] == "Fund A"
        assert df.iloc[0]["AUM"] == "NA"
        assert df.iloc[0]["Expense Ratio"] == "NA"
    
    @pytest.mark.integration
    def test_grouping_by_fund_type(self):
        """Test that data is correctly grouped by fund type."""
        results = [
            {"Fund Name": "Fund A", "Fund Type": "Equity", "AUM": "1000"},
            {"Fund Name": "Fund B", "Fund Type": "Debt", "AUM": "500"},
            {"Fund Name": "Fund C", "Fund Type": "Equity", "AUM": "800"},
            {"Fund Name": "Fund D", "Fund Type": "Hybrid", "AUM": "600"},
        ]
        
        # Group by fund type
        data_by_fund_type = {}
        for result in results:
            fund_type = result.get("Fund Type", "Others")
            if fund_type not in data_by_fund_type:
                data_by_fund_type[fund_type] = []
            data_by_fund_type[fund_type].append(result)
        
        assert len(data_by_fund_type) == 3
        assert len(data_by_fund_type["Equity"]) == 2
        assert len(data_by_fund_type["Debt"]) == 1
        assert len(data_by_fund_type["Hybrid"]) == 1
