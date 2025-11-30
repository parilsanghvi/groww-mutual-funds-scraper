"""
Unit tests for the FundScraper class.

These tests use mocked HTML fixtures to test data extraction methods
without requiring actual web scraping or browser automation.
"""

import pytest
from bs4 import BeautifulSoup
from unittest.mock import Mock, MagicMock, patch
import os
import sys

# Add parent directory to path to import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_mutual_fund_details import FundScraper


@pytest.fixture
def sample_html():
    """Load sample HTML fixture."""
    fixture_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'sample_fund_page.html')
    with open(fixture_path, 'r', encoding='utf-8') as f:
        return f.read()


@pytest.fixture
def sample_soup(sample_html):
    """Create BeautifulSoup object from sample HTML."""
    return BeautifulSoup(sample_html, 'html.parser')


@pytest.fixture
def mock_driver(sample_html):
    """Create a mock Selenium driver."""
    driver = MagicMock()
    driver.page_source = sample_html
    driver.find_elements.return_value = []
    driver.execute_script.return_value = 1000
    return driver


@pytest.fixture
def scraper_instance():
    """Create a FundScraper instance without initializing the driver."""
    scraper = FundScraper()
    scraper.driver = MagicMock()
    return scraper


class TestFundScraperAUM:
    """Test AUM extraction methods."""
    
    @pytest.mark.unit
    def test_extract_aum_from_table(self, scraper_instance, sample_soup, mock_driver):
        """Test AUM extraction from table with fund name matching."""
        scraper_instance.driver = mock_driver
        tables = sample_soup.find_all('table')
        fund_name = "HDFC Equity Growth Fund - Direct Plan - Growth"
        
        result = scraper_instance._extract_aum(tables, fund_name)
        
        # The fixture has "₹5,234.56 Cr" in the table
        assert result == "₹5,234.56 Cr"
    
    @pytest.mark.unit
    def test_extract_aum_partial_name_match(self, scraper_instance, sample_soup, mock_driver):
        """Test AUM extraction with partial fund name match."""
        scraper_instance.driver = mock_driver
        tables = sample_soup.find_all('table')
        fund_name = "HDFC Equity Growth"  # Partial name
        
        result = scraper_instance._extract_aum(tables, fund_name)
        
        assert result == "₹5,234.56 Cr"
    
    @pytest.mark.unit
    def test_extract_aum_fallback_regex(self, scraper_instance, mock_driver):
        """Test AUM extraction using regex fallback when table match fails."""
        # Create HTML without fund name in table but with "Fund size" label
        html = """
        <html>
        <body>
            <div>Fund size ₹1,234Cr</div>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Mock driver to simulate finding elements
        mock_elem = MagicMock()
        mock_elem.text = "Fund size ₹1,234Cr"
        mock_elem.get_attribute.return_value = "Fund size ₹1,234Cr"
        mock_driver.find_elements.return_value = [mock_elem]
        
        scraper_instance.driver = mock_driver
        tables = soup.find_all('table')
        
        result = scraper_instance._extract_aum(tables, "Some Fund Name")
        
        # Should extract using regex fallback
        assert "1,234" in result or "1234" in result
    
    @pytest.mark.unit
    def test_extract_aum_not_found(self, scraper_instance, mock_driver):
        """Test AUM extraction when data is not available."""
        html = "<html><body><div>No AUM data here</div></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        scraper_instance.driver = mock_driver
        mock_driver.find_elements.return_value = []
        
        tables = soup.find_all('table')
        
        result = scraper_instance._extract_aum(tables, "Fund Name")
        
        assert result == "NA"


class TestFundScraperExpenseAndLoad:
    """Test expense ratio and exit load extraction."""
    
    @pytest.mark.unit
    def test_extract_expense_and_load(self, scraper_instance, sample_soup):
        """Test extraction of expense ratio and exit load."""
        data = {}
        scraper_instance._extract_expense_and_load(sample_soup, data)
        
        assert data["Expense Ratio"] == "0.75%"
        assert data["Exit Load"] == "1% if redeemed within 1 year"
    
    @pytest.mark.unit
    def test_extract_expense_and_load_not_found(self, scraper_instance):
        """Test when expense ratio and exit load are not available."""
        html = "<html><body></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        
        scraper_instance._extract_expense_and_load(soup, data)
        
        assert data["Expense Ratio"] == "NA"
        assert data["Exit Load"] == "NA"


class TestFundScraperBenchmark:
    """Test benchmark extraction."""
    
    @pytest.mark.unit
    def test_extract_benchmark(self, scraper_instance, sample_soup):
        """Test benchmark extraction from table."""
        tables = sample_soup.find_all('table')
        
        result = scraper_instance._extract_benchmark(tables)
        
        assert result == "Nifty 50 TRI"
    
    @pytest.mark.unit
    def test_extract_benchmark_not_found(self, scraper_instance):
        """Test when benchmark is not available."""
        html = "<html><body><table><tr><td>No benchmark</td></tr></table></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        
        result = scraper_instance._extract_benchmark(tables)
        
        assert result == "NA"


class TestFundScraperReturnsAndRank:
    """Test returns and rank extraction."""
    
    @pytest.mark.unit
    def test_extract_returns_and_rank(self, scraper_instance, sample_soup):
        """Test extraction of fund returns, category averages, and ranks."""
        tables = sample_soup.find_all('table')
        data = {"Fund Name": "HDFC Equity Growth Fund"}
        
        scraper_instance._extract_returns_and_rank(tables, data)
        
        # Check fund returns
        assert data["1Y Fund Return"] == "15.2%"
        assert data["3Y Fund Return"] == "18.5%"
        assert data["5Y Fund Return"] == "14.3%"
        assert data["All Fund Return"] == "12.8%"
        
        # Check category averages
        assert data["1Y Category Avg"] == "13.5%"
        assert data["3Y Category Avg"] == "16.2%"
        assert data["5Y Category Avg"] == "12.9%"
        assert data["All Category Avg"] == "11.5%"
        
        # Check ranks
        assert data["1Y Rank"] == "45"
        assert data["3Y Rank"] == "32"
        assert data["5Y Rank"] == "28"
        assert data["All Rank"] == "25"
    
    @pytest.mark.unit
    def test_extract_returns_and_rank_not_found(self, scraper_instance):
        """Test when returns data is not available."""
        html = "<html><body></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        data = {"Fund Name": "Test Fund"}
        
        scraper_instance._extract_returns_and_rank(tables, data)
        
        # All values should default to "NA"
        for period in ["1Y", "3Y", "5Y", "All"]:
            assert data[f"{period} Fund Return"] == "NA"
            assert data[f"{period} Category Avg"] == "NA"
            assert data[f"{period} Rank"] == "NA"


class TestFundScraperRatios:
    """Test ratio extraction (P/E, P/B, Alpha, Beta, etc.)."""
    
    @pytest.mark.unit
    def test_extract_ratios(self, scraper_instance, sample_soup):
        """Test extraction of all financial ratios."""
        tables = sample_soup.find_all('table')
        data = {}
        
        scraper_instance._extract_ratios(tables, data)
        
        assert data["P/E Ratio"] == "25.4"
        assert data["P/B Ratio"] == "3.8"
        assert data["Alpha"] == "2.5"
        assert data["Beta"] == "0.95"
        assert data["Sharpe"] == "1.45"
        assert data["Sortino"] == "1.85"
    
    @pytest.mark.unit
    def test_extract_ratios_not_found(self, scraper_instance):
        """Test when ratio data is not available."""
        html = "<html><body></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        data = {}
        
        scraper_instance._extract_ratios(tables, data)
        
        assert data["P/E Ratio"] == "NA"
        assert data["P/B Ratio"] == "NA"
        assert data["Alpha"] == "NA"
        assert data["Beta"] == "NA"
        assert data["Sharpe"] == "NA"
        assert data["Sortino"] == "NA"


class TestFundScraperManagers:
    """Test fund manager extraction."""
    
    @pytest.mark.unit
    def test_extract_managers(self, scraper_instance, sample_soup):
        """Test extraction of fund manager names and tenures."""
        data = {}
        
        scraper_instance._extract_managers(sample_soup, data)
        
        expected = "Rahul Goswami (Since Jun 2018), Priya Sharma (Since Jan 2020)"
        assert data["Fund Managers"] == expected
    
    @pytest.mark.unit
    def test_extract_managers_empty(self, scraper_instance):
        """Test when no fund manager data is available."""
        html = "<html><body></body></html>"
        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        
        scraper_instance._extract_managers(soup, data)
        
        assert data["Fund Managers"] == ""


class TestFundScraperIntegration:
    """Integration tests for complete data parsing."""
    
    @pytest.mark.unit
    def test_parse_data_complete(self, scraper_instance, sample_soup, mock_driver):
        """Test complete data parsing with all fields."""
        # Mock WebDriverWait to return fund name
        with patch('get_mutual_fund_details.WebDriverWait') as mock_wait:
            mock_elem = MagicMock()
            mock_elem.text = "HDFC Equity Growth Fund - Direct Plan - Growth"
            mock_wait.return_value.until.return_value = mock_elem
            
            scraper_instance.driver = mock_driver
            result = scraper_instance._parse_data(sample_soup, mock_driver)
        
        # Verify all key fields are extracted
        assert result["Fund Name"] == "HDFC Equity Growth Fund - Direct Plan - Growth"
        assert result["Fund Type"] == "Large Cap"
        assert "5,234" in result["AUM"]
        assert result["Expense Ratio"] == "0.75%"
        assert result["Exit Load"] == "1% if redeemed within 1 year"
        assert result["Benchmark"] == "Nifty 50 TRI"
        assert result["1Y Fund Return"] == "15.2%"
        assert result["P/E Ratio"] == "25.4"
        assert "Rahul Goswami" in result["Fund Managers"]


class TestFundScraperDriverSetup:
    """Test driver setup and context manager."""
    
    @pytest.mark.unit
    def test_context_manager_setup_teardown(self):
        """Test that the context manager properly sets up and tears down driver."""
        with patch.object(FundScraper, 'setup_driver') as mock_setup:
            mock_driver = MagicMock()
            
            scraper = FundScraper()
            scraper.driver = mock_driver
            
            with scraper as s:
                assert s is not None
                mock_setup.assert_called_once()
            
            # Verify quit was called
            mock_driver.quit.assert_called_once()
    
    @pytest.mark.unit
    def test_init(self):
        """Test FundScraper initialization."""
        scraper = FundScraper()
        assert scraper.driver is None
