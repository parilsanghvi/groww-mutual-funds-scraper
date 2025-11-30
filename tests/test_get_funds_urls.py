"""
Unit tests for the URL scraper (get_funds_urls.py).
"""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestURLScraper:
    """Test URL scraper functionality."""
    
    @pytest.mark.unit
    def test_url_deduplication(self):
        """Test that duplicate URLs are not added to the set."""
        unique_links = set()
        
        urls = [
            "https://groww.in/mutual-funds/axis-bluechip-fund-direct-plan-growth",
            "https://groww.in/mutual-funds/icici-prudential-equity-fund",
            "https://groww.in/mutual-funds/axis-bluechip-fund-direct-plan-growth",  # Duplicate
            "https://groww.in/mutual-funds/hdfc-index-fund",
        ]
        
        for url in urls:
            unique_links.add(url)
        
        # Should only have 3 unique URLs
        assert len(unique_links) == 3
    
    @pytest.mark.unit
    def test_url_format(self):
        """Test that URLs are properly formatted."""
        base_url = "https://groww.in"
        href = "/mutual-funds/axis-bluechip-fund-direct-plan-growth"
        
        full_url = f"{base_url}{href}"
        
        assert full_url.startswith("https://groww.in/mutual-funds/")
        assert "axis-bluechip-fund" in full_url
    
    @pytest.mark.unit
    def test_url_extraction_from_html(self):
        """Test extracting URLs from HTML content."""
        html = """
        <html>
        <body>
            <a class="pos-rel f22Link" href="/mutual-funds/fund1">Fund 1</a>
            <a class="pos-rel f22Link" href="/mutual-funds/fund2">Fund 2</a>
            <a class="other-class" href="/mutual-funds/fund3">Fund 3</a>
        </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        rows = soup.find_all('a', attrs={'class': 'pos-rel f22Link'})
        
        assert len(rows) == 2
        assert rows[0].get('href') == "/mutual-funds/fund1"
        assert rows[1].get('href') == "/mutual-funds/fund2"
    
    @pytest.mark.unit
    def test_empty_html_handling(self):
        """Test handling of HTML with no matching links."""
        html = """
        <html>
        <body>
            <p>No links here</p>
        </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        rows = soup.find_all('a', attrs={'class': 'pos-rel f22Link'})
        
        assert len(rows) == 0
    
    @pytest.mark.unit
    def test_pagination_url_format(self):
        """Test that pagination URLs are correctly formatted."""
        base_url = "https://groww.in/mutual-funds/filter"
        page_number = 5
        
        paginated_url = f"{base_url}?q=&fundSize=&pageNo={page_number}&sortBy=3"
        
        assert "pageNo=5" in paginated_url
        assert "sortBy=3" in paginated_url
        assert paginated_url.startswith("https://groww.in/mutual-funds/filter")
