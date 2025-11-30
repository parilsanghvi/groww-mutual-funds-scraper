import logging
import time
import random
import threading
import queue
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FundScraper:
    def __init__(self):
        self.driver = None

    def __enter__(self):
        self.setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()

    def setup_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # Suppress logging from chrome
        options.add_argument("--log-level=3")
        
        try:
            self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        except Exception as e:
            logger.error(f"Failed to initialize driver: {e}")
            raise

    def scrape_url(self, url):
        if not self.driver:
            self.setup_driver()
            
        try:
            logger.info(f"Scraping URL: {url}")
            self.driver.get(url.strip())
            
            # Scroll to load content
            self._scroll_page()
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            data = self._parse_data(soup, self.driver)
            data['URL'] = url
            return data
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}", exc_info=True)
            return None

    def _scroll_page(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def _parse_data(self, soup, driver):
        data = {}
        
        # Fund Name
        try:
            fund_name_elem = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "mfh239SchemeName"))
            )
            data["Fund Name"] = fund_name_elem.text
        except Exception:
            fund_name_elem = soup.find('h1', class_='mfh239SchemeName')
            data["Fund Name"] = fund_name_elem.text if fund_name_elem else "NA"

        # Fund Type
        fund_type_elements = soup.find_all('div', attrs={'class': 'mfh239PillsContainer'})
        data["Fund Type"] = fund_type_elements[1].text if len(fund_type_elements) > 1 else "NA"

        # Tables extraction helper
        tables = soup.find_all('table')

        # AUM
        data["AUM"] = self._extract_aum(tables, data["Fund Name"])

        # Expense Ratio & Exit Load
        self._extract_expense_and_load(soup, data)

        # Benchmark
        data["Benchmark"] = self._extract_benchmark(tables)

        # Returns and Rank
        self._extract_returns_and_rank(tables, data)

        # Ratios (P/E, P/B, Alpha, Beta, etc.)
        self._extract_ratios(tables, data)

        # Fund Managers
        self._extract_managers(soup, data)

        return data

    def _extract_aum(self, tables, fund_name):
        # Method 1: Table search (Case Insensitive)
        for table in tables:
            if "Fund Size" in table.text or "Fund size" in table.text:
                headers = [th.text.strip() for th in table.find_all('th')]
                try:
                    fund_size_idx = -1
                    for i, h in enumerate(headers):
                        if "Fund Size" in h or "Fund size" in h:
                            fund_size_idx = i
                            break
                    
                    if fund_size_idx != -1:
                        rows = table.find_all('tr')
                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if not cols: continue
                            
                            row_name = cols[0].text.strip()
                            norm_fund_name = fund_name.lower().replace(' ', '')
                            norm_row_name = row_name.lower().replace(' ', '')
                            
                            if norm_row_name in norm_fund_name or norm_fund_name in norm_row_name:
                                if len(cols) > fund_size_idx:
                                    val = cols[fund_size_idx].text.strip()
                                    if any(c.isdigit() for c in val):
                                        return val
                        
                        # Fallback: First data row if no name match
                        # REMOVED: This was causing issues where it picked the first row of "Similar Funds" table
                        # resulting in duplicate AUMs for funds in the same category.
                        # We will rely on Method 2 (Regex) if name match fails.
                        pass

                except Exception as e:
                    logger.warning(f"Error parsing AUM table: {e}")

        # Method 2: Search by text in entire soup (Fallback)
        try:
            # Find elements containing "Fund size" (case insensitive)
            target_elems = [
                elem for elem in self.driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'fund size')]")
            ]
            
            for elem in target_elems:
                # Check element text and parent text
                texts_to_check = [elem.text, elem.get_attribute("textContent")]
                try:
                    parent = elem.find_element(By.XPATH, "..")
                    texts_to_check.append(parent.text)
                    texts_to_check.append(parent.get_attribute("textContent"))
                except:
                    pass
                
                for text in texts_to_check:
                    if not text: continue
                    # Regex to find pattern like ₹1,234.56Cr or 1234Cr
                    # Normalize text
                    norm_text = text.lower().replace('\n', ' ').replace('\r', '')
                    if "fund size" in norm_text:
                        # Extract part after fund size
                        after_label = norm_text.split("fund size")[1]
                        match = re.search(r'(?:₹\s?)?[\d,.]+\s*cr', after_label, re.IGNORECASE)
                        if match:
                            val = match.group(0)
                            # Normalize: remove ₹, Cr, CR, space
                            val = val.replace('₹', '').replace('Cr', '').replace('CR', '').replace('cr', '').strip()
                            return val

        except Exception as e:
            logger.warning(f"Error in AUM fallback: {e}")

        return "NA"

    def _extract_expense_and_load(self, soup, data):
        data["Expense Ratio"] = "NA"
        data["Exit Load"] = "NA"
        
        headings = soup.find_all('div', attrs={'class': 'mf320Heading'})
        for div in headings:
            text = div.text
            if 'Expense Ratio' in text and ':' in text:
                 data["Expense Ratio"] = text.split(':')[1].replace('Inclusive of GST', '').strip()
            
            if div.h3 and "Exit load" in div.h3.text:
                 data["Exit Load"] = div.p.text.strip() if div.p else "NA"

    def _extract_benchmark(self, tables):
        for table in tables:
            if "Fund benchmark" in table.text:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if th and td and "Fund benchmark" in th.text:
                        return td.text.strip()
        return "NA"

    def _extract_returns_and_rank(self, tables, data):
        fund_returns = {}
        category_averages = {}
        rank_within_category = {}
        
        returns_table = None
        for table in tables:
            if "Rank with in category" in table.text or "Category average" in table.text:
                returns_table = table
                break
        
        if returns_table:
            headers = [th.text.strip() for th in returns_table.find_all('th')]
            rows = returns_table.find_all('tr')
            for row in rows:
                cols = row.find_all(['td', 'th'])
                col_texts = [c.text.strip() for c in cols]
                if not col_texts: continue
                
                label = col_texts[0]
                
                # Identify row type
                target_dict = None
                if "Fund returns" in label or data.get("Fund Name", "") in label: # Sometimes label is fund name
                    target_dict = fund_returns
                elif "Category average" in label:
                    target_dict = category_averages
                elif "Rank with in category" in label:
                    target_dict = rank_within_category
                
                if target_dict is not None:
                    for i, header in enumerate(headers):
                        if i > 0 and i < len(col_texts):
                            target_dict[header] = col_texts[i]

        for period in ["1Y", "3Y", "5Y", "All"]:
            data[f"{period} Fund Return"] = fund_returns.get(period, "NA")
            data[f"{period} Category Avg"] = category_averages.get(period, "NA")
            data[f"{period} Rank"] = rank_within_category.get(period, "NA")

    def _extract_ratios(self, tables, data):
        data["P/E Ratio"] = "NA"
        data["P/B Ratio"] = "NA"
        data["Alpha"] = "NA"
        data["Beta"] = "NA"
        data["Sharpe"] = "NA"
        data["Sortino"] = "NA"

        for table in tables:
            # P/E & P/B
            if "P/E Ratio" in table.text:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    texts = [c.text.strip() for c in cols]
                    if len(texts) >= 2:
                        if "P/E Ratio" in texts[0]: data["P/E Ratio"] = texts[1]
                        elif "P/B Ratio" in texts[0]: data["P/B Ratio"] = texts[1]
            
            # Stats
            if "Alpha" in table.text and "Beta" in table.text:
                rows = table.find_all('tr')
                for row in rows:
                    header = row.find('th')
                    value = row.find('td')
                    if header and value:
                        h_text = header.text.strip()
                        v_text = value.text.strip()
                        if "Alpha" in h_text: data["Alpha"] = v_text
                        elif "Beta" in h_text: data["Beta"] = v_text
                        elif "Sharpe" in h_text: data["Sharpe"] = v_text
                        elif "Sortino" in h_text: data["Sortino"] = v_text

    def _extract_managers(self, soup, data):
        managers = []
        manager_sections = soup.find_all('div', class_='fm982CardText')
        for m in manager_sections:
            name = m.find('div', class_='fm982PersonName')
            tenure = m.find('div', class_='contentSecondary')
            n_text = name.text.strip() if name else "Unknown"
            t_text = tenure.text.strip() if tenure else "Unknown"
            managers.append(f"{n_text} ({t_text})")
        data["Fund Managers"] = ', '.join(managers)


def worker(url_queue, results_list, failed_list):
    """
    Worker thread function that maintains a persistent browser session.
    """
    with FundScraper() as scraper:
        while True:
            try:
                url = url_queue.get(block=False)
            except queue.Empty:
                break
            
            try:
                result = scraper.scrape_url(url)
                if result:
                    results_list.append(result)
                else:
                    failed_list.append(url)
            except Exception as e:
                logger.error(f"Worker failed on {url}: {e}")
                failed_list.append(url)
            finally:
                url_queue.task_done()
                # Small delay to be nice
                time.sleep(random.uniform(0.5, 1.5))


def main():
    try:
        with open('mutual_funds_links.txt', 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error("mutual_funds_links.txt not found.")
        return

    # Thread-safe structures
    url_queue = queue.Queue()
    for url in urls:
        url_queue.put(url)
        
    results_list = [] # Lists are thread-safe for append in CPython, but let's be careful. 
    # Actually, append is atomic in CPython, but for strict correctness we could use a lock or a queue.
    # Given the low contention, a list with append is fine, or we can use a results queue.
    # Let's use a list for simplicity as we join threads before processing.
    # To be perfectly safe, let's use a results queue or lock.
    results_queue = queue.Queue()
    failed_list = []
    failed_lock = threading.Lock() # Lock for failed list if we care about order or race conditions (append is atomic though)

    # Wrapper to handle results queue
    def worker_wrapper():
        local_results = []
        local_failed = []
        worker(url_queue, local_results, local_failed)
        
        for r in local_results:
            results_queue.put(r)
        
        with failed_lock:
            failed_list.extend(local_failed)

    max_workers = 10
    logger.info(f"Starting scraping with {max_workers} persistent workers for {len(urls)} URLs...")
    
    threads = []
    for _ in range(max_workers):
        t = threading.Thread(target=worker_wrapper)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Process results
    data_by_fund_type = {}
    while not results_queue.empty():
        result = results_queue.get()
        fund_type = result.get("Fund Type", "Others")
        if fund_type not in data_by_fund_type:
            data_by_fund_type[fund_type] = []
        data_by_fund_type[fund_type].append(result)

    if failed_list:
        logger.warning(f"{len(failed_list)} tasks failed. Check log for details.")

    # Save to Excel
    columns = [
        'Fund Name', 'Fund Type', 'AUM',
        '1Y Fund Return', '1Y Category Avg', '1Y Rank',
        '3Y Fund Return', '3Y Category Avg', '3Y Rank',
        '5Y Fund Return', '5Y Category Avg', '5Y Rank',
        'All Fund Return', 'All Category Avg', 'All Rank',
        'P/E Ratio', 'P/B Ratio', 'Alpha', 'Beta', 'Sharpe', 'Sortino',
        'Expense Ratio', 'Exit Load', 'Benchmark', 'Fund Managers', 'URL'
    ]

    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f'mutual_funds_details_{timestamp}.xlsx'
    
    if data_by_fund_type:
        print(f"Saving data to {filename}...")
        with pd.ExcelWriter(filename) as writer:
            for fund_type, data in data_by_fund_type.items():
                df = pd.DataFrame(data)
                for col in columns:
                    if col not in df.columns:
                        df[col] = "NA"
                df = df[columns]
                sheet_name = fund_type[:31].replace('/', '-')
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print("Done.")
    else:
        print("No data collected.")

if __name__ == "__main__":
    main()
