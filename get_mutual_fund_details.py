from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to scrape a single URL
def scrape_fund_data(url):
    df_row = {}
    
    # Setup Chrome options with webdriver-manager for automatic ChromeDriver compatibility
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run headless for background execution
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = None
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        
        # Navigate to the fund's URL
        driver.get(url.strip())

        # Scroll to the bottom to load all content
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1) # Wait for scroll
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Parse the page content
        page = driver.page_source
        soup = BeautifulSoup(page, 'html.parser')

        # Extract fund details
        try:
            fund_name = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "mfh239SchemeName"))
            ).text
        except Exception:
             # Fallback if wait fails, try soup directly or just NA
            fund_name_elem = soup.find('h1', class_='mfh239SchemeName')
            fund_name = fund_name_elem.text if fund_name_elem else "NA"

        df_row["Fund Name"] = fund_name

        # Fund type
        fund_type_elements = soup.find_all('div', attrs={'class': 'mfh239PillsContainer'})
        fund_type = fund_type_elements[1].text if len(fund_type_elements) > 1 else "NA"
        df_row["Fund Type"] = fund_type

        # AUM (Fund Size)
        # Look for table with "Fund Size" in header
        aum = "NA"
        tables = soup.find_all('table')
        for table in tables:
            if "Fund Size" in table.text:
                # Check headers
                headers = [th.text.strip() for th in table.find_all('th')]
                fund_size_index = -1
                for i, h in enumerate(headers):
                    if "Fund Size" in h:
                        fund_size_index = i
                        break
                
                if fund_size_index != -1:
                    # Get first data row
                    rows = table.find_all('tr')
                    # Skip header row
                    for row in rows:
                        cols = row.find_all('td')
                        if not cols: 
                            # Try th if first column is th (some tables use th for row headers)
                            cols = row.find_all(['td', 'th'])
                        
                        # If this is a data row (has enough columns)
                        if len(cols) > fund_size_index:
                            # Check if it's the fund row (usually first row after header, or matches fund name)
                            # Assuming first row is the fund
                            val = cols[fund_size_index].text.strip()
                            # Basic check to ensure it's a number/value
                            if any(c.isdigit() for c in val):
                                aum = val
                                break
                break
        df_row["AUM"] = aum

        # Expense Ratio
        expense_ratio_div = soup.find('div', attrs={'class': 'mf320Heading'})
        expense_ratio = "NA"
        if expense_ratio_div:
            text = expense_ratio_div.text
            if ':' in text:
                expense_ratio = text.split(':')[1].replace('Inclusive of GST', '').strip()
        df_row["Expense Ratio"] = expense_ratio

        # Exit Load
        exit_load_divs = soup.find_all('div', class_='mf320Heading')
        exit_load_text = "NA"
        if len(exit_load_divs) > 1:
            exit_load_div = exit_load_divs[1]
            if exit_load_div.h3 and "Exit load" in exit_load_div.h3.text:
                 exit_load_text = exit_load_div.p.text.strip()
        df_row["Exit Load"] = exit_load_text

        # Benchmark
        benchmark = "NA"
        # Look for table with "Fund benchmark" in header
        for table in tables:
            if "Fund benchmark" in table.text:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if th and td and "Fund benchmark" in th.text:
                        benchmark = td.text.strip()
                        break
                if benchmark != "NA":
                    break
        df_row["Benchmark"] = benchmark

        # Returns and Rank
        fund_returns = {}
        category_averages = {}
        rank_within_category = {}
        
        # Find the returns table (contains "Rank with in category" or "Category average")
        returns_table = None
        for table in tables:
            if "Rank with in category" in table.text or "Category average" in table.text:
                returns_table = table
                break
        
        if returns_table:
            headers = [th.text.strip() for th in returns_table.find_all('th')]
            # Headers are usually ['', '1Y', '3Y', '5Y', 'All']
            
            rows = returns_table.find_all('tr')
            for row in rows:
                cols = row.find_all(['td', 'th'])
                col_texts = [c.text.strip() for c in cols]
                if not col_texts: continue
                
                label = col_texts[0]
                # Skip the header row itself if it's being iterated
                if label in headers and label != "Fund returns" and label != "Category average": continue

                if "Fund returns" in label:
                    for i, header in enumerate(headers):
                        if i > 0 and i < len(col_texts):
                            fund_returns[header] = col_texts[i]
                elif "Category average" in label:
                    for i, header in enumerate(headers):
                        if i > 0 and i < len(col_texts):
                            category_averages[header] = col_texts[i]
                elif "Rank with in category" in label:
                    for i, header in enumerate(headers):
                        if i > 0 and i < len(col_texts):
                            rank_within_category[header] = col_texts[i]

        for period in ["1Y", "3Y", "5Y", "All"]:
            df_row[f"{period} Fund Return"] = fund_returns.get(period, "NA")
            df_row[f"{period} Category Avg"] = category_averages.get(period, "NA")
            df_row[f"{period} Rank"] = rank_within_category.get(period, "NA")

        # P/E and P/B Ratio
        pe_ratio = "NA"
        pb_ratio = "NA"
        
        # Find table with P/E Ratio
        pe_table = None
        for table in tables:
            if "P/E Ratio" in table.text:
                pe_table = table
                break
        
        if pe_table:
            rows = pe_table.find_all('tr')
            for row in rows:
                cols = row.find_all(['td', 'th'])
                col_texts = [c.text.strip() for c in cols]
                if len(col_texts) >= 2:
                    if "P/E Ratio" in col_texts[0]:
                        pe_ratio = col_texts[1]
                    elif "P/B Ratio" in col_texts[0]:
                        pb_ratio = col_texts[1]

        df_row["P/E Ratio"] = pe_ratio
        df_row["P/B Ratio"] = pb_ratio

        # Alpha, Beta, Sharpe, Sortino
        stats_table = None
        for table in tables:
            if "Alpha" in table.text and "Beta" in table.text:
                stats_table = table
                break
        
        alpha = beta = sharpe = sortino = "NA"
        if stats_table:
            rows = stats_table.find_all('tr')
            for row in rows:
                header = row.find('th')
                value = row.find('td')
                if header and value:
                    header_text = header.text.strip()
                    val_text = value.text.strip()
                    if "Alpha" in header_text: alpha = val_text
                    elif "Beta" in header_text: beta = val_text
                    elif "Sharpe" in header_text: sharpe = val_text
                    elif "Sortino" in header_text: sortino = val_text
        
        df_row["Alpha"] = alpha
        df_row["Beta"] = beta
        df_row["Sharpe"] = sharpe
        df_row["Sortino"] = sortino

        # Extract Fund Managers and their tenure
        fund_managers = []
        manager_sections = soup.find_all('div', class_='fm982CardText')

        for manager in manager_sections:
            name_div = manager.find('div', class_='fm982PersonName')
            tenure_div = manager.find('div', class_='contentSecondary')
            
            name = name_div.text.strip() if name_div else "Unknown"
            tenure = tenure_div.text.strip() if tenure_div else "Unknown"
            fund_managers.append(f"{name} ({tenure})")

        # Combine all managers into a single string, separating them by commas
        df_row["Fund Managers"] = ', '.join(fund_managers)

        print(f"{fund_name} data added successfully.")
        return df_row

    except Exception as e:
        print(f"{url} data could not be added due to error: {e}")
        return None
    finally:
        if driver:
            driver.quit()

def main():
    # Read the list of URLs
    try:
        with open('mutual_funds_links.txt', 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("mutual_funds_links.txt not found.")
        return

    # Dictionary to store data by fund type
    data_by_fund_type = {}

    # List to store failed tasks
    failed_tasks = []

    # Run scraping in parallel using ThreadPoolExecutor
    # Reduced max_workers to avoid rate limiting/resource exhaustion
    max_workers = 6 
    print(f"Starting scraping with {max_workers} workers for {len(urls)} URLs...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(scrape_fund_data, url): url for url in urls}

        # Collect results as they complete
        for i, future in enumerate(as_completed(future_to_url)):
            url = future_to_url[future]
            try:
                result = future.result()
                if result:  # If data was scraped successfully
                    fund_type = result.get("Fund Type", "Others")
                    if fund_type not in data_by_fund_type:
                        data_by_fund_type[fund_type] = []
                    data_by_fund_type[fund_type].append(result)
                else:
                    failed_tasks.append(f"Task for URL {url} returned None")
            except Exception as e:
                # Store the failed task details
                failed_tasks.append(f"Task for URL {url} failed with error: {e}")
            
            # Add a small random delay to be nice to the server
            time.sleep(random.uniform(0.5, 1.5))
            
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(urls)} URLs")

    # Print all failed tasks at the end
    if failed_tasks:
        print(f"\n{len(failed_tasks)} Failed Tasks:")
        # Print only first 10 failures to avoid spamming console
        for task in failed_tasks[:10]:
            print(task)
        if len(failed_tasks) > 10:
            print(f"...and {len(failed_tasks) - 10} more.")
    else:
        print("\nAll tasks completed successfully.")

    # Define the consistent column order
    columns = [
        'Fund Name', 'Fund Type', 'AUM',
        '1Y Fund Return', '1Y Category Avg', '1Y Rank',
        '3Y Fund Return', '3Y Category Avg', '3Y Rank',
        '5Y Fund Return', '5Y Category Avg', '5Y Rank',
        'All Fund Return', 'All Category Avg', 'All Rank',
        'P/E Ratio', 'P/B Ratio', 'Alpha', 'Beta', 'Sharpe', 'Sortino',
        'Expense Ratio', 'Exit Load', 'Benchmark', 'Fund Managers'
    ]

    # Save each fund type to a separate sheet in the same Excel file
    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f'mutual_funds_details_{timestamp}.xlsx'
    
    if data_by_fund_type:
        print(f"Saving data to {filename}...")
        with pd.ExcelWriter(filename) as writer:
            for fund_type, data in data_by_fund_type.items():
                df = pd.DataFrame(data)
                # Reorder columns to match the specified order, filling any missing columns with "NA"
                # Only reorder if columns exist in df, otherwise add them
                for col in columns:
                    if col not in df.columns:
                        df[col] = "NA"
                
                df = df[columns] # Enforce order
                
                # Excel sheet names have a 31-character limit
                sheet_name = fund_type[:31].replace('/', '-') # Replace invalid chars
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print("Done.")
    else:
        print("No data collected to save.")

if __name__ == "__main__":
    main()
