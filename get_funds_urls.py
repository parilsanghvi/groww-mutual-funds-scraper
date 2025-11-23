import requests
from bs4 import BeautifulSoup

# Initialize a set to keep track of unique URLs
unique_links = set()

with open("mutual_funds_links.txt", 'w') as f:
    # Fetch the first page and extract links
    page = requests.get("https://groww.in/mutual-funds/filter")
    soup = BeautifulSoup(page.text, 'html.parser')
    rows = soup.find_all('a', attrs={'class': 'pos-rel f22Link'})

    # Add unique links to the file
    for row in rows:
        link = f"https://groww.in{row.get('href')}"
        if link not in unique_links:
            unique_links.add(link)
            f.write(f"{link}\n")

    # Loop through subsequent pages and extract links
    for page_number in range(0, 105):
        page = requests.get(f"https://groww.in/mutual-funds/filter?q=&fundSize=&pageNo={page_number}&sortBy=3")
        soup = BeautifulSoup(page.text, 'html.parser')
        rows = soup.find_all('a', attrs={'class': 'pos-rel f22Link'})

        for row in rows:
            link = f"https://groww.in{row.get('href')}"
            # Only add if it's not already in the set
            if link not in unique_links:
                unique_links.add(link)
                f.write(f"{link}\n")
