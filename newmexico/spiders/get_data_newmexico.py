import time

import evpn
import scrapy
from scrapy.cmdline import execute
from datetime import datetime
import pandas as pd
import os


# Define helper functions outside the class
def extract_link(row):
    """Extract the link from a row."""
    return row.xpath('./td/strong/a/@href').get() or 'N/A'


def extract_title(row):
    """Extract the title from a row."""
    return row.xpath('./td/strong/a/text()').get() or 'N/A'


def extract_date(row):
    """Extract and format the date from a row."""
    date = row.xpath('./td/strong/text()').get()
    if date:
        try:
            date_object = datetime.strptime(date, '%m/%d/%y')
            return date_object.strftime('%Y-%m-%d')
        except ValueError:
            return 'N/A'
    return 'N/A'


class GetDataNewmexicoSpider(scrapy.Spider):
    name = "get_data_newmexico"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_list = []  # Initialize an empty list to store scraped data
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (USA)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (USA)
        self.api.connect(country_id='207')  # USA country code for vpn
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

    def start_requests(self):
        cookies = {
            'cmplz_rt_consented_services': '',
            'cmplz_rt_policy_id': '1',
            'cmplz_rt_marketing': 'allow',
            'cmplz_rt_statistics': 'allow',
            'cmplz_rt_preferences': 'allow',
            'cmplz_rt_functional': 'allow',
            '_ga': 'GA1.2.313705771.1732858800',
            '_gid': 'GA1.2.1740210805.1733233226',
            'dbbc_breadcrumbs': '%5B%5D',
            '_gat_gtag_UA_46787532_1': '1',
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        yield scrapy.Request(
            url='https://www.tax.newmexico.gov/news-alerts/',
            cookies=cookies,
            headers=headers,
            dont_filter=True,
            callback=self.parse,
        )

    def parse(self, response):
        # Locate all rows in the table
        all_rows = response.xpath('//div[@class="et_pb_text_inner"]/table/tbody/tr')

        # Iterate through each row
        for idx, row in enumerate(all_rows, start=1):  # Add an index starting from 1
            # Extract data using helper functions
            link = extract_link(row)
            title = extract_title(row)
            date = extract_date(row)

            # Append data as a dictionary to the list
            self.data_list.append({
                'id': idx,  # Assign the current index as the ID
                'date': date,
                'title': title,
                'link': link,
            })

    def closed(self, reason):
        """Generate an Excel file with the scraped data when the spider closes."""
        if self.data_list:
            # Ensure the 'output' directory exists
            output_dir = '../output'
            os.makedirs(output_dir, exist_ok=True)

            # Define the file path
            filename = os.path.join(output_dir, 'newmexico_news_alerts.xlsx')

            # Save data to the Excel file
            df = pd.DataFrame(self.data_list)
            df.to_excel(filename, index=False)

            self.logger.info(f"Data saved to {filename}")
        else:
            self.logger.info("No data was scraped to save.")

        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute('scrapy crawl get_data_newmexico'.split())
