import scrapy
import logging
from scrapy.crawler import CrawlerProcess
import os
import pandas as pd
from urllib.parse import urlparse

class CsvPipeline(object):
    def __init__(self):
        self.file_path = 'urls.csv'
        self.urls_seen = set()
        if os.path.exists(self.file_path):
            df = pd.read_csv(self.file_path)
            self.urls_seen = set(df['listing_url'])
    def close_spider(self, spider):
        pass
    def process_item(self, item, spider):
        normalized_url = self.normalize_url(item['listing_url'])
        if normalized_url not in self.urls_seen:
            self.urls_seen.add(normalized_url)
            item['listing_url'] = normalized_url
            df = pd.DataFrame({'listing_url': [item['listing_url']]})
            df.to_csv(self.file_path, mode='a', header=not os.path.exists(self.file_path), index=False)
        return item
    def normalize_url(self, url):
        parsed_url = urlparse(url)
        listing_id = parsed_url.path.split('/')[2] # Extract the listing ID
        normalized_url = "https://www.etsy.com/listing/{}".format(listing_id)
        return normalized_url
class EtsySpider(scrapy.Spider):
    name = "etsy_spider"
    start_urls = ["https://www.etsy.com/fr/c?explicit=1&locationQuery=2542007&category_landing_page=1&order=most_relevant"]

    custom_settings = {
        'LOG_LEVEL': logging.WARNING,
        'ITEM_PIPELINES': {'__main__.CsvPipeline': 1},
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 130,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        'CONCURRENT_REQUESTS_PER_IP': 80,
        'DOWNLOAD_DELAY': 0.2,
    }

    count = 0
    total = 100
    urls_seen = set() # ensemble pour stocker les URLs déjà vus
    def parse(self, response):
        self.count += 1
        nexturl = "https://www.etsy.com/fr/c?explicit=1&locationQuery=2542007&category_landing_page=1&order=most_relevant&ref=pagination&page={}".format(self.count)

        for result in response.xpath('//div[@class="wt-bg-white wt-display-block wt-pb-xs-2 wt-mt-xs-0"]/div/div/div/ol/li/div/div/a'):
            url = result.xpath('@href').extract_first()
            yield scrapy.Request(url=url, callback=self.parse_detail)

        if self.count < self.total + 1:
            yield scrapy.Request(nexturl, self.parse)

    def parse_detail(self, response):
        listing_url = response.xpath('//link[@rel="canonical"]/@href').get()

        if listing_url not in self.urls_seen:
            self.urls_seen.add(listing_url)

        items = {
            'listing_url': listing_url
        }
        yield items

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like  Gecko) Chrome/113.0.0.0 Safari/537.36'
   })
process.crawl(EtsySpider)
process.start()
