import scrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
from scrapy.selector import Selector
import time
import psycopg2
from Daraz_pk.items import DarazPkItem
from scrapy.http import TextResponse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class DarazSpider(scrapy.Spider):
    name = "daraz_spider"
    allowed_domains = ["daraz.pk"]

    def __init__(self, *args, **kwargs):
        super(DarazSpider, self).__init__(*args, **kwargs)
        self.driver = None
        self.conn = None
        self.cur = None
        self.last_url = None

    def start_requests(self):
        self.driver = webdriver.Chrome()
        self.driver.maximize_window()

        self.conn = psycopg2.connect(database="DarazProducts1", user="postgres", password="123456", host="localhost")
        self.cur = self.conn.cursor()

        while True:
            try:
                self.cur.execute('SELECT url, category FROM product_urls WHERE processed = FALSE LIMIT 1')
                product_urls = self.cur.fetchall()

                if not product_urls:
                    break

                for product_url, category in product_urls:
                    if self.last_url == product_url:  # Check if the URL is the same as the last one
                        self.logger.warning(f"Stuck in a loop with URL: {product_url}")
                        continue

                    self.logger.info(f"Processing URL: {product_url}")
                    self.driver.get(product_url)
                    time.sleep(3)
                    yield scrapy.Request(url=product_url, callback=self.parse, meta={'category': category, 'product_url': product_url})

            except Exception as e:
                self.logger.error(f"Error processing URL: {e}")
                continue

        self.cur.close()
        self.conn.close()
        self.driver.quit()

    def parse(self, response):
        try:
           product_url = response.meta['product_url']
           category = response.meta['category']
        
           body = self.driver.page_source
           scrapy_response = TextResponse(url=response.url, body=body, encoding='utf-8')
           item = DarazPkItem()

           item['Productname'] = scrapy_response.css('span.pdp-mod-product-badge-title::text').getall()
           item['BrandName'] = scrapy_response.css('a.pdp-product-brand__brand-link::text').getall()
           item['DiscountPrice'] = scrapy_response.css('span.pdp-price_size_xl::text').getall()
           item['OrighnalPrice'] = scrapy_response.css('span.pdp-price_size_xs::text').getall()
           raw_image_urls = scrapy_response.css('img.gallery-preview-panel__image::attr(src)').getall()
           url = scrapy_response.url
           item['Category'] = category
           item['ProductUrl'] = url    
           clean_image_urls = [scrapy_response.urljoin(image_url) for image_url in raw_image_urls]
           item['image_urls'] = clean_image_urls
           item['images'] = []

           driver = self.driver
           driver.execute_script("window.scrollBy(0, 1000);")
           time.sleep(3)

           selector = Selector(text=driver.page_source)
           item['Rating'] = selector.css('span.score::text').getall()
           all_reviews = []

           empty_reviews = 0
           last_page_reviews = None

           while True:
                 selector = Selector(text=driver.page_source)
                 review_items = selector.css('div.review-content div.review-item')
                 reviews = [item.css('div.review-content-sl::text').getall() for item in review_items]

                 if not reviews:
                    empty_reviews += 1
                 else:
                    empty_reviews = 0

                 if empty_reviews >= 3 or all(not review for review in reviews) or (last_page_reviews is not None and last_page_reviews == reviews):
                    break

                 all_reviews.extend(reviews)
                 #next_button = driver.find_element(By.CSS_SELECTOR, "li.ant-pagination-next")
                 # Wait for the "Next Page" button to be clickable
                 next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "li.ant-pagination-next"))
                    )
                 if next_button.get_attribute("aria-disabled") == "true":
                     break
                 
                 time.sleep(3)
                 next_button.click()
                 time.sleep(2)
                 last_page_reviews = reviews

           item['reviews'] = all_reviews
           yield item
           print("Item is yielded")
           self.cur.execute('UPDATE product_urls SET processed = TRUE WHERE url = %s', (product_url,))
           self.conn.commit()
           self.logger.info(f"Marked URL as processed: {product_url}")
           self.last_url = product_url
           self.logger.info(f"Updated last_url to: {self.last_url}")
        except Exception as e:
            self.logger.error(f"Error parsing response: {e}")

    def closed(self, reason):
        print("Spider closed with reason:", reason)
