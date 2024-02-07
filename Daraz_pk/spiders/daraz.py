import scrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
from scrapy.selector import Selector
import time
import json
from Daraz_pk.items import DarazPkItem
import time
from scrapy.selector import Selector
from selenium.webdriver.common.by import By

class DarazSpider(scrapy.Spider):
    name = "daraz_spider"
    allowed_domains = ["daraz.pk"]

    # Initialize the driver
    try:
        driver = webdriver.Chrome()
        driver.maximize_window()
    except Exception as e:
        print(f"Error initializing WebDriver: {e}")

    def start_requests(self):
        categories = ['smartphones']  # replace with your actual categories
        urls = [("https://www.daraz.pk/%s/?page=%d" % (category, i), category) for category in categories for i in range(1,2)]
        for url, category in urls:
            yield scrapy.Request(url, meta={'category': category})

    def parse(self, response):
        category = response.meta['category']
        
        try:
            # Extract all script tags within the head section
            script_tags = response.xpath('//head/script[4]').getall() # Extract all script tags data

            # Find the script containing the product links
            target_script = None
            for script in script_tags:
                if 'window.pageData' in script:  # Identify the script containing product links
                    target_script = script
                    break

            if target_script:
                # Extract the JSON object from the script
                start_index = target_script.find('{')
                end_index = target_script.rfind('}') + 1
                json_data = target_script[start_index:end_index]

                # Load the JSON data
                data = json.loads(json_data)
                # Extract product links
                product_urls = []
                if 'mods' in data and 'listItems' in data['mods']:
                    for item in data['mods']['listItems']:
                        if isinstance(item, dict) and 'productUrl' in item:
                            product_urls.append(("https:" + item['productUrl'], category))  # Pass category along with product URL

            for product_url, category in product_urls:
                self.driver.get(product_url)
                yield scrapy.Request(url=product_url, callback=self.parse_product_details, meta={'driver': self.driver, 'category': category})
        except Exception as e:
            print(f"Error in parse method: {e}")

    def parse_product_details(self, response):
        try:
            time.sleep(3)
            driver = response.meta.get('driver')
            category = response.meta.get('category')
            if driver and category:
                time.sleep(2)
                body = driver.page_source
                response = scrapy.http.TextResponse(url=response.url, body=body, encoding='utf-8')

            # Rest of the code...

                time.sleep(2)
                body = driver.page_source
                response = scrapy.http.TextResponse(url=response.url, body=body, encoding='utf-8')

            # extract product details here
                item = DarazPkItem()

                item['Productname'] = response.css('span.pdp-mod-product-badge-title::text').getall()
                item['BrandName'] = response.css('a.pdp-product-brand__brand-link::text').getall()
                item['DiscountPrice'] = response.css('span.pdp-price_size_xl::text').getall()
                item['OrighnalPrice'] = response.css('span.pdp-price_size_xs::text').getall()
                raw_image_urls = response.css('img.gallery-preview-panel__image::attr(src)').getall()
                
                url=response.url
                item['Category'] = category
                item['ProductUrl'] = url

                clean_image_urls = []
                for imag_url in raw_image_urls:
                    clean_image_urls.append(response.urljoin(imag_url))
                
                item['image_urls'] = clean_image_urls
                item['images'] = []

            #scroll down to load the reviews section
                driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(2)
            # Parse the current page with Scrapy
                response = Selector(text=driver.page_source)
                item['Rating'] = response.css('span.score::text').getall()
                all_reviews = []

                attempts = 0
                while True:
                    # Parse the current page with Scrapy
                    response = Selector(text=driver.page_source)

                    # Check if empty_review element is present
                    if response.css('div.mod-empty'):
                        break

                    # Extract the reviews on the current page
                    review_items = response.css('div.review-content div.review-item')
                    reviews = [item.css('div.review-content-sl::text').getall() for item in review_items]
                    all_reviews.extend(reviews)

                    # Find the "Next" button
                    next_button = driver.find_element(By.CSS_SELECTOR, "li.ant-pagination-next")

                    # If the "Next" button is disabled, break the loop
                    if next_button.get_attribute("aria-disabled") == "true":
                        break

                    # Click the "Next" button and wait for the page to load
                    next_button.click()
                    time.sleep(2)

                    attempts += 1
                    if attempts >= 3:
                        break

                item['reviews'] = all_reviews
                yield item
        except Exception as e:
            print(f"Error in parse_product_details method: {e}")