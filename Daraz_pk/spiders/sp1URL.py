import scrapy
import json
import psycopg2

class DarazURLSpider(scrapy.Spider):
    name = "daraz123"
    allowed_domains = ["daraz.pk"]

    def start_requests(self):
        self.conn = psycopg2.connect(database="DarazProducts1", user="postgres", password="123456", host="localhost")
        self.cur = self.conn.cursor()
        self.cur.execute('CREATE TABLE IF NOT EXISTS product_urls (id serial PRIMARY KEY, url TEXT, category TEXT)')
    # ...
        categories = ['smartphones']  # replace with your actual categories
        urls = [("https://www.daraz.pk/%s/?page=%d" % (category, i), category) for category in categories for i in range(1,2)]
        for url, category in urls:
            yield scrapy.Request(url, meta={'category': category})

    def closed(self, reason):
        self.conn.commit()
        self.cur.close()
        self.conn.close()


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
                            product_url = "https:" + item['productUrl']
                            self.cur.execute('INSERT INTO product_urls (url, category) VALUES (%s, %s)', (product_url, category))

         
        except Exception as e:
            self.log(f"Error in parse method: {e}")