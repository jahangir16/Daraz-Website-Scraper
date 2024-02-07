# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface    
import logging
from itemadapter import ItemAdapter


class DarazPkPipeline:
    def process_item(self, item, spider):
        def filter_empty_reviews(reviews):
            return [''.join(review).replace('\n', '') for review in reviews if review and ''.join(review).replace('\n', '')]

        item['reviews'] = filter_empty_reviews(item['reviews'])

        return item
    
#now here i do all the code of how to connect to the database and how to save the data in the database
    #I AM USING postgresql database
    #so first i have to install the psycopg2 library
    #pip install psycopg2
    #then i have to install the sqlalchemy library
    #pip install sqlalchemy
    # NOW MADE A CLASS PostgresDemoPipeline
    #I HAVE TO IMPORT THE sqlalchemy library
    #from sqlalchemy import create_engine
    #from sqlalchemy.orm import sessionmaker


# pipelines.py

import psycopg2
import psycopg2.extras


class PostgresDemoPipeline:

    def __init__(self):
        ## Connection Details
        hostname = 'localhost'
        username = 'postgres'
        password = '123456' # your password
        database = 'DarazProducts1'

        ## DB Connection
        ## Create/Connect to database
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
        ## Create cursor, used to execute commands
        self.cur = self.connection.cursor()

        ## Create quotes table if none exists
         ## Create quotes table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS products(
            id serial PRIMARY KEY, 
            ProductName TEXT,
            BrandName TEXT,
            DiscountPrice TEXT,
            OrighnalPrice TEXT,
            Category TEXT,
            ProductUrl TEXT,
            image_urls TEXT,
            images JSONB,
            CONSTRAINT unique_product UNIQUE (ProductName, BrandName)
        )
        """)
        
        ## Create reviews table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews(
    id SERIAL PRIMARY KEY,
    product_id INT,
    review_content TEXT,
    CONSTRAINT unique_review UNIQUE (product_id, review_content),
    FOREIGN KEY (product_id) REFERENCES products(id)
)
        """)
       # self.cur.execute("""
        #ALTER TABLE reviews
       # ADD CONSTRAINT unique_review UNIQUE (product_id, review_content)
        #""")

        pass

    def process_item(self, item, spider):
        ## Define insert statement for quotes table
        self.cur.execute(""" 
        INSERT INTO products (ProductName, BrandName, DiscountPrice, OrighnalPrice, Category, ProductUrl, image_urls, images)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ProductName, BrandName) DO UPDATE 
        SET DiscountPrice = EXCLUDED.DiscountPrice, OrighnalPrice = EXCLUDED.OrighnalPrice, Category = EXCLUDED.Category, ProductUrl = EXCLUDED.ProductUrl, image_urls = EXCLUDED.image_urls, images = EXCLUDED.images
        RETURNING id
        """, (
        item.get("Productname"),
        item.get("BrandName"),
        item.get("DiscountPrice"),
        item.get("OrighnalPrice"),
        item.get("Category"),
        item.get("ProductUrl"),
        item.get("image_urls"),
        psycopg2.extras.Json(item.get("images")) if item.get("images") else None
        ))

        product_id_row = self.cur.fetchone()
    
        if product_id_row:
            product_id = product_id_row[0]  # Get the ID of the inserted product
        
        ## Insert reviews into the reviews table
            for review in item.get("reviews", []):
                try:
                    self.cur.execute("""
                    INSERT INTO reviews (product_id, review_content) VALUES (%s, %s)
                     ON CONFLICT (product_id, review_content) DO NOTHING
                    """, (product_id, review))
                except Exception as e:
                    logging.error(f"Error inserting review into database: {e}")
                    logging.error(f"Item that caused the error: {item}")
        
        ## Commit the transaction after inserting all reviews
            self.connection.commit()
        else:
            print("No product ID returned from the database.")
        
        return item



    def close_spider(self, spider):
        #self.connection.commit()
        ## Close cursor & connection to database
        print("Closing Data Added to Database") 
        self.cur.close()
        self.connection.close() 
       