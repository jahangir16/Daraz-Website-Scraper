# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DarazPkItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    Productname = scrapy.Field()
    BrandName= scrapy.Field()
    DiscountPrice= scrapy.Field()
    OrighnalPrice= scrapy.Field()
    Rating= scrapy.Field()
    image_urls = scrapy.Field()
    images = scrapy.Field()
    reviews = scrapy.Field()
    ProductUrl = scrapy.Field()
    Category = scrapy.Field()
