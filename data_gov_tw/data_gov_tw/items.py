# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DataGovTwItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class MappingItem(scrapy.Item):
    filename = scrapy.Field()
    id = scrapy.Field()
    name = scrapy.Field()
    description = scrapy.Field()
    changed_date = scrapy.Field()
    columns = scrapy.Field()
    char_encoding = scrapy.Field()
    file_format = scrapy.Field()
