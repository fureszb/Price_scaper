import scrapy

class ArkeresoItem(scrapy.Item):
    aruhaz = scrapy.Field()
    termek = scrapy.Field()
    nev = scrapy.Field()
    ar = scrapy.Field()
    url = scrapy.Field()  # FIXED: changed from 'link' to 'url'
    AI_Validacio = scrapy.Field()
    AI_Pontszam = scrapy.Field()
    AI_Indoklas = scrapy.Field()